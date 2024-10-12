use crate::channel_registry::ChannelRegistry;
use crate::streamkey::{Gatekeeper, Streamkey};
use access_unit::AccessUnit;
use bytes::Bytes;
use discovery::Nodes;
use futures::stream;
use futures::{SinkExt, StreamExt};
use srt_tokio::{SrtListener, SrtSocket};
use std::collections::HashSet;
use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{timeout, Duration};
use tracing::{error, info};
use ts::demuxer::TsDemuxer;

const TIMEOUT_SECS: u64 = 120;
// in SRT packets
const METRICS_INTERVAL: u32 = 400;

const SRT_NEW: &str = "SRT:NEW";
const SRT_UP: &str = "SRT:UP ";
const SRT_FWD: &str = "SRT:FWD";
const SRT_BYE: &str = "SRT:BYE";

// TODO: per-region configuration
pub struct ReplicationTopology {
    // min number of vlan nodes to replicate to
    vlan_min: usize,
    // eg 2成 - replicate to max 20% of vlan nodes (2 per decem).
    vlan_max_ncheng: usize,
}

impl ReplicationTopology {
    pub fn new(vlan_min: usize, vlan_max_ncheng: usize) -> Self {
        Self {
            vlan_min,
            vlan_max_ncheng,
        }
    }

    pub fn vlan_min(&self) -> usize {
        self.vlan_min
    }

    pub fn vlan_max_ncheng(&self) -> usize {
        self.vlan_max_ncheng
    }
}

pub struct SRTListener;

impl SRTListener {
    pub async fn start(
        base64_encoded_pem_key: &str,
        addr: SocketAddr,
        channels: Arc<ChannelRegistry>,
        lan: Option<Arc<Nodes>>,
        dns: Option<Arc<Nodes>>,
        topology: ReplicationTopology,
    ) -> Result<
        (
            oneshot::Receiver<()>,
            oneshot::Receiver<()>,
            watch::Sender<()>,
        ),
        Box<dyn Error + Send + Sync>,
    > {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let (up_tx, up_rx) = oneshot::channel();
        let (fin_tx, fin_rx) = oneshot::channel();

        let gatekeeper = Gatekeeper::new(base64_encoded_pem_key)?;

        let port = addr.port();

        let srv = async move {
            match SrtListener::builder().bind(addr).await {
                Ok((_, mut incoming)) => {
                    info!("SRT Multiplex Server is listening at: {addr}");

                    if let Err(e) = up_tx.send(()) {
                        error!("Failed to send startup signal: {:?}", e);
                        return Err(Box::<dyn Error + Send + Sync>::from(
                            "Failed to send startup signal",
                        ));
                    }

                    let channels = channels.clone();

                    let vlan_min = topology.vlan_min();
                    let vlan_max_ncheng = topology.vlan_max_ncheng();

                    loop {
                        tokio::select! {
                            _ = shutdown_rx.changed() => {
                                info!("Received shutdown signal, exiting...");
                                break;
                            }

                            Some(request) = incoming.incoming().next() => {
                                let stream_key: Streamkey;
                                if let Some(id) = &request.stream_id() {
                                    stream_key = gatekeeper.streamkey(id)?;
                                    info!("{} {} {}", SRT_NEW, stream_key.key(), stream_key.id());
                                } else {
                                    return Err(Box::<dyn Error + Send + Sync>::from(
                                        "Failed to retrieve stream key",
                                    ));
                                }

                                let remote_ip = request.remote().ip();

                                let fwd_to_dns = stream_key.is_origin();
                                if fwd_to_dns {
                                    info!("srt connection from {} is new stream key; forwarding to dns regions", request.remote().ip())
                                } else {
                                    info!("srt connection from {} has stream id; not forwarding to dns regions", request.remote().ip())
                                }

                                let fwd_to_lan = if stream_key.is_origin() {
                                    true
                                } else if remote_ip.is_loopback() {
                                    false
                                } else if let IpAddr::V4(ipv4) = remote_ip {
                                    crate::ip::is_global(&ipv4)
                                } else {
                                    false
                                };

                                if fwd_to_lan {
                                    info!("srt connection from {} appears external; forwarding to local vlan", request.remote().ip());

                                    if let Some(lan) = &lan {
                                        for node in lan.all() {
                                            info!("forwarding to {}", node.ip());
                                        }
                                    }
                                } else {
                                    info!("srt connection from {} appears local; doing nothing", request.remote().ip())
                                }

                                let stream_id = stream_key.id().to_string();
                                let stream_key_str = stream_key.key();

                                let mut forward_lan_sockets: Vec<SrtSocket> = Vec::new();
                                let mut forward_dns_sockets: Vec<SrtSocket> = Vec::new();

                                let channels = channels.clone();
                                let lan_nodes = lan.clone();
                                let dns_nodes = dns.clone();

                                let encoded_key = gatekeeper.encode_key(&stream_key_str, stream_key.id()).unwrap_or("TODO".to_string());
                                tokio::spawn(async move {
                                    if let Ok(mut srt_socket) = request.accept(None).await {

                                        let mut n_lan = 0;
                                        // mirror to all nodes on the vlan upto default replication
                                        // limit - this is so we don't exhaust all available playlist slots.
                                        if fwd_to_lan {
                                            if let Some(nodes) = lan_nodes {
                                                let nodes = nodes.all();
                                                let max_lan = (nodes.len() * vlan_max_ncheng) / 10;
                                                for node in &nodes {
                                                    match SrtSocket::builder().call(node.addr(port), Some(&encoded_key)).await {
                                                        Ok(socket) => {
                                                            forward_lan_sockets.push(socket);
                                                            n_lan += 1;
                                                            info!("Added new LAN node: {}", node.addr(port));
                                                        }
                                                        Err(e) => {
                                                            // this is possibly because the node has no
                                                            // available playlist slots, in which case
                                                            // we will try another.
                                                            error!("Failed to connect to new LAN node {}: {:?}", node.addr(port), e);
                                                        }
                                                    }

                                                    if n_lan >= max_lan && n_lan >= vlan_min {
                                            //            break;
                                                    }
                                                }
                                            }
                                        }

                                        let mut added_tags = HashSet::new();

                                        // forward every stream once to one node in each global region.
                                        if fwd_to_dns {
                                            if let Some(nodes) = dns_nodes {
                                                let mut own_tag = String::from("");
                                                for node in &nodes.all() {
                                                    if node.is_self() {
                                                        if let Some(tag) = node.tag() {
                                                            own_tag = tag.to_string();
                                                        }
                                                    }
                                                }

                                                for node in nodes.all() {
                                                    if let Some(tag) = node.tag() {
                                                        if tag == &own_tag {
                                                            info!("skipping dns node {}-{} for streamid {} as has own tag of {}", tag, node.seq().unwrap_or(0), stream_id, own_tag);
                                                            continue;
                                                        }
                                                        if added_tags.contains(tag) {
                                                            continue;
                                                        }
                                                    }

                                                    info!("adding dns node {}-{} with ip {} for streamid {}", node.tag().unwrap(), node.seq().unwrap(), node.ip(), stream_id);


                                                    match SrtSocket::builder()
                                                        .call(node.addr(port), Some(&encoded_key))
                                                        .await
                                                    {
                                                        Ok(socket) => {
                                                            forward_dns_sockets.push(socket);
                                                            info!("Added new DNS node: {}", node.addr(port));

                                                            if let Some(tag) = node.tag() {
                                                                added_tags.insert(tag.clone());
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!("Failed to connect to new DNS node {}: {:?}", node.addr(port), e);
                                                        }
                                                    }
                                                }

                                                let all_tags: HashSet<String> = nodes.all().iter().filter_map(|node| node.tag().cloned()).collect();
                                                let missing_tags: HashSet<_> = all_tags.difference(&added_tags).collect();

                                                if !missing_tags.is_empty() {
                                                    error!("Failed to fwd to regions {:?} this will cause a loss of service stream_id={}", missing_tags, stream_id);
                                                }
                                            }
                                        }

                                        let (tx, rx) = mpsc::channel::<AccessUnit>(34);
                                        channels.insert(stream_key.id(), rx).await;
                                        let demux_tx = TsDemuxer::start(tx.clone());

                                        let mut i: u32 = 0;

                                        loop {
                                            select! {
                                                result = timeout(Duration::from_secs(TIMEOUT_SECS), srt_socket.next()) => {
                                                    match result {
                                                       Ok(Some(Ok((instant, data)))) => {
                                                            // Wrap the received data into a stream
                                                            let stream = stream::once(async {
                                                                Ok((Instant::now(), data.clone()))
                                                            });
                                                            if let Err(e) = srt_socket.send_all(&mut stream.boxed()).await {
                                                                eprintln!("Failed to send data: {}", e);
                                                           }
                                                            let bytes = Bytes::from(data);
                                                            i = i.wrapping_add(1);

                                                            if i%METRICS_INTERVAL == 0 {
                                                                info!("{} key={} id={} addr={}", SRT_UP, stream_key.key(), stream_key.id(), srt_socket.settings().remote);
                                                            }

                                                            // Forward to LAN sockets if available
                                                            {
                                                                for (index, forward_socket) in forward_lan_sockets.iter_mut().enumerate() {
                                                                    if let Err(e) = forward_socket.send((Instant::now(), bytes.clone())).await {
                                                                        error!("Error forwarding to LAN socket {}: {:?}", index, e);
                                                                    }
                                                                    if i%METRICS_INTERVAL == 0 {
                                                                        info!("{} key={} id={} addr={}", SRT_FWD, stream_key.key(), stream_key.id(), forward_socket.settings().remote);
                                                                    }
                                                                }
                                                            }

                                                            // Forward to DNS sockets if available
                                                            {
                                                                for (index, forward_socket) in forward_dns_sockets.iter_mut().enumerate() {
                                                                    if let Err(e) = forward_socket.send((Instant::now(), bytes.clone())).await {
                                                                        error!("Error forwarding to DNS socket {}: {:?}", index, e);
                                                                    }
                                                                    if i%METRICS_INTERVAL == 0 {
                                                                        info!("{} key={} id={} addr={}", SRT_FWD, stream_key.key(), stream_key.id(), forward_socket.settings().remote);
                                                                    }

                                                                }
                                                            }

                                                            if let Err(e) = demux_tx.send(bytes).await {
                                                                error!("Error sending to demuxer: {:?}", e);
                                                                break;
                                                            }
                                                        }
                                                        Ok(Some(Err(e))) => {
                                                            info!("Error receiving packet: {:?}", e);
                                                            break;
                                                        }
                                                        Ok(None) => {
                                                            info!("SRT connection closed");
                                                            break;
                                                        }
                                                        Err(_) => {
                                                            info!("Timeout reached, no packets received in the last {} seconds, breaking the loop", TIMEOUT_SECS);
                                                            break;
                                                        }
                                                    }
                                                }
                                                //_ = fin_rx.changed() => {
                                                //   info!("{} id={} rejected - playlist slots full", SRT_BYE, stream_id);
                                                //    break;
                                                //}
                                                else => {
                                                    // Exit the loop when there are no more packets
                                                    break;
                                                }
                                            }
                                        }

                                        info!("Client disconnected stream_key={} stream_id={}", stream_key.key(), stream_key.id());

                                        info!("srt stream {} ended: shutdown complete", stream_key.id());
                                    } else {
                                        error!("Failed to accept SRT connection");
                                    }
                                });
                            }

                            // Handle case when stream ends
                            else => break,
                        }
                    }

                    if let Err(e) = fin_tx.send(()) {
                        error!("Failed to send finish signal: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to bind SRT listener: {:?}", e);
                    return Err(Box::new(e) as Box<dyn Error + Send + Sync>);
                }
            }

            Ok::<(), Box<dyn Error + Send + Sync>>(())
        };

        tokio::spawn(srv);

        Ok((up_rx, fin_rx, shutdown_tx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::env;
    use tokio::task;
    use tokio::time::{interval, Duration};
    use tracing_subscriber::prelude::*;

    // ffmpeg -re -f lavfi -i testsrc=duration=3600:size=1280x720:rate=30 -f lavfi -i sine=frequency=1000:duration=3600 -c:v libx264 -preset veryfast -tune zerolatency -b:v 2M -c:a aac -b:a 128k -f mpegts "srt://127.0.0.1:8000?pkt_size=1316&streamid=test_streamkey"

    async fn consume_channel(key: u64, mut rx: mpsc::Receiver<AccessUnit>) {
        println!("Started consuming channel for key: {}", key);
        while let Some(access_unit) = rx.recv().await {
            println!(
                "Received access unit for key {}: {:?}",
                key, access_unit.dts
            );
        }
        println!("Channel for key {} has been closed", key);
    }

    async fn run_listener() {
        const ENV_FILE: &str = include_str!("../.env");

        for line in ENV_FILE.lines() {
            if let Some((key, value)) = line.split_once('=') {
                env::set_var(key.trim(), value.trim());
            }
        }
        let subscriber =
            tracing_subscriber::registry().with(tracing_subscriber::fmt::Layer::default());

        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global default subscriber");

        let privkey_pem = env::var("PRIVKEY_PEM").unwrap();
        let channels = ChannelRegistry::new();
        let channels = Arc::new(channels);

        let channels_clone = channels.clone();
        task::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            let mut known_keys = HashSet::new();

            loop {
                interval.tick().await;

                let keys = channels_clone.list_keys().await;
                for key in keys {
                    if known_keys.insert(key) {
                        println!("New key detected: {}", key);

                        // Get and remove the channel from the registry
                        if let Some(rx) = channels_clone.remove(key).await {
                            // Spawn a new task to consume this channel
                            task::spawn(async move {
                                consume_channel(key, rx).await;
                            });
                        }
                    }
                }
            }
        });

        let topology = ReplicationTopology::new(1, 3);
        let srt_bind_addr: SocketAddr = ([0, 0, 0, 0], 8000).into();
        let (srt_up, srt_fin, srt_shutdown) =
            SRTListener::start(&privkey_pem, srt_bind_addr, channels, None, None, topology)
                .await
                .unwrap();
        let _ = srt_up.await;
        let _ = srt_fin.await;
    }

    #[tokio::test]
    async fn test_listener() {
        run_listener().await;
    }
}
