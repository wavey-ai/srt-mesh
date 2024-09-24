use access_unit::AccessUnit;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

pub struct ChannelRegistry {
    channels: Arc<RwLock<BTreeMap<u64, Arc<mpsc::Receiver<AccessUnit>>>>>,
}

impl ChannelRegistry {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn insert(&self, id: u64, tx: mpsc::Receiver<AccessUnit>) {
        let mut channels = self.channels.write().await;
        channels.insert(id, Arc::new(tx));
    }

    pub async fn get(&self, id: u64) -> Option<Arc<mpsc::Receiver<AccessUnit>>> {
        let channels = self.channels.read().await;
        channels.get(&id).cloned()
    }

    pub async fn remove(&self, id: u64) -> Option<mpsc::Receiver<AccessUnit>> {
        let mut channels = self.channels.write().await;
        channels
            .remove(&id)
            .map(|arc_rx| Arc::try_unwrap(arc_rx).ok().unwrap())
    }

    pub async fn list_keys(&self) -> Vec<u64> {
        let channels = self.channels.read().await;
        channels.keys().cloned().collect()
    }
}
