use base64::engine::general_purpose::STANDARD as base64_engine;
use base64::Engine;
use hmac::{Hmac, Mac};
use rustls::{Certificate, PrivateKey};
use sha2::Sha256;
use sonyflake::Sonyflake;
use std::error::Error;
use tls_helpers::privkey_from_base64;

type HmacSha256 = Hmac<Sha256>;

pub struct Streamkey {
    key: String,
    id: u64,
    origin: bool,
}

impl Streamkey {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn key(&self) -> String {
        self.key.to_owned()
    }

    pub fn is_origin(&self) -> bool {
        self.origin
    }
}
pub struct Gatekeeper {
    snowflakes: Sonyflake,
    private_key: PrivateKey,
}

impl Gatekeeper {
    pub fn new(base64_encoded_pem_key: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let snowflakes = Sonyflake::new()?;
        let private_key = privkey_from_base64(base64_encoded_pem_key)?;

        Ok(Self {
            snowflakes,
            private_key,
        })
    }

    pub fn streamkey(
        &self,
        maybe_encoded_key: &str,
    ) -> Result<Streamkey, Box<dyn Error + Send + Sync>> {
        let stream_key: String;
        let stream_id: u64;
        let origin: bool;

        if let Ok(res) = self.decode_key(maybe_encoded_key) {
            stream_key = res.0;
            stream_id = res.1;
            origin = false;
        } else {
            // TODO: check provided key is a steam key in the allow list
            stream_key = maybe_encoded_key.to_string();
            stream_id = self.snowflakes.next_id().unwrap();
            origin = true;
        }
        Ok(Streamkey {
            key: stream_key,
            id: stream_id,
            origin,
        })
    }

    pub fn encode_key(&self, key: &str, id: u64) -> Result<String, Box<dyn Error>> {
        let data = format!("{}:{}", key, id);
        let hmac_key = self.private_key.0.clone();
        let mut mac = HmacSha256::new_from_slice(&hmac_key)?;
        mac.update(data.as_bytes());
        let signature = mac.finalize().into_bytes();

        let hmac = base64_engine.encode(&signature);
        Ok(format!("{}:{}", data, hmac))
    }

    pub fn decode_key(&self, encoded: &str) -> Result<(String, u64), Box<dyn Error>> {
        let parts = encoded.rsplitn(2, ':').collect::<Vec<&str>>();

        if parts.len() != 2 {
            return Err("Invalid encoded key format".into());
        }

        let data = parts[1];
        let received_hmac = base64_engine.decode(parts[0])?;

        let hmac_key = self.private_key.0.clone();
        let mut mac = HmacSha256::new_from_slice(&hmac_key)?;
        mac.update(data.as_bytes());

        mac.verify_slice(&received_hmac)?;

        let key_id_parts = data.split(':').collect::<Vec<&str>>();
        if key_id_parts.len() != 2 {
            return Err("Invalid data format".into());
        }

        let key = key_id_parts[0].to_owned();
        let id = key_id_parts[1].parse::<u64>()?;
        Ok((key, id))
    }
}
