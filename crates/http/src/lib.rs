use serde::Deserialize;

pub mod server;
pub mod error;

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub bind_addr: String,
    pub port: u16,
}
