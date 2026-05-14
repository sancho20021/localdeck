use anyhow::Context;
use serde::Deserialize;
use std::path::Path;

use localdeck_http::HttpConfig;
use localdeck_storage::config::Config as DBConfig;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub storage: DBConfig,
    pub http: HttpConfig,
}

impl Config {
    /// load the config file. first tries the env var LOCALDECK_CONFIG, then the provided path
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let contents = std::fs::read_to_string(path).expect("Failed to read user config");
        toml::from_str(&contents).with_context(|| "Failed to parse config TOML")
    }
}

#[cfg(test)]
mod tests {
    use localdeck_storage::config::Database;

    use super::*;

    #[test]
    fn test_parse_config_toml() -> anyhow::Result<()> {
        let toml_str = r#"
version = 1

[storage.database]
type = "InMemory"

[storage.library_source]
roots = [{type = "File", path = "/home/sancho20021/Music"}]
follow_symlinks = true
ignored_dirs = ['C:\Users\sanch\Music\music\Sample pack']

[http]
bind_addr = "127.0.0.1"
port = 8080
"#;

        // Deserialize TOML into Config
        let cfg: Config = toml::from_str(toml_str)?;

        // Check database variant
        assert!(cfg.storage.database == Database::InMemory);

        assert_eq!(cfg.http.bind_addr, "127.0.0.1");
        assert_eq!(cfg.http.port, 8080);
        Ok(())
    }
}
