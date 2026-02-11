use anyhow::Context;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub version: u32,
    pub database: Database,
    pub library_source: LibrarySource,
    pub http: HttpConfig,
    pub public_endpoint: PublicEndpoint,
    pub data: Data,
}

/// LocalDeck data dir: used for storing artwork, database, etc
#[derive(Debug, Deserialize)]
pub struct Data {
    root_dir: Location,
    /// must be relative to root_dir
    artwork_dir: PathBuf,
}

/// "public" endpoint that will be used on QR codes and NFCs
#[derive(Debug, Deserialize)]
pub struct PublicEndpoint {
    /// example: http://main-deck:8080
    pub base_url: String,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Config> {
        let contents = std::fs::read_to_string(path).expect("Failed to read user config");
        toml::from_str(&contents).with_context(|| "Failed to parse config TOML")
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub bind_addr: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum Database {
    InMemory,
    OnDisk { location: Location },
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "type")]
pub enum Location {
    File { path: PathBuf },
    Usb { label: String, path: PathBuf },
}

#[derive(Debug, Deserialize, Default)]
pub struct LibrarySource {
    pub roots: Vec<Location>,
    pub follow_symlinks: bool,
    /// directories on computer that should be ignored when scanning the library. Does not work with USB directories
    #[serde(default)]
    pub ignored_dirs: Vec<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_config_toml() -> anyhow::Result<()> {
        let toml_str = r#"
version = 1

[database]
type = "InMemory"

[library_source]
roots = [{type = "File", path = "/home/sancho20021/Music"}]
follow_symlinks = true
ignored_dirs = ['C:\Users\sanch\Music\music\Sample pack']

[data]
root_dir = { type = "File", path = "/home/sancho20021/hello" }
artwork_dir = "artwork"

[http]
bind_addr = "127.0.0.1"
port = 8080

[public_endpoint]
base_url = "hello"
"#;

        // Deserialize TOML into Config
        let cfg: Config = toml::from_str(toml_str)?;

        // Check version
        assert_eq!(cfg.version, 1);

        // Check database variant
        assert!(cfg.database == Database::InMemory);

        // Check library source
        assert_eq!(
            cfg.library_source.roots,
            vec![Location::File {
                path: PathBuf::from("/home/sancho20021/Music")
            }]
        );
        assert!(cfg.library_source.follow_symlinks);

        Ok(())
    }

    #[test]
    fn test_parse_file_database_config() -> anyhow::Result<()> {
        let toml_str = r#"
type = "OnDisk"
location = { type = "File", path = "/tmp/localdex.db" }
"#;

        let cfg: Database = toml::from_str(toml_str)?;

        // Check database variant
        assert!(
            matches!(cfg, Database::OnDisk { location: Location::File { path } } if path == PathBuf::from("/tmp/localdex.db"))
        );
        Ok(())
    }

    #[test]
    fn test_parse_usb_database_config() -> anyhow::Result<()> {
        let toml_str = r#"
type = "OnDisk"
location = { type = "Usb", label = "MUSIC", path = "localdex.db" }
"#;

        let cfg: Database = toml::from_str(toml_str)?;

        // Check database variant
        assert!(
            matches!(cfg, Database::OnDisk { location: Location::Usb { label, path } }
                if label == "MUSIC" && path == PathBuf::from("localdex.db"))
        );

        Ok(())
    }
}
