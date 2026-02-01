use anyhow::Context;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub version: u32,
    pub database: Database,
    pub library_source: LibrarySource,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Config> {
        let contents = std::fs::read_to_string(path).expect("Failed to read user config");
        toml::from_str(&contents).with_context(|| "Failed to parse config TOML")
    }
}

#[derive(Debug, Deserialize)]
pub struct Database {
    pub in_memory: bool,
    pub path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct LibrarySource {
    pub roots: Vec<PathBuf>,
    pub follow_symlinks: bool,
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
in_memory = true

[library_source]
roots = ["/home/sancho20021/Music"]
follow_symlinks = true
"#;

        // Deserialize TOML into Config
        let cfg: Config = toml::from_str(toml_str)?;

        // Check version
        assert_eq!(cfg.version, 1);

        // Check database variant
        assert!(cfg.database.in_memory);

        // Check library source
        assert_eq!(
            cfg.library_source.roots,
            vec![PathBuf::from("/home/sancho20021/Music")]
        );
        assert!(cfg.library_source.follow_symlinks);

        Ok(())
    }

    #[test]
    fn test_parse_file_database_config() -> anyhow::Result<()> {
        let toml_str = r#"
version = 1

[database]
in_memory = false
path = "/tmp/localdex.db"

[library_source]
roots = ["/home/sancho20021/Music"]
follow_symlinks = false
"#;

        let cfg: Config = toml::from_str(toml_str)?;

        // Check version
        assert_eq!(cfg.version, 1);

        // Check database variant
        assert!(!cfg.database.in_memory);
        assert_eq!(cfg.database.path, Some(PathBuf::from("/tmp/localdex.db")));

        // Check library source
        assert_eq!(
            cfg.library_source.roots,
            vec![PathBuf::from("/home/sancho20021/Music")]
        );
        assert!(!cfg.library_source.follow_symlinks);

        Ok(())
    }
}
