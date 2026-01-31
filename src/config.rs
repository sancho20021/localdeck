use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub version: u32,
    pub database: Database,
    pub library: Library,
    pub scan: Scan,
}

impl Config {
    pub fn load(path: &str) -> Config {
        let contents = std::fs::read_to_string(path).expect("Failed to read user config");
        toml::from_str(&contents).expect("Failed to parse config TOML")
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Database {
    InMemory,
    InFile(PathBuf),
}

#[derive(Debug, Deserialize)]
pub struct Library {
    pub roots: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct Scan {
    pub follow_symlinks: bool,
}
