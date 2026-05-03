use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
#[serde(tag = "type")]
pub enum Location {
    File { path: PathBuf },
    Usb { label: String, path: PathBuf },
}

impl Location {
    pub fn from_path<P: AsRef<Path>>(p: P) -> Self {
        Self::File {
            path: p.as_ref().to_path_buf(),
        }
    }

    pub fn as_path(&self) -> anyhow::Result<PathBuf> {
        match self {
            Location::File { path } => Ok(path.clone()),
            Location::Usb { .. } => Err(anyhow!(
                "Location includes usb label, can't unpack as simple path"
            )),
        }
    }
    pub fn join(&self, rel: &Path) -> Self {
        match self {
            Location::Usb { label, path } => Location::Usb {
                label: label.clone(),
                path: path.join(rel),
            },
            Location::File { path } => Location::File {
                path: path.join(rel),
            },
        }
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Location::File { path } => write!(f, "{}", path.to_string_lossy()),
            Location::Usb { label, path } => write!(f, "USB({})/{}", label, path.to_string_lossy()),
        }
    }
}
