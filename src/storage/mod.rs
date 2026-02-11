use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use anyhow::Context;

use crate::{config::Location, storage::usb::find_mount_by_label};

pub mod db;
pub mod error;
mod fs;
pub mod operations;
pub(crate) mod schema;
pub mod usb;

fn resolve_location(location: &Location) -> Result<PathBuf, anyhow::Error> {
    match location {
        Location::File { path } => Ok(path.clone()),
        Location::Usb { label, path } => {
            let mount = find_mount_by_label(label)
                .with_context(|| format!("USB with label '{label}' not found"))?;

            return Ok(mount.join(path));
        }
    }
}

impl Location {
    pub fn from_path<P: AsRef<Path>>(p: P) -> Self {
        Self::File {
            path: p.as_ref().to_path_buf(),
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
