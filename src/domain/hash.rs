use std::{any, path::Path};

use blake3::Hash;

/// Represents the track ID.
///
/// One can get track ID from a music file,
/// and then use it to search for that file in the filesystem.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TrackId(pub Hash);

impl TrackId {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes))
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }

    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        Ok(Self(blake3::hash(path)?))
    }
}
