use std::path::Path;

use anyhow::Context;
use blake3::Hash;

/// Represents the track ID.
///
/// One can get track ID from a music file,
/// and then use it to search for that file in the filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrackId(pub Hash);

impl std::fmt::Display for TrackId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl TrackId {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes))
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }

    pub fn from_hex<S: AsRef<[u8]>>(hex: S) -> anyhow::Result<Self> {
        Ok(Self(
            blake3::Hash::from_hex(hex).with_context(|| "Failed to parse track id")?,
        ))
    }

    /// reads file and hashes it
    pub fn from_file(path: &Path) -> Result<Self, std::io::Error> {
        let contents =
            std::fs::read(path)?;
        Ok(Self::from_bytes(&contents))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::domain::hash::TrackId;

    #[test]
    fn same_contents_same_hash() {
        let tmp = TempDir::new().unwrap();
        let a = tmp.path().join("a.mp3");
        let b = tmp.path().join("b.mp3");

        std::fs::write(&a, b"same").unwrap();
        std::fs::write(&b, b"same").unwrap();

        let ha = TrackId::from_file(&a).unwrap();
        let hb = TrackId::from_file(&b).unwrap();

        assert_eq!(ha, hb);
    }

    #[test]
    fn different_contents_different_hash() {
        let tmp = TempDir::new().unwrap();
        let a = tmp.path().join("a.mp3");
        let b = tmp.path().join("b.mp3");

        std::fs::write(&a, b"something").unwrap();
        std::fs::write(&b, b"soomething").unwrap();

        let ha = TrackId::from_file(&a).unwrap();
        let hb = TrackId::from_file(&b).unwrap();

        assert_ne!(ha, hb);
    }
}
