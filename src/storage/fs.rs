//! Module to scan music directories in the file system

use anyhow::anyhow;
use walkdir::WalkDir;

use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

use crate::{
    config::{self, Location},
    domain::hash::TrackId,
    storage::{error::StorageError, usb::LocationResolver},
};

const MUSIC_EXTENSIONS: &[&str] = &["mp3", "flac", "wav", "m4a", "ogg", "aac"];

pub fn is_music_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| MUSIC_EXTENSIONS.contains(&ext.to_lowercase().as_str()))
        .unwrap_or(false)
}

#[derive(Debug)]
pub struct FileStorage {
    pub loc_resolver: LocationResolver,
}

impl FileStorage {
    pub fn new() -> Self {
        Self {
            loc_resolver: LocationResolver::default(),
        }
    }

    pub fn scan(&mut self, config: &config::LibrarySource) -> Result<FsSnapshot, StorageError> {
        let observed_at = SystemTime::now();
        let files = self.scan_dirs(config.follow_symlinks, &config.roots, &config.ignored_dirs)?;
        Ok(FsSnapshot { observed_at, files })
    }

    /// Recursively scans all music files in given directories. Retrieves their paths and track ids
    pub fn scan_dirs(
        &mut self,
        follow_symlinks: bool,
        roots: &Vec<Location>,
        ignored_dirs: &[PathBuf],
    ) -> Result<Vec<ObservedFile>, StorageError> {
        let scanned_dirs = roots
            .iter()
            .map(|root| {
                println!("Scanning {root}");
                self.scan_dir(follow_symlinks, root, ignored_dirs)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(scanned_dirs.into_iter().flatten().collect())
    }

    /// Recursively scans all music files in the given directory. Retrieves their paths and track ids
    pub fn scan_dir(
        &mut self,
        follow_symlinks: bool,
        root: &Location,
        ignored_dirs: &[PathBuf],
    ) -> Result<Vec<ObservedFile>, StorageError> {
        let root_path = self.loc_resolver.resolve(root).map_err(|e| {
            StorageError::Internal(anyhow!("failed to resolve library source root: {e}"))
        })?;
        let root_str = root_path.to_string_lossy();

        let walker = WalkDir::new(&root_path).follow_links(follow_symlinks);

        let paths = walker
            // filter out ignored directories
            .into_iter()
            .filter_entry(|entry| {
                let entry_path = entry.path();
                // keep the entry if it's not inside any ignored directory
                !ignored_dirs
                    .iter()
                    .any(|ignored| entry_path.starts_with(ignored))
            })
            .filter_map(|e| match e {
                Ok(e) => Some(e),
                Err(err) => {
                    println!("error while scanning dir {root_str}, skipping an entry: {err:?}");
                    None
                }
            })
            .map(|e| e.path().to_path_buf())
            .filter(|e| is_music_file(e))
            .map(|p| -> Result<_, StorageError> {
                let rel = p.strip_prefix(&root_path).map_err(|_| {
                    StorageError::Internal(anyhow!(
                        "Bug: Failed to strip root prefix when scanning dir"
                    ))
                })?;
                let loc = root.join(rel);
                Ok((p, loc))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let ids = paths.iter().map(|(path, _)| TrackId::from_file(path));

        paths
            .iter()
            .zip(ids)
            .map(|((_, path), id)| Ok(ObservedFile::new(id?, path.clone())))
            .collect()
    }
}

#[derive(Debug)]
pub struct FsSnapshot {
    pub observed_at: SystemTime,
    pub files: Vec<ObservedFile>,
}

impl FsSnapshot {}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ObservedFile {
    pub track_id: TrackId,
    pub loc: Location,
}

impl ObservedFile {
    pub fn new(id: TrackId, loc: Location) -> Self {
        Self { track_id: id, loc }
    }
}

/// Best-effort check that a path points to a real, playable music file.
///
/// This does NOT decode audio, but rules out:
/// - missing paths
/// - directories / special files
/// - wrong extensions
/// - empty files
/// - unreadable files
pub fn is_valid_music_path(path: &Path) -> bool {
    // Must exist and be a file
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return false,
    };

    if !meta.is_file() {
        return false;
    }

    // Must look like music by extension
    if !is_music_file(path) {
        return false;
    }

    // Must not be empty
    if meta.len() == 0 {
        return false;
    }

    // Must be readable (cheap probe)
    std::fs::File::open(path).is_ok()
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{
        config::{self, Location},
        storage::fs::FileStorage,
    };

    #[test]
    fn scan_finds_music_files_and_hashes_them() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let root_path = tmp.path().to_path_buf();
        let root = Location::from_path(&root_path);

        // fake files
        let song1 = root_path.join("song1.mp3");
        let song2 = root_path.join("song2.flac");
        let not_music = root_path.join("notes.txt");

        std::fs::write(&song1, b"aaa").unwrap();
        std::fs::write(&song2, b"bbb").unwrap();
        std::fs::write(&not_music, b"ccc").unwrap();

        let files = FileStorage::new().scan_dir(false, &root, &[]).unwrap();

        assert_eq!(files.len(), 2);

        let paths: Vec<_> = files
            .iter()
            .map(|f| f.loc.as_path())
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(paths.contains(&song1));
        assert!(paths.contains(&song2));
    }

    #[test]
    fn scan_dirs_scans_multiple_directories() -> anyhow::Result<()> {
        use std::fs;
        use tempfile::TempDir;

        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();

        let song1 = dir1.path().join("a.mp3");
        let song2 = dir2.path().join("b.flac");
        let not_music = dir2.path().join("notes.txt");

        fs::write(&song1, b"song one").unwrap();
        fs::write(&song2, b"song two").unwrap();
        fs::write(&not_music, b"ignore me").unwrap();

        let config = config::LibrarySource {
            follow_symlinks: false,
            roots: vec![
                Location::from_path(dir1.path()),
                Location::from_path(dir2.path()),
            ],
            ignored_dirs: vec![],
        };

        let snapshot = FileStorage::new().scan(&config).unwrap();

        assert_eq!(snapshot.files.len(), 2);

        let paths: Vec<_> = snapshot
            .files
            .iter()
            .map(|f| f.loc.as_path())
            .collect::<Result<_, _>>()?;
        assert!(paths.contains(&song1));
        assert!(paths.contains(&song2));
        Ok(())
    }

    #[test]
    fn scan_respects_ignored_dirs() -> anyhow::Result<()> {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Music files
        let song1 = root.join("song1.mp3");
        let song2 = root.join("song2.flac");

        // Ignored directory
        let ignored_dir = root.join("ignored");
        std::fs::create_dir_all(&ignored_dir).unwrap();
        let ignored_song = ignored_dir.join("ignored_song.mp3");

        std::fs::write(&song1, b"aaa").unwrap();
        std::fs::write(&song2, b"bbb").unwrap();
        std::fs::write(&ignored_song, b"ccc").unwrap();

        let files = FileStorage::new()
            .scan_dir(false, &Location::from_path(root), &[ignored_dir.clone()])
            .unwrap();

        // Should find only the two non-ignored music files
        assert_eq!(files.len(), 2);

        let paths: Vec<_> = files
            .iter()
            .map(|f| f.loc.as_path())
            .collect::<Result<_, _>>()?;
        assert!(paths.contains(&song1));
        assert!(paths.contains(&song2));
        assert!(!paths.contains(&ignored_song));
        Ok(())
    }
}
