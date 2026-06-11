//! Module to scan music directories in the file system

use anyhow::anyhow;
use walkdir::WalkDir;

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use crate::{
    config::{self, LibrarySource},
    error::StorageError,
    file_hash::FileHash,
    location::Location,
    usb::LocationResolver,
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
    config: LibrarySource,
}

impl FileStorage {
    pub fn new(config: LibrarySource) -> Self {
        Self {
            loc_resolver: LocationResolver::default(),
            config,
        }
    }

    /// Recursively scans all music files in given directories. Retrieves their paths and metadata
    pub fn scan(&mut self) -> Result<FsSnapshot, StorageError> {
        let roots: Vec<Location> = self.config.roots.clone();
        let scanned_dirs = roots
            .iter()
            .map(|root| {
                println!("Scanning {root}");
                self.scan_dir(root)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(scanned_dirs.into_iter().flatten().collect())
    }

    /// Recursively scans all music files in the given directory. Retrieves their paths and metadata
    pub fn scan_dir(&mut self, root: &Location) -> Result<Vec<FileWithMeta>, StorageError> {
        let root_path = self.loc_resolver.resolve(root).map_err(|e| {
            StorageError::Internal(anyhow!("failed to resolve library source root: {e}"))
        })?;
        let root_str = root_path.to_string_lossy();

        let walker = WalkDir::new(&root_path).follow_links(self.config.follow_symlinks);

        walker
            // filter out ignored directories
            .into_iter()
            .filter_entry(|entry| {
                let entry_path = entry.path();
                // keep the entry if it's not inside any ignored directory
                !self
                    .config
                    .ignored_dirs
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
            .map(|e| {
                let pathbuf = e.path().to_path_buf();
                (e, pathbuf)
            })
            .filter(|(_, p)| is_music_file(p))
            .map(|(e, p)| -> Result<_, StorageError> {
                let metadata = e.metadata().map_err(|e| {
                    StorageError::Internal(anyhow!(
                        "Failed to get metadata of file {}: {}",
                        p.to_string_lossy(),
                        e
                    ))
                })?;

                let file_size = metadata.len() as i64;

                let rel = p.strip_prefix(&root_path).map_err(|_| {
                    StorageError::Internal(anyhow!(
                        "Bug: Failed to strip root prefix when scanning dir"
                    ))
                })?;
                let loc = root.join(rel);
                Ok(FileWithMeta { loc, file_size })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Takes a physical system path and maps it back to a logical library Location
    /// based on the currently configured roots.
    pub fn reverse_resolve(&mut self, physical_path: &Path) -> Result<Location, StorageError> {
        let target = physical_path.canonicalize()?;

        // Iterate through all roots defined in your config
        for root in &self.config.roots {
            // Resolve the physical base path of this specific root configuration
            if let Ok(base_path) = self.loc_resolver.resolve(root) {
                if let Ok(canonical_base) = base_path.canonicalize() {
                    // If our target physical path starts with this base path, we found our home
                    if let Ok(relative_path) = target.strip_prefix(&canonical_base) {
                        return Ok(root.join(relative_path));
                    }
                }
            }
        }
        Err(StorageError::PathOutsideLibrary(target))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct HashedFile {
    pub hash: FileHash,
    pub file: FileWithMeta,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct FileWithMeta {
    pub loc: Location,
    /// Files size in bytes
    pub file_size: i64,
}

impl FileWithMeta {
    pub fn size_mb(&self) -> f32 {
        ((self.file_size / 1024) as f32) / 1024.
    }
}

pub type FsSnapshot = HashSet<FileWithMeta>;

impl HashedFile {
    pub fn new(id: FileHash, file: FileWithMeta) -> Self {
        Self { hash: id, file }
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

    use crate::{config::LibrarySource, error::StorageError, fs::FileStorage, location::Location};

    #[test]
    fn scan_finds_music_files() {
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

        let files = FileStorage::new(LibrarySource {
            roots: vec![root.clone()],
            follow_symlinks: false,
            ignored_dirs: vec![],
        })
        .scan_dir(&root)
        .unwrap();

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

        let config = LibrarySource {
            follow_symlinks: false,
            roots: vec![
                Location::from_path(dir1.path()),
                Location::from_path(dir2.path()),
            ],
            ignored_dirs: vec![],
        };

        let snapshot = FileStorage::new(config).scan().unwrap();

        assert_eq!(snapshot.len(), 2);

        let paths: Vec<_> = snapshot
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

        let files = FileStorage::new(LibrarySource {
            roots: vec![Location::from_path(root)],
            follow_symlinks: false,
            ignored_dirs: vec![ignored_dir.clone()],
        })
        .scan_dir(&Location::from_path(root))
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

    #[test]
    fn test_reverse_resolve_success() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let root_path = tmp.path().to_path_buf();
        let root = Location::from_path(&root_path);

        // Create a physical file down inside the directory structure
        let song = root_path.join("album").join("song.mp3");
        std::fs::create_dir_all(song.parent().unwrap()).unwrap();
        std::fs::write(&song, b"aaa").unwrap();

        let mut fs_storage = FileStorage::new(LibrarySource {
            roots: vec![root.clone()],
            follow_symlinks: false,
            ignored_dirs: vec![],
        });

        // Act: Map the absolute physical path back to a structured Location
        let resolved_loc = fs_storage.reverse_resolve(&song).unwrap();

        // Assert: It should match our original root plus the relative sub-path
        assert_eq!(
            resolved_loc,
            Location::File {
                path: root_path.join("album").join("song.mp3")
            }
        );
    }

    #[test]
    fn test_reverse_resolve_fails_outside_library() {
        use tempfile::TempDir;

        let tmp_library = TempDir::new().unwrap();
        let tmp_outside = TempDir::new().unwrap();

        let library_path = tmp_library.path().join("music");
        std::fs::create_dir_all(&library_path).unwrap();

        // Create a file completely outside the configured library folders
        let outside_file = tmp_outside.path().join("outside_song.mp3");
        std::fs::write(&outside_file, b"bbb").unwrap();

        let mut fs_storage = FileStorage::new(LibrarySource {
            roots: vec![Location::from_path(&library_path)],
            follow_symlinks: false,
            ignored_dirs: vec![],
        });

        // Act
        let result = fs_storage.reverse_resolve(&outside_file);

        // Assert: It must fail with PathOutsideLibrary
        assert!(result.is_err());
        match result {
            Err(StorageError::PathOutsideLibrary(failed_path)) => {
                assert_eq!(failed_path, outside_file.canonicalize().unwrap());
            }
            _ => panic!("Expected StorageError::PathOutsideLibrary error variant"),
        }
    }
}
