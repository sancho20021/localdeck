//! Module to observe music directories in the file system

use walkdir::WalkDir;

use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use crate::{config, domain::hash::TrackId};

const MUSIC_EXTENSIONS: &[&str] = &["mp3", "flac", "wav", "m4a", "ogg", "aac"];

pub fn is_music_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| MUSIC_EXTENSIONS.contains(&ext.to_lowercase().as_str()))
        .unwrap_or(false)
}

#[derive(Debug)]
pub struct ScanSnapshot {
    pub observed_at: Instant,
    pub files: Vec<ObservedFile>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ObservedFile {
    pub track_id: TrackId,
    pub path: PathBuf,
}

pub fn scan_dir(config: &config::Scan, root: &PathBuf) -> anyhow::Result<Vec<ObservedFile>> {
    let root_str = root.to_string_lossy();
    let paths = WalkDir::new(root)
        .follow_links(config.follow_symlinks)
        .into_iter()
        .filter_map(|e| {
            if let Ok(e) = e {
                Some(e)
            } else {
                println!("error while scanning dir {root_str}, skipping an entry: {e:?}");
                None
            }
        })
        .map(|e| e.path().to_path_buf())
        .filter(|e| is_music_file(&e))
        .collect::<Vec<_>>();

}
