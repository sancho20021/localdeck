use thiserror::Error;

use crate::track::TrackId;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("track {0} not found")]
    TrackNotFound(String),

    #[error("track {track} has no valid music files: {extra}")]
    InvalidTrackFile { track: TrackId, extra: String },

    #[error("filesystem error: {0}")]
    Fs(#[from] std::io::Error),

    #[error("internal error: {0}")]
    Internal(anyhow::Error),
    #[error("not allowed to modify metadata of track {0}")]
    MetadataOverwriteDenied(TrackId),

    #[error("track {0} has no metadata yet, creating it requires both title and artist")]
    RequiredMetaMissing(TrackId),

    #[error(
        "Slave track {0} contains metadata. Set ignore_slave_meta to true to overwrite or discard it."
    )]
    SlaveTrackHasMetadata(TrackId),

    #[error("The path '{0}' is outside of all configured library directories and USB roots.")]
    PathOutsideLibrary(std::path::PathBuf),

    #[error(
        "file '{0}' is recorded in the database but does not exist on disk. Remove the record with `forget`. If you moved the file instead, run `update` before `forget`."
    )]
    FileMissing(std::path::PathBuf),

    #[error("'{0}' is not a regular file (symlink or directory), refusing to touch it")]
    NotARegularFile(std::path::PathBuf),

    #[error("file '{0}' changed since the last update, run `update` first")]
    FileChangedSinceSync(std::path::PathBuf),
}
