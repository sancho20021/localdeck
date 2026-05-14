use thiserror::Error;

use crate::{location::Location, track_id::TrackId};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("track {0} not found")]
    TrackNotFound(TrackId),

    #[error("track {track} has no valid music files: {extra}")]
    InvalidTrackFile { track: TrackId, extra: String },

    #[error("filesystem error: {0}")]
    Fs(#[from] std::io::Error),

    #[error("invalid track id: {0}")]
    InvalidTrackId(String),

    #[error("duplicate location error, location: {path}, hint: {hint}")]
    DuplicateLocation { path: Location, hint: String },

    #[error("internal error: {0}")]
    Internal(anyhow::Error),
    #[error("not allowed to modify metadata of track {0}")]
    MetadataOverwriteDenied(TrackId),

    #[error("required metadata (title, artist, ...) not provided for track {0}")]
    RequiredMetaMissing(TrackId),
}
