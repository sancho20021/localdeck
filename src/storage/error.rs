use crate::{location::Location, storage::usb::ResolveError};

use thiserror::Error;

use crate::domain::hash::TrackId;

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

    #[error("invalid track id")]
    InvalidTrackId,

    #[error("duplicate location error, location: {path}, hint: {hint}")]
    DuplicateLocation { path: Location, hint: String },

    #[error("internal error: {0}")]
    Internal(anyhow::Error),
    #[error("not allowed to modify metadata of track {0}")]
    MetadataOverwriteDenied(TrackId),

    #[error("required metadata (title, artist, ...) not provided for track {0}")]
    RequiredMetaMissing(TrackId),
}
