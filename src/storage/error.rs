use thiserror::Error;

use crate::domain::hash::TrackId;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("track {0} not found")]
    TrackNotFound(TrackId),

    #[error("track {track} has no valid music files")]
    InvalidTrackFile { track: TrackId },

    #[error("filesystem error: {0}")]
    Fs(#[from] std::io::Error),

    #[error("invalid track id")]
    InvalidTrackId,

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}
