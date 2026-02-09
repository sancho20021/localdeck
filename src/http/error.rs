use rouille::Response;

use crate::storage::error::StorageError;

#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
    /// invalid byte range requested
    InvalidRange,
}

impl ApiError {
    pub fn status_code(&self) -> u16 {
        match self {
            ApiError::NotFound(_) => 404,
            ApiError::BadRequest(_) => 400,
            ApiError::Internal(_) => 500,
            ApiError::InvalidRange => 416,
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::TrackNotFound(id) => {
                ApiError::NotFound(format!("track {} not found", id))
            }

            StorageError::InvalidTrackFile { track } => {
                ApiError::BadRequest(format!("track {} has no valid files", track))
            }

            StorageError::InvalidTrackId => ApiError::BadRequest("invalid track id".into()),

            StorageError::Database(_) | StorageError::Fs(_) | StorageError::Internal(_) => {
                ApiError::Internal("internal server error".into())
            }
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::NotFound(msg) | ApiError::BadRequest(msg) | ApiError::Internal(msg) => {
                write!(f, "{}", msg)
            }
            ApiError::InvalidRange => {
                write!(f, "invalid byte range")
            }
        }
    }
}

impl ApiError {
    pub fn into_response(self) -> Response {
        Response::text(format!("{self}")).with_status_code(self.status_code())
    }
}
