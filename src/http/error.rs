use rouille::Response;

use crate::storage::error::StorageError;

#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
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

impl ApiError {
    pub fn into_response(self) -> Response {
        match self {
            ApiError::NotFound(msg) =>
                Response::text(msg).with_status_code(404),

            ApiError::BadRequest(msg) =>
                Response::text(msg).with_status_code(400),

            ApiError::Internal(msg) =>
                Response::text(msg).with_status_code(500),
        }
    }
}
