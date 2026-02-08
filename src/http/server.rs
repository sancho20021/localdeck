use log::info;
use rouille::{Request, Response};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    config::HttpConfig,
    domain::{hash::TrackId, track::Track},
    http::error::ApiError,
    storage::{error::StorageError, operations::Storage},
};

pub struct HttpServer {
    storage: Arc<Mutex<Storage>>,
    pub config: HttpConfig,
}

impl HttpServer {
    pub fn new(storage: Storage, config: HttpConfig) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
            config,
        }
    }

    pub fn run(self) {
        let addr = format!("{}:{}", self.config.bind_addr, self.config.port);
        let storage = self.storage.clone();

        rouille::start_server(addr, move |request| Self::handle_request(request, &storage));
    }

    fn handle_request(request: &Request, storage: &Arc<Mutex<Storage>>) -> Response {
        Self::log_request(request);

        let response = rouille::router!(request,
            (GET) (/tracks/{id: String}) => {
                Self::handle_get_track(id, storage)
            },

            (GET) (/tracks/{id: String}/stream) => {
                Self::handle_get_track_stream(id, storage)
            },
            (GET) (/listen/{_id: String}) => {
                        Self::handle_listen_page()
            },

            _ => Response::empty_404()
        );

        info!("Response: {} {}", request.method(), response.status_code);
        response
    }

    fn log_request(request: &Request) {
        info!("{} {}", request.method(), request.url());
    }

    fn handle_get_track(id: String, storage: &Arc<Mutex<Storage>>) -> Response {
        let track_id = match TrackId::from_hex(&id) {
            Ok(id) => id,
            Err(_) => return ApiError::from(StorageError::InvalidTrackId).into_response(),
        };

        let result = {
            let mut storage = storage.lock().unwrap();
            storage.get_track(track_id)
        };

        match result {
            Ok((track, path)) => Response::json(&TrackResponse::from_domain(&track, path)),

            Err(e) => ApiError::from(e).into_response(),
        }
    }

    fn handle_get_track_stream(id: String, storage: &Arc<Mutex<Storage>>) -> Response {
        let track_id = match TrackId::from_hex(&id) {
            Ok(id) => id,
            Err(_) => return ApiError::from(StorageError::InvalidTrackId).into_response(),
        };

        // Lock storage and fetch track
        let result = {
            let mut storage = storage.lock().unwrap();
            storage.get_track(track_id)
        };

        match result {
            Ok((track, path)) => {
                // Return file as stream
                let mime = Self::mime_for_track(&path);

                match std::fs::File::open(&path) {
                    Ok(file) => {
                        log::debug!(
                            "STREAM {} -> 200 OK, path: {}, MIME type: {}",
                            id,
                            path.to_string_lossy(),
                            mime
                        );
                        let response = Response::from_file(mime, file)
                            .with_additional_header(
                                "X-Track-Artist",
                                track.metadata.artist.unwrap_or_default(),
                            )
                            .with_additional_header(
                                "X-Track-Title",
                                track.metadata.title.unwrap_or_default(),
                            );
                        response
                    }
                    Err(e) => ApiError::from(StorageError::Fs(e)).into_response(),
                }
            }

            Err(e) => ApiError::from(e).into_response(),
        }
    }

    fn handle_listen_page() -> Response {
        Response::html(include_str!("../../html/index.html"))
    }

    fn mime_for_track(path: &PathBuf) -> String {
        let ext = path
            .extension()
            .map(|ext| ext.to_string_lossy())
            .map(|s| s.to_lowercase());
        let default = || {
            mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string()
        };
        ext.and_then(|ext| Self::mime_from_ext(ext.as_str()))
            .unwrap_or_else(default)
    }

    /// Map file extension (without dot) to proper MIME type for browser playback.
    /// Returns None if the extension is not recognized.
    pub fn mime_from_ext(ext: &str) -> Option<String> {
        match ext {
            "m4a" => Some("audio/x-m4a".to_string()), // Safari iOS compatible
            "aac" => Some("audio/aac".to_string()),
            "mp3" => Some("audio/mpeg".to_string()),
            "wav" => Some("audio/wav".to_string()),
            "ogg" => Some("audio/ogg".to_string()),
            "flac" => Some("audio/flac".to_string()),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TrackResponse {
    track_id: String,
    path: PathBuf,
    metadata: TrackMetadataResponse,
}

#[derive(Serialize, Deserialize)]
struct TrackMetadataResponse {
    artist: Option<String>,
    title: Option<String>,
}

impl TrackResponse {
    fn from_domain(track: &Track, path: PathBuf) -> Self {
        Self {
            track_id: track.id.to_hex(),
            path,
            metadata: TrackMetadataResponse {
                artist: track.metadata.artist.clone(),
                title: track.metadata.title.clone(),
            },
        }
    }
}

#[cfg(test)]
pub fn parse_json_response<T: serde::de::DeserializeOwned>(
    response: rouille::Response,
) -> anyhow::Result<T> {
    Ok(serde_json::from_reader(
        response.data.into_reader_and_size().0,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::hash::TrackId,
        storage::operations::Storage,
        storage::schema::{self, FILES, PATH, TRACK_ID},
    };

    use rouille::Request;
    use rusqlite::{Connection, params};
    use std::{
        fs,
        sync::{Arc, Mutex},
    };
    use tempfile::tempdir;

    fn setup_storage() -> anyhow::Result<Arc<Mutex<Storage>>> {
        let conn = Connection::open_in_memory()?;
        schema::init(&conn)?;
        Ok(Arc::new(Mutex::new(Storage::from_existing_conn(
            conn,
            Default::default(),
        ))))
    }

    // --------------------------------------------------
    // ✅ SUCCESS
    // --------------------------------------------------

    #[test]
    fn test_http_get_track_success() -> anyhow::Result<()> {
        let storage = setup_storage()?;

        let dir = tempdir()?;
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"x")?;

        let track_id = TrackId::from_file(&file_path)?;

        {
            let locked = storage.lock().unwrap();

            locked.db.execute(
                &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
                params![track_id.to_hex(), file_path.to_string_lossy()],
            )?;
        }

        let request = Request::fake_http(
            "GET",
            format!("/tracks/{}", track_id.to_hex()),
            vec![],
            vec![],
        );

        let response = HttpServer::handle_request(&request, &storage);

        assert_eq!(response.status_code, 200);

        let body: TrackResponse = parse_json_response(response)?;

        assert_eq!(body.track_id, track_id.to_hex());
        assert_eq!(body.path, file_path);

        Ok(())
    }

    // --------------------------------------------------
    // ❌ INVALID TRACK ID
    // --------------------------------------------------

    #[test]
    fn test_http_get_track_invalid_id() -> anyhow::Result<()> {
        let storage = setup_storage()?;

        let request = Request::fake_http("GET", "/tracks/not-a-valid-id", vec![], vec![]);

        let response = HttpServer::handle_request(&request, &storage);

        assert_eq!(response.status_code, 400);

        Ok(())
    }

    // --------------------------------------------------
    // ❌ TRACK NOT IN DB
    // --------------------------------------------------

    #[test]
    fn test_http_get_track_not_found() -> anyhow::Result<()> {
        let storage = setup_storage()?;

        let track_id = TrackId::from_bytes(&[0, 1, 3]).to_hex();

        let request = Request::fake_http("GET", format!("/tracks/{}", track_id), vec![], vec![]);

        let response = HttpServer::handle_request(&request, &storage);

        assert_eq!(response.status_code, 404);

        Ok(())
    }

    #[test]
    fn test_http_get_track_stream_success() -> anyhow::Result<()> {
        let storage = setup_storage()?;

        let dir = tempdir()?;
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"x")?;

        let track_id = TrackId::from_file(&file_path)?;

        {
            let locked = storage.lock().unwrap();

            locked.db.execute(
                &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
                params![track_id.to_hex(), file_path.to_string_lossy()],
            )?;
        }

        let request = Request::fake_http(
            "GET",
            format!("/tracks/{}/stream", track_id.to_hex()),
            vec![],
            vec![],
        );

        let response = HttpServer::handle_request(&request, &storage);

        assert_eq!(response.status_code, 200);

        // Read the response body bytes to check content
        let mut body = Vec::new();
        response
            .data
            .into_reader_and_size()
            .0
            .read_to_end(&mut body)?;

        assert_eq!(body, b"x");

        Ok(())
    }

    #[test]
    fn test_http_get_track_stream_not_found() -> anyhow::Result<()> {
        let storage = setup_storage()?;
        let track_id = TrackId::from_bytes(&[0, 1, 3]);

        let request = Request::fake_http(
            "GET",
            format!("/tracks/{}/stream", track_id.to_hex()),
            vec![],
            vec![],
        );

        let response = HttpServer::handle_request(&request, &storage);

        assert_eq!(response.status_code, 404);

        Ok(())
    }

    #[test]
    fn test_http_get_track_stream_invalid_id() -> anyhow::Result<()> {
        let storage = setup_storage()?;

        let request = Request::fake_http("GET", "/tracks/not-a-valid-id/stream", vec![], vec![]);

        let response = HttpServer::handle_request(&request, &storage);

        assert_eq!(response.status_code, 400);

        Ok(())
    }
}
