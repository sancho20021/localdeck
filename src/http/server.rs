use anyhow::anyhow;
use log::info;
use rouille::{Request, Response, url};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::TcpStream,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
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
        rouille::start_server(addr, move |request| self.handle_request(request));
    }

    fn handle_request(&self, request: &Request) -> Response {
        Self::log_request(request);

        let response = rouille::router!(request,
            (GET) (/tracks/{id: String}) => {
                Self::handle_get_track(id, &self.storage)
            },

            (GET) (/tracks/{id: String}/stream) => {
                Self::handle_get_track_stream(id, &self.storage)
            },
            (GET) (/listen/{_id: String}) => {
                        Self::handle_listen_page()
            },
            (GET) (/play) => {
                self.handle_play(request)
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

    /// returns Response with ok status, or ApiError
    fn get_track_stream(id: String, storage: &Arc<Mutex<Storage>>) -> Result<Response, ApiError> {
        let track_id = TrackId::from_hex(&id).map_err(|_| StorageError::InvalidTrackId)?;

        // Lock storage and fetch track
        let mut storage = storage.lock().map_err(|e| {
            StorageError::Internal(anyhow!(
                "Could not access localdeck storage under lock: {e}"
            ))
        })?;
        let (track, path) = storage.get_track(track_id)?;
        let mime = Self::mime_for_track(&path);

        let file = std::fs::File::open(&path).map_err(StorageError::Fs)?;
        log::debug!(
            "STREAM {} -> 200 OK, path: {}, MIME type: {}",
            id,
            path.to_string_lossy(),
            mime
        );

        Ok(Response::from_file(mime, file)
            .with_additional_header("X-Track-Artist", track.metadata.artist.unwrap_or_default())
            .with_additional_header("X-Track-Title", track.metadata.title.unwrap_or_default()))
    }

    fn handle_get_track_stream(id: String, storage: &Arc<Mutex<Storage>>) -> Response {
        match Self::get_track_stream(id, storage) {
            Ok(r) => r,
            Err(e) => e.into_response(),
        }
    }

    fn handle_listen_page() -> Response {
        Response::html(include_str!("../../html/stream.html"))
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

    /// tries to stream just like /track/stream route.
    ///
    /// if fails, offers to redirect to youtube
    fn handle_play(&self, request: &Request) -> Response {
        let hash = if let Some(hash) = request.get_param("h") {
            hash
        } else {
            return Response::text("Error: missing media hash").with_status_code(400);
        };

        let youtube_id = request.get_param("y");

        match Self::get_track_stream(hash, &self.storage) {
            Ok(resp) => resp,

            Err(err) => {
                let error_text = err.to_string();
                let status = err.status_code();

                self.render_fallback(youtube_id.as_deref(), status, &error_text)
            }
        }
    }

    fn render_fallback(&self, youtube_id: Option<&str>, status: u16, reason: &str) -> Response {
        match youtube_id {
            Some(yid) => {
                let template = include_str!("../../html/offer_yt.html");
                Response::html(
                    template
                        .replace("{{YT_URL}}", &format!("https://youtu.be/{}", yid))
                        .replace("{{STATUS}}", &status.to_string())
                        .replace("{{REASON}}", reason),
                )
            }
            None => {
                let template = include_str!("../../html/no_track.html");
                Response::html(
                    template
                        .replace("{{STATUS}}", &status.to_string())
                        .replace("{{REASON}}", reason),
                )
            }
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

    pub fn parse_text_response(response: rouille::Response) -> String {
        let mut buf = String::new();
        let mut reader = response.data.into_reader_and_size().0;
        reader.read_to_string(&mut buf);
        buf
    }

    fn create_server(db: &Arc<Mutex<Storage>>) -> HttpServer {
        HttpServer {
            storage: Arc::clone(db),
            config: HttpConfig {
                bind_addr: "0.0.0.0".to_string(),
                port: 8080,
            },
        }
    }

    fn mock_trackid(x: i32) -> TrackId {
        let bytes = x.to_be_bytes();
        TrackId::from_bytes(&bytes)
    }

    fn mock_trackid_str(x: i32) -> String {
        mock_trackid(x).to_hex()
    }

    fn create_server_with_track(track_id: i32, path: &str) -> HttpServer {
        let storage = setup_storage().unwrap();

        {
            let locked = storage.lock().unwrap();

            locked
                .db
                .execute(
                    &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
                    params![mock_trackid_str(track_id), path],
                )
                .unwrap();
        }
        create_server(&storage)
    }

    fn create_empty_server() -> HttpServer {
        let storage = setup_storage().unwrap();
        create_server(&storage)
    }

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

        let response = create_server(&storage).handle_request(&request);

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

        let response = create_server(&storage).handle_request(&request);

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

        let response = create_server(&storage).handle_request(&request);

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

        let response = create_server(&storage).handle_request(&request);

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

        let response = create_server(&storage).handle_request(&request);

        assert_eq!(response.status_code, 404);

        Ok(())
    }

    #[test]
    fn test_http_get_track_stream_invalid_id() -> anyhow::Result<()> {
        let storage = setup_storage()?;

        let request = Request::fake_http("GET", "/tracks/not-a-valid-id/stream", vec![], vec![]);

        let response = create_server(&storage).handle_request(&request);

        assert_eq!(response.status_code, 400);

        Ok(())
    }

    #[test]
    fn test_play_streams_track_successfully() {
        let server = create_server_with_track(123, "somewhere"); // storage contains track

        let request = Request::fake_http("GET", "/play?h=123", vec![], vec![]);

        let response = server.handle_request(&request);
        let status = response.status_code;

        assert!(
            status == 200 || status == 206,
            "expected streaming response, got {}. response: {}",
            status,
            parse_text_response(response)
        );
    }

    #[test]
    fn test_play_fallback_with_youtube() {
        let server = create_empty_server(); // no tracks in storage

        let request = Request::fake_http("GET", "/play?h=123&y=dQw4w9WgXcQ", vec![], vec![]);

        let response = server.handle_request(&request);
        let status = response.status_code;

        assert_eq!(
            status,
            200,
            "expected fallback html, got {}. response: {}",
            status,
            parse_text_response(response)
        );

        let body = parse_text_response(response);

        assert!(
            body.contains("youtu.be/dQw4w9WgXcQ"),
            "expected youtube link in fallback, got: {}",
            body
        );
    }

    #[test]
    fn test_play_fallback_without_youtube() {
        let server = create_empty_server(); // no tracks in storage

        let request = Request::fake_http("GET", "/play?h=123", vec![], vec![]);

        let response = server.handle_request(&request);
        let status = response.status_code;

        assert_eq!(
            status,
            200,
            "expected fallback html, got {}. response: {}",
            status,
            parse_text_response(response)
        );

        let body = parse_text_response(response);

        assert!(
            body.contains("No YouTube link"),
            "expected no-youtube fallback message, got: {}",
            body
        );
    }

    #[test]
    fn test_play_missing_hash() {
        let server = create_empty_server();

        let request = Request::fake_http("GET", "/play", vec![], vec![]);

        let response = server.handle_request(&request);
        let status = response.status_code;

        assert_eq!(
            status,
            400,
            "expected 400 for missing hash, got {}. response: {}",
            status,
            parse_text_response(response)
        );

        let body = parse_text_response(response);

        assert!(
            body.contains("missing media hash"),
            "expected missing-hash error, got: {}",
            body
        );
    }
}
