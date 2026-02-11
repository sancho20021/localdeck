use anyhow::anyhow;
use log::info;
use rouille::{Request, Response};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    config::HttpConfig,
    domain::{hash::TrackId, track::TrackMetadata},
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

    /// Never change the /play route as it will be printed on qrs or nfc
    fn handle_request(&self, request: &Request) -> Response {
        Self::log_request(request);

        let response = rouille::router!(request,
            (GET) (/tracks/{id: String}) => {
                Self::handle_get_track(id, &self.storage)
            },

            (GET) (/tracks/{id: String}/stream) => {
                Self::handle_get_track_stream(id, request, &self.storage)
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
            Ok((track, path, metadata)) => {
                Response::json(&TrackResponse::from_domain(&track, path, metadata))
            }

            Err(e) => ApiError::from(e).into_response(),
        }
    }

    /// streams music file, respecting byterange
    /// returns Response with ok status, or ApiError
    fn get_track_stream(
        id: String,
        request: &Request,
        storage: &Arc<Mutex<Storage>>,
    ) -> Result<Response, ApiError> {
        let track_id = TrackId::from_hex(&id).map_err(|_| StorageError::InvalidTrackId)?;

        let mut storage = storage.lock().map_err(|e| {
            StorageError::Internal(anyhow!(
                "Could not access localdeck storage under lock: {e}"
            ))
        })?;
        let (_, path, meta) = storage.get_track(track_id)?;
        let mime = Self::mime_for_track(&path);

        let mut file = File::open(&path).map_err(StorageError::Fs)?;
        let file_size = file.metadata().map_err(StorageError::Fs)?.len();

        let with_extra_headers = |resp: Response| -> Response {
            let mut resp = resp.with_additional_header("Accept-Ranges", "bytes");

            if let Some(meta) = meta {
                resp = resp
                    .with_additional_header("X-Track-Artist", meta.artist)
                    .with_additional_header("X-Track-Title", meta.title);
            }
            resp
        };

        // ---------------------------------------------
        // Parse Range header if present
        // ---------------------------------------------
        let range_header = request.header("Range");
        if let Some(range) = range_header {
            // Expect something like "bytes=123-456"
            if let Some((start, end)) = Self::parse_http_range(range, file_size)? {
                let chunk_size = end - start + 1;
                let mut buffer = vec![0u8; chunk_size as usize];

                file.seek(SeekFrom::Start(start))
                    .map_err(StorageError::Fs)?;
                file.read_exact(&mut buffer).map_err(StorageError::Fs)?;

                log::debug!(
                    "STREAM {} -> 206 Partial Content, path: {}, MIME type: {}, bytes {}-{}",
                    id,
                    path.to_string_lossy(),
                    mime,
                    start,
                    end
                );

                let resp = with_extra_headers(
                    Response::from_data(mime, buffer)
                        .with_status_code(206)
                        .with_additional_header(
                            "Content-Range",
                            format!("bytes {}-{}/{}", start, end, file_size),
                        ),
                );

                return Ok(resp);
            }
        }

        // No Range header, return full file
        log::debug!(
            "STREAM {} -> 200 OK, path: {}, MIME type: {}",
            id,
            path.to_string_lossy(),
            mime
        );
        Ok(with_extra_headers(Response::from_file(mime, file)))
    }

    /// parse "bytes=start-end" header
    /// Returns (start, end) or error
    fn parse_http_range(range: &str, file_size: u64) -> Result<Option<(u64, u64)>, ApiError> {
        if !range.starts_with("bytes=") {
            return Ok(None);
        }

        let range = &range[6..]; // strip "bytes="
        let parts: Vec<&str> = range.split('-').collect();
        if parts.len() != 2 {
            return Ok(None);
        }

        let start = parts[0].parse::<u64>().unwrap_or(0);
        let end = if !parts[1].is_empty() {
            parts[1].parse::<u64>().unwrap_or(file_size - 1)
        } else {
            file_size - 1
        };

        if start > end || end >= file_size {
            return Err(ApiError::InvalidRange);
        }

        Ok(Some((start, end)))
    }

    fn handle_get_track_stream(
        id: String,
        request: &Request,
        storage: &Arc<Mutex<Storage>>,
    ) -> Response {
        match Self::get_track_stream(id, request, storage) {
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
    /// if fails, offers to redirect to youtube.
    ///
    /// Never change the api interface of this method as this route is used on printed qrs / nfcs.
    fn handle_play(&self, request: &Request) -> Response {
        let hash = if let Some(hash) = request.get_param("h") {
            hash
        } else {
            return Response::text("Error: missing media hash").with_status_code(400);
        };

        let youtube_id = request.get_param("y");

        match Self::get_track_stream(hash, request, &self.storage) {
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
    metadata: Option<TrackMetadataResponse>,
}

#[derive(Serialize, Deserialize)]
struct TrackMetadataResponse {
    pub artist: String,
    pub title: String,
    pub year: Option<u32>,
    pub label: Option<String>,
    pub artwork: Option<String>,
}

impl TrackResponse {
    fn from_domain(track: &TrackId, path: PathBuf, meta: Option<TrackMetadata>) -> Self {
        Self {
            track_id: track.to_string(),
            path,
            metadata: meta.map(|metadata| TrackMetadataResponse {
                artist: metadata.artist.clone(),
                title: metadata.title.clone(),
                year: metadata.year,
                label: metadata.label.clone(),
                artwork: metadata.artwork.clone().map(|a| a.0),
            }),
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
        storage::schema::{FILES, PATH, TRACK_ID},
    };

    use rouille::Request;
    use rusqlite::params;
    use std::{
        fs,
        str::FromStr,
        sync::{Arc, Mutex},
    };
    use tempfile::tempdir;

    pub fn parse_text_response(response: rouille::Response) -> String {
        let mut buf = String::new();
        let mut reader = response.data.into_reader_and_size().0;
        reader.read_to_string(&mut buf).unwrap();
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

    fn create_server_with_tracks<S: AsRef<str>>(
        tracks: impl IntoIterator<Item = (TrackId, S)>,
    ) -> HttpServer {
        let storage = setup_storage().unwrap();

        {
            let mut locked = storage.lock().unwrap();
            locked
                .insert_tracks(
                    tracks
                        .into_iter()
                        .map(|(t, p)| (t, PathBuf::from_str(p.as_ref()).unwrap())),
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
        Ok(Arc::new(Mutex::new(Storage::new(
            crate::config::Database::InMemory,
            Default::default(),
        )?)))
    }

    // --------------------------------------------------
    // ✅ SUCCESS
    // --------------------------------------------------

    #[test]
    fn test_http_get_track_success() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"x")?;

        let server = create_server_with_tracks([(mock_trackid(1), file_path.to_string_lossy())]);

        let request = Request::fake_http(
            "GET",
            format!("/tracks/{}", mock_trackid_str(1)),
            vec![],
            vec![],
        );

        let response = server.handle_request(&request);

        assert_eq!(response.status_code, 200);

        let body: TrackResponse = parse_json_response(response)?;

        assert_eq!(body.track_id, mock_trackid_str(1));
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
        let dir = tempdir()?;
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"x")?;

        let server = create_server_with_tracks([(mock_trackid(1), file_path.to_string_lossy())]);

        let request = Request::fake_http(
            "GET",
            format!("/tracks/{}/stream", mock_trackid_str(1)),
            vec![],
            vec![],
        );

        let response = server.handle_request(&request);

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
        let server = create_server_with_tracks([(mock_trackid(123), "somewhere")]); // storage contains track

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
            body.contains("no youtube link"),
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

    #[test]
    fn test_stream_headers() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"x").unwrap();

        let track_id = mock_trackid(12345);
        let server = create_server_with_tracks([(track_id, &file_path.to_string_lossy())]);

        let request =
            Request::fake_http("GET", format!("/tracks/{track_id}/stream"), vec![], vec![]);
        let response =
            HttpServer::get_track_stream(track_id.to_string(), &request, &server.storage)
                .expect("streaming should succeed");

        // Check that Accept-Ranges header is present
        assert_eq!(
            response
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("Accept-Ranges")),
            true
        );

        // Check status code
        assert!(
            response.status_code == 200 || response.status_code == 206,
            "expected 200 or 206, got {}",
            response.status_code
        );
    }

    #[test]
    fn test_stream_partial_range() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"asdfghjkas").unwrap();

        let track_id = mock_trackid(12345);
        let server = create_server_with_tracks([(track_id, &file_path.to_string_lossy())]);

        // Request a partial range
        let request = Request::fake_http(
            "GET",
            format!("/tracks/{track_id}/stream"),
            vec![("Range".into(), "bytes=2-5".into())],
            vec![],
        );

        let response =
            HttpServer::get_track_stream(track_id.to_string(), &request, &server.storage)
                .expect("partial streaming should succeed");

        assert_eq!(response.status_code, 206);

        let content_range = response
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("Content-Range"))
            .expect("Content-Range header should be present")
            .1
            .to_string();

        assert_eq!(content_range, "bytes 2-5/10");
    }

    #[test]
    fn test_stream_invalid_range_returns_416() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"x").unwrap();

        let track_id = mock_trackid(12345);
        let server = create_server_with_tracks([(track_id, &file_path.to_string_lossy())]);

        // Request a range beyond file size
        let request = Request::fake_http(
            "GET",
            "/tracks/{track_id}/stream",
            vec![("Range".into(), "bytes=20-30".into())],
            vec![],
        );

        let response =
            HttpServer::get_track_stream(track_id.to_string(), &request, &server.storage);

        assert!(matches!(response, Err(ApiError::InvalidRange)));
    }

    #[test]
    fn test_http_get_track_with_metadata() -> anyhow::Result<()> {
        use std::fs;
        use tempfile::tempdir;

        // ---------- Setup temp directory and music file ----------
        let dir = tempdir()?;
        let file_path = dir.path().join("song.mp3");
        fs::write(&file_path, b"x")?;

        let track_id = mock_trackid(42);
        let track_id_str = mock_trackid_str(42);

        // ---------- Setup server with track and metadata ----------
        // `create_server_with_tracks` should accept metadata if we extend it
        let server = create_server_with_tracks([(track_id, file_path.to_string_lossy())]);

        // Insert metadata directly into the test DB
        server.storage.lock().unwrap().db.execute(
            r#"
            INSERT INTO track_metadata (track_id, title, artist, year, label, artwork_url)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            [
                &track_id.to_string(),
                "Test Song",
                "Test Artist",
                "2026",
                "Test Label",
                "cover.jpg",
            ],
        )?;

        // ---------- Make the HTTP request ----------
        let request =
            Request::fake_http("GET", format!("/tracks/{}", track_id_str), vec![], vec![]);
        let response = server.handle_request(&request);

        assert_eq!(response.status_code, 200);

        // ---------- Parse JSON response ----------
        let body: TrackResponse = parse_json_response(response)?;

        // ---------- Assertions ----------
        assert_eq!(body.track_id, track_id_str);
        assert_eq!(body.path, file_path);

        // Metadata assertions
        let metadata = body.metadata.expect("Metadata should be present");
        assert_eq!(metadata.title, "Test Song");
        assert_eq!(metadata.artist, "Test Artist");
        assert_eq!(metadata.year, Some(2026));
        assert_eq!(metadata.label.as_deref(), Some("Test Label"));
        assert_eq!(
            metadata.artwork.as_ref().map(|a| a.as_str()),
            Some("cover.jpg")
        );

        Ok(())
    }
}
