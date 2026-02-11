use rusqlite::Connection;

pub mod tables {
    pub const FILES: &str = "files";
    pub const UPDATES: &str = "updates";
    pub const TRACK_METADATA: &str = "track_metadata";
    pub const TRACKS: &str = "tracks";

    pub const ALL_TABLES: &[&str] = &[TRACKS, FILES, UPDATES, TRACK_METADATA];
}

pub mod columns {
    pub const TRACK_ID: &str = "track_id";
    pub const PATH: &str = "path";
    pub const UPDATED_AT: &str = "updated_at";
    pub const TITLE: &str = "title";
    pub const ARTIST: &str = "artist";
    pub const YEAR: &str = "year";
    pub const LABEL: &str = "label";
    pub const ARTWORK_URL: &str = "artwork_url";
}

pub use columns::*;
pub use tables::*;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS tracks (
    track_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS files (
    track_id TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (track_id, path),
    FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS updates (
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS track_metadata (
    track_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist TEXT NOT NULL,
    year INTEGER,
    label TEXT,
    artwork_url TEXT,
    FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_track_metadata_artist
    ON track_metadata(artist);

CREATE INDEX IF NOT EXISTS idx_track_metadata_year
    ON track_metadata(year);
"#;

pub fn init(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(SCHEMA)
}
