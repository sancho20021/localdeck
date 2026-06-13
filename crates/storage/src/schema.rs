use rusqlite::Connection;

pub mod tables {
    pub const FILES: &str = "files";
    pub const UPDATES: &str = "updates";
    pub const TRACK_METADATA: &str = "track_metadata";
    pub const TRACKS: &str = "tracks";
    pub const CARD_MAPPINGS: &str = "card_mappings";

    pub const ALL_TABLES: &[&str] = &[TRACKS, FILES, UPDATES, TRACK_METADATA, CARD_MAPPINGS];
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
    pub const USB_LABEL: &str = "usb_label";
    pub const FILE_SIZE: &str = "file_size";
    pub const FILE_HASH: &str = "file_hash";
    pub const CARD_ID: &str = "card_id";
}

pub use columns::*;
pub use tables::*;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS tracks (
    track_id INTEGER PRIMARY KEY AUTOINCREMENT
);

-- 2. Card Mappings: Translation layer matching a physical card's printed id
-- to a specific digital track_id. One track can have multiple card aliases.
CREATE TABLE IF NOT EXISTS card_mappings (
    card_id TEXT PRIMARY KEY,
    track_id INTEGER NOT NULL,
    FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS files (
    usb_label TEXT NOT NULL,
    path TEXT NOT NULL,
    track_id INTEGER NOT NULL,
    file_size INTEGER NOT NULL,
    file_hash TEXT NOT NULL,
    PRIMARY KEY (usb_label, path),
    FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS updates (
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS track_metadata (
    track_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    artist TEXT NOT NULL,
    year INTEGER,
    label TEXT,
    artwork_url TEXT,
    FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE
);

-- Fast lookup when checking if a file's hash already exists in the library
CREATE INDEX IF NOT EXISTS idx_files_hash
    ON files(file_hash);

CREATE INDEX IF NOT EXISTS idx_files_track_id ON files(track_id);

CREATE INDEX IF NOT EXISTS idx_track_metadata_artist
    ON track_metadata(artist);
"#;

pub fn init(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(SCHEMA)
}
