use rusqlite::Connection;

pub mod tables {
    pub const FILES: &str = "files";
    pub const SCANS: &str = "scans";
}

pub mod columns {
    pub const TRACK_ID: &str = "track_id";
    pub const PATH: &str = "path";
    pub const LAST_SEEN: &str = "last_seen";
    pub const SCANNED_AT: &str = "scanned_at";
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS files (
    track_id TEXT NOT NULL,
    path TEXT NOT NULL,
    last_seen INTEGER NOT NULL,
    PRIMARY KEY (track_id, path)
);

CREATE TABLE IF NOT EXISTS scans (
    scanned_at INTEGER NOT NULL
);
";

fn init(conn: &Connection) {
    let
}
"#;

pub fn init(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(SCHEMA)
}
