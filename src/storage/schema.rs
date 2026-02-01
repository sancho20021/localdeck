use anyhow::Context;
use rusqlite::Connection;

pub mod tables {
    pub const FILES: &str = "files";
    pub const UPDATES: &str = "updates";

    pub const ALL_TABLES: &[&str] = &[FILES, UPDATES];
}

pub mod columns {
    pub const TRACK_ID: &str = "track_id";
    pub const PATH: &str = "path";
    pub const UPDATED_AT: &str = "updated_at";
}

pub use tables::*;
pub use columns::*;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS files (
    track_id TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (track_id, path)
);

CREATE TABLE IF NOT EXISTS updates (
    updated_at INTEGER NOT NULL
);
"#;

pub fn init(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(SCHEMA)
        .with_context(|| "Failed to apply schema to the database")
}
