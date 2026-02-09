use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use chrono::{DateTime, Local};
use rusqlite::Connection;

use crate::{
    config::Database,
    storage::{error::StorageError, resolve_location, schema},
};

pub type SecondsSinceUnix = i64;

fn open_in_memory() -> Result<rusqlite::Connection, rusqlite::Error> {
    Connection::open_in_memory()
}

fn open_from_file(path: &Path) -> Result<rusqlite::Connection, rusqlite::Error> {
    Connection::open(path)
}

pub fn open(config: &Database) -> Result<rusqlite::Connection, StorageError> {
    let db = match config {
        Database::InMemory => open_in_memory()?,
        Database::OnDisk { location } => {
            let path = resolve_location(location).map_err(StorageError::Internal)?;
            open_from_file(&path)?
        }
    };
    schema::init(&db)?;
    Ok(db)
}

/// converts time to number of seconds since unix_epoch
pub fn system_time_to_i64(time: SystemTime) -> anyhow::Result<SecondsSinceUnix> {
    i64::try_from(
        time.duration_since(UNIX_EPOCH)
            .with_context(|| "failed to get unix timestamp")?
            .as_secs(),
    )
    .with_context(|| "failed to get scan timestamp in seconds")
}

/// converts number of seconds since unix epoch local time to local date time
pub fn i64_seconds_to_local_time(since_unix: i64) -> anyhow::Result<DateTime<Local>> {
    let datetime = DateTime::from_timestamp_secs(since_unix).ok_or(anyhow!(
        "failed to convert {since_unix} s timestamp to datetime"
    ))?;

    Ok(DateTime::from(datetime))
}

#[cfg(test)]
mod tests {
    use crate::{
        config::Database,
        storage::{db::open, schema},
    };

    #[test]
    fn open_in_memory_db_initializes_schema() {
        let db = open(&Database::InMemory).unwrap();

        let mut stmt = db
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap();

        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        for table in schema::tables::ALL_TABLES {
            assert!(tables.contains(&table.to_string()));
        }
    }
}
