use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Local};
use rusqlite::Connection;

use crate::{
    config::Database,
    storage::{error::StorageError, schema, usb::find_mount_by_label},
};

pub type SecondsSinceUnix = i64;

fn open_in_memory() -> Result<rusqlite::Connection, rusqlite::Error> {
    Connection::open_in_memory()
}

fn open_from_file(path: &Path) -> Result<rusqlite::Connection, rusqlite::Error> {
    Connection::open(path)
}

fn resolve_database_path(db: &Database) -> Result<Option<PathBuf>, anyhow::Error> {
    if db.in_memory {
        return Ok(None);
    }

    if let Some(path) = &db.path {
        return Ok(Some(path.clone()));
    }

    if let (Some(label), Some(rel)) = (&db.usb_label, &db.relative_path) {
        let mount = find_mount_by_label(label)
            .with_context(|| format!("USB with label '{label}' not found"))?;

        return Ok(Some(mount.join(rel)));
    }

    bail!("database config invalid: no path or usb_label provided");
}

pub fn open(config: &Database) -> Result<rusqlite::Connection, StorageError> {
    let path = resolve_database_path(config).map_err(StorageError::Internal)?;
    let db = if let Some(path) = path {
        open_from_file(&path)?
    } else {
        open_in_memory()?
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
        let db = open(&Database {
            in_memory: true,
            path: None,
            usb_label: None,
            relative_path: None,
        })
        .unwrap();

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
