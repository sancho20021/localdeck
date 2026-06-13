use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::anyhow;
use chrono::{DateTime, Local};

#[cfg(test)]
use crate::config::LibrarySource;
use crate::{
    CardId,
    config::{Config, Database},
    db::{self, DBConfig, i64_seconds_to_local_time, system_time_to_i64},
    error::StorageError,
    file_hash::FileHash,
    fs::{FileStorage, FileWithMeta, FsSnapshot, is_valid_music_path},
    location::{LOCATION_PATH_SEP, Location, replace_windows_slashes},
    schema::{columns, tables},
    track::{ArtworkRef, Track, TrackId, TrackMetadata},
    usb::ResolveError,
};

use columns::*;
use rusqlite::{ErrorCode, OptionalExtension, Transaction, params};
use tables::*;

pub use crate::fs::HashedFile;

/// Main structure that implements all storage logic
pub struct Storage {
    pub(crate) db: rusqlite::Connection,
    fs: FileStorage,
}

#[derive(Debug)]
pub struct ForgetReport {
    /// files removed
    pub removed_files: usize,
    /// tracks where some of its location removed
    pub affected_tracks: usize,
    /// tracks which no longer exist
    pub removed_tracks: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct CleanDanglingReport {
    /// Number of dangling track ids removed from TRACKS.
    pub removed_tracks: usize,
}

#[derive(Debug, Default)]
pub struct StaleTracks {
    /// Track exists in TRACKS and METADATA but has no files.
    pub metadata_only: Vec<TrackId>,

    /// Track exists in TRACKS but has neither files nor metadata.
    pub dangling: Vec<TrackId>,
}

impl Storage {
    /// when called, opens a data base connection
    /// and applies migrations
    pub fn new(config: Config) -> Result<Self, StorageError> {
        let mut fs = FileStorage::new(config.library_source);
        let db_config = match config.database {
            Database::InMemory => DBConfig::InMemory,
            Database::OnDisk { location } => DBConfig::OnDisk {
                location: fs.loc_resolver.resolve(&location).map_err(|e| {
                    StorageError::Internal(anyhow!("Failed to resolve DB location: {e}"))
                })?,
            },
        };

        let db: rusqlite::Connection = db::open(db_config)?;
        Ok(Self { db, fs })
    }

    #[cfg(test)]
    fn from_existing_conn(db: rusqlite::Connection, lib_config: LibrarySource) -> Self {
        Self {
            db,
            fs: FileStorage::new(lib_config),
        }
    }

    /// Retrieves all tracks present in database
    fn get_tracks(&mut self) -> Result<Vec<TrackId>, StorageError> {
        // TODO: test
        let tx = self.db.transaction()?;
        let track_ids = {
            let mut stmt = tx.prepare(&format!("SELECT {TRACK_ID} FROM {TRACKS}"))?;

            stmt.query_map([], |row| {
                let id: i64 = row.get(0)?;
                Ok(id)
            })?
            .collect::<Result<Vec<TrackId>, _>>()?
        };
        tx.commit()?;
        Ok(track_ids)
    }

    /// Opens transaction, must not be used in a loop for performance
    fn get_track_files(&mut self, track: TrackId) -> Result<Vec<HashedFile>, StorageError> {
        let mut tx = self.db.transaction()?;
        let res = Self::_get_track_files(&mut tx, track)?;
        tx.commit()?;
        Ok(res)
    }

    /// Retrieves all files from database that correspond to the given track
    fn _get_track_files(
        tx: &mut Transaction,
        track: TrackId,
    ) -> Result<Vec<HashedFile>, StorageError> {
        // TODO: write test
        let files = {
            // Query the files table directly filtering by the integer track_id
            let mut stmt = tx.prepare(&format!(
                "SELECT {USB_LABEL}, {PATH}, {FILE_SIZE}, {FILE_HASH}
             FROM {FILES}
             WHERE {TRACK_ID} = ?"
            ))?;

            stmt.query_map([track], |row| {
                let usb_label: String = row.get(0)?;
                let path: String = row.get(1)?;
                let file_size: i64 = row.get(2)?;
                let hash: String = row.get(3)?;

                Ok((LocationRow { usb_label, path }, file_size, hash))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        let files = files
            .into_iter()
            .map(|(lr, file_size, hash)| {
                Ok(HashedFile {
                    hash: FileHash::from_hex(hash).map_err(|e| {
                        StorageError::Internal(anyhow!("Database contains invalid file hash {e}"))
                    })?,
                    file: FileWithMeta {
                        loc: lr.into(),
                        file_size,
                    },
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;

        Ok(files)
    }

    pub fn scan_metadata(&mut self) -> Result<Vec<Track>, StorageError> {
        let tx = self.db.transaction()?; // rusqlite::Error propagates here

        let mut stmt = tx.prepare(
            &format!("SELECT {TRACK_ID}, {TITLE}, {ARTIST}, {YEAR}, {LABEL}, {ARTWORK_URL} FROM {TRACK_METADATA}"),
        )?;

        // query_map returns Result<Rows<Result<Track, StorageError>>, rusqlite::Error>
        let rows = stmt.query_map([], |row| {
            let track_id: i64 = row.get(0)?;

            Ok(Ok(Track {
                id: track_id,
                metadata: TrackMetadata {
                    title: row.get(1)?,
                    artist: row.get(2)?,
                    year: row.get(3)?,
                    label: row.get(4)?,
                    artwork: row.get::<_, Option<String>>(5)?.map(ArtworkRef),
                },
            }))
        })?;
        // flatten results: first unwrap DB errors, then propagate custom errors
        let metadata_list: Vec<Track> = rows
            .collect::<Result<Vec<Result<Track, StorageError>>, rusqlite::Error>>()? // unwrap DB errors
            .into_iter()
            .collect::<Result<Vec<Track>, StorageError>>()?; // propagate TrackId errors
        drop(stmt);

        tx.commit()?;

        Ok(metadata_list)
    }

    fn insert_update_time(tx: &Transaction) -> Result<(), StorageError> {
        let time_secs = system_time_to_i64(SystemTime::now()).map_err(StorageError::Internal)?;
        // ---------- Record update timestamp ----------
        tx.execute(
            &format!("INSERT INTO {UPDATES} ({UPDATED_AT}) VALUES (?1)"),
            params![time_secs],
        )?;
        Ok(())
    }

    /// Reads the latest updated timestamp from the database.
    pub fn updated_at(&mut self) -> Result<DateTime<Local>, StorageError> {
        // We use COALESCE to gracefully fall back to 0 if the table is empty
        let sql = format!("SELECT COALESCE(MAX({UPDATED_AT}), 0) FROM {UPDATES}");
        let latest_time: i64 = self.db.query_one(&sql, [], |row| row.get(0))?;
        i64_seconds_to_local_time(latest_time).map_err(|e| StorageError::Internal(e))
    }

    /// Helper to look up an existing track ID by file hash, or provision a new track row if missing.
    fn get_or_create_track_id(
        tx: &Transaction,
        hash: &FileHash,
    ) -> Result<TrackId, rusqlite::Error> {
        let hash = hash.to_string();
        // Query to find existing track by file hash
        let query = format!("SELECT {TRACK_ID} FROM {FILES} WHERE {FILE_HASH} = ?1 LIMIT 1");
        let mut find_track_stmt = tx.prepare_cached(&query)?;

        let existing_track_id: Option<TrackId> = find_track_stmt
            .query_row(params![hash], |row| row.get(0))
            .optional()?;

        if let Some(id) = existing_track_id {
            Ok(id)
        } else {
            // Insert a new default row into tracks to auto-increment a new ID
            let insert_query = format!("INSERT INTO {TRACKS} DEFAULT VALUES");
            let mut insert_track_stmt = tx.prepare_cached(&insert_query)?;
            insert_track_stmt.execute([])?;

            Ok(tx.last_insert_rowid())
        }
    }

    /// Inserts a single file entry bound to a specific TrackId.
    /// Returns `Ok(true)` if inserted, or `Ok(false)` if ignored due to a location conflict.
    fn insert_file(
        tx: &rusqlite::Transaction,
        track_id: TrackId,
        hashed_file: &HashedFile,
    ) -> Result<bool, StorageError> {
        let insert_file_query = format!(
            "INSERT OR IGNORE INTO {FILES} ({USB_LABEL}, {PATH}, {TRACK_ID}, {FILE_SIZE}, {FILE_HASH}) \
             VALUES (?1, ?2, ?3, ?4, ?5)"
        );
        let mut stmt = tx.prepare_cached(&insert_file_query)?;

        let loc_row = LocationRow::from_location(hashed_file.file.loc.clone())?;
        let rows_changed = stmt.execute(rusqlite::params![
            loc_row.usb_label,
            loc_row.path,
            track_id,
            hashed_file.file.file_size,
            hashed_file.hash.to_string()
        ])?;

        Ok(rows_changed > 0)
    }

    /// Inserts track files, grouping by hash. Reuses track IDs on hash matches.
    ///
    /// Ignores location conflicts. Returns only newly inserted items.
    fn insert_files(
        &mut self,
        files: impl IntoIterator<Item = HashedFile>,
    ) -> Result<HashMap<TrackId, HashSet<HashedFile>>, StorageError> {
        let mut grouped_by_hash: HashMap<FileHash, Vec<HashedFile>> = HashMap::new();
        for hashed_file in files {
            grouped_by_hash
                .entry(hashed_file.hash.clone())
                .or_default()
                .push(hashed_file);
        }

        let tx = self.db.transaction()?;
        let mut inserted_tracks: HashMap<TrackId, HashSet<HashedFile>> = HashMap::new();

        for (hash, hashed_files) in grouped_by_hash {
            // Find existing track or generate a brand new one for this content hash
            let track_id = Self::get_or_create_track_id(&tx, &hash)?;

            for hashed_file in hashed_files {
                // Call the granular single insert helper
                if Self::insert_file(&tx, track_id, &hashed_file)? {
                    inserted_tracks
                        .entry(track_id)
                        .or_default()
                        .insert(hashed_file);
                }
            }
        }

        if !inserted_tracks.is_empty() {
            Self::insert_update_time(&tx)?;
        }

        tx.commit()?;
        Ok(inserted_tracks)
    }

    /// Recursively scans all music files in the library source. Retrieves their paths and metadata
    fn scan_fs(self_fs: &mut FileStorage) -> Result<FsSnapshot, StorageError> {
        println!("Scanning music on file system...");
        let fs = self_fs.scan()?;
        Ok(fs)
    }

    /// checks for new music files not present in database
    pub fn check_new(&mut self) -> Result<HashSet<FileWithMeta>, StorageError> {
        let mut fs = HashSet::new();
        let mut tx = self.db.transaction()?;
        for file in Self::scan_fs(&mut self.fs)? {
            if Self::_find_track_by_file(&mut tx, &file)?.is_none() {
                fs.insert(file);
            }
        }
        tx.commit()?;
        Ok(fs)
    }

    /// Returns tracks that have no associated files.
    ///
    /// Splits results into:
    /// - `metadata_only`: tracks that still have metadata
    /// - `dangling`: tracks that have neither files nor metadata
    pub fn check_stale(&mut self) -> Result<StaleTracks, StorageError> {
        let tx = self.db.transaction()?;

        let stale_rows = {
            let mut stmt = tx.prepare(&format!(
                "
            SELECT
                t.{TRACK_ID},
                m.{TRACK_ID} IS NOT NULL as has_metadata
            FROM {TRACKS} t
            LEFT JOIN {FILES} f
                ON t.{TRACK_ID} = f.{TRACK_ID}
            LEFT JOIN {TRACK_METADATA} m
                ON t.{TRACK_ID} = m.{TRACK_ID}
            WHERE f.{TRACK_ID} IS NULL
            "
            ))?;

            stmt.query_map([], |row| {
                let track_id: i64 = row.get(0)?;
                let has_metadata: bool = row.get(1)?;

                Ok((track_id, has_metadata))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        tx.commit()?;

        let mut result = StaleTracks::default();

        for (track_id, has_metadata) in stale_rows {
            if has_metadata {
                result.metadata_only.push(track_id);
            } else {
                result.dangling.push(track_id);
            }
        }

        Ok(result)
    }

    /// Scans for untracked files, hashes them, and commits them to the database.
    pub fn update_db_with_new_files(
        &mut self,
    ) -> Result<HashMap<TrackId, HashSet<HashedFile>>, StorageError> {
        let new_files = self.check_new()?;
        if !new_files.is_empty() {
            println!("Hashing {} new files", new_files.len());
        }
        let with_hash = new_files.into_iter().map(|f| {
            let path = self.fs.loc_resolver.resolve(&f.loc);
            let path = match path {
                Ok(path) => path,
                Err(e) => return Err(StorageError::Internal(anyhow!("Failed to resolve a file location. Possibly a drive got removed during the operation: {e}"))),
            };
            let hash = FileHash::from_file(&path)?;
            Ok(HashedFile::new(hash, f))
        }).collect::<Result<Vec<_>, _>>()?;
        self.insert_files(with_hash.clone())
    }

    /// checks for tracks without available files.
    pub fn check_missing(
        &mut self,
    ) -> Result<HashMap<TrackId, HashSet<FileWithMeta>>, StorageError> {
        let fs = self.fs.scan()?;

        let mut track_db_locs: HashMap<TrackId, HashSet<FileWithMeta>> = Default::default();

        let tracks = self.get_tracks()?;

        let mut tx = self.db.transaction()?;
        for track in tracks {
            let track_files = Self::_get_track_files(&mut tx, track)?;
            for db_file in track_files {
                if !fs.contains(&db_file.file) {
                    track_db_locs
                        .entry(track)
                        .or_insert(Default::default())
                        .insert(db_file.file);
                }
            }
        }
        tx.commit()?;
        Ok(track_db_locs)
    }

    /// Merges a slave track into a master track.
    /// All files and card mappings belonging to the slave are moved to the master.
    /// The slave track and its metadata are completely deleted.
    ///
    /// # Errors
    /// Returns `StorageError::SlaveTrackHasMetadata` if the slave track has metadata
    /// AND `ignore_slave_meta` is set to `false`.
    pub fn merge_tracks(
        &mut self,
        master_id: TrackId,
        slave_id: TrackId,
        ignore_slave_meta: bool,
    ) -> Result<(), StorageError> {
        if master_id == slave_id {
            return Ok(());
        }

        let tx = self.db.transaction()?;

        // 1. Protection Check: Check if the slave track has metadata
        let slave_has_meta_query =
            format!("SELECT 1 FROM {TRACK_METADATA} WHERE {TRACK_ID} = ?1 LIMIT 1");
        let has_meta: bool = tx
            .prepare_cached(&slave_has_meta_query)?
            .query_row(rusqlite::params![slave_id], |_| Ok(true))
            .optional()?
            .unwrap_or(false);

        if has_meta && !ignore_slave_meta {
            return Err(StorageError::SlaveTrackHasMetadata(slave_id));
        }

        // 2. Point all files belonging to the slave track to the master track
        let update_files_query =
            format!("UPDATE {FILES} SET {TRACK_ID} = ?1 WHERE {TRACK_ID} = ?2");
        tx.prepare_cached(&update_files_query)?
            .execute(rusqlite::params![master_id, slave_id])?;

        // 3. Point all card mappings belonging to the slave track to the master track
        let update_cards_query =
            format!("UPDATE {CARD_MAPPINGS} SET {TRACK_ID} = ?1 WHERE {TRACK_ID} = ?2");
        tx.prepare_cached(&update_cards_query)?
            .execute(rusqlite::params![master_id, slave_id])?;

        // 4. Delete the slave track from the tracks ledger.
        // Due to FOREIGN KEY (... ) ON DELETE CASCADE, this automatically deletes
        // the slave track's metadata entry from the track_metadata table.
        let delete_track_query = format!("DELETE FROM {TRACKS} WHERE {TRACK_ID} = ?1");
        tx.prepare_cached(&delete_track_query)?
            .execute(rusqlite::params![slave_id])?;

        // 5. Update ledger tracking time since the library structures changed
        Self::insert_update_time(&tx)?;

        tx.commit()?;
        Ok(())
    }

    /// Links a physical file path to an existing master track.
    /// This is useful for adding high-quality, fixed, or alternative versions.
    pub fn add_file_to_track(
        &mut self,
        master_id: TrackId,
        physical_path: &Path,
    ) -> Result<(), StorageError> {
        // 1. Invert the physical path back to a structured library Location
        let location = self.fs.reverse_resolve(physical_path)?;
        // 2. Compute the file properties needed for insertion
        let file_size = std::fs::metadata(physical_path)?.len() as i64;
        let hash = FileHash::from_file(physical_path)?;

        let hashed_file = HashedFile::new(
            hash,
            FileWithMeta {
                loc: location,
                file_size,
            },
        );
        let mut tx = self.db.transaction()?;
        // Make sure master track exists
        let _ = Self::_resolve_track(&mut tx, master_id.to_string())?;
        let inserted = Self::insert_file(&tx, master_id, &hashed_file)?;
        if inserted {
            Self::insert_update_time(&tx)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_track_metadata(
        &mut self,
        track_id: TrackId,
    ) -> Result<Option<TrackMetadata>, StorageError> {
        // ---------- Load metadata ----------
        let mut stmt = self.db.prepare(&format!(
            "SELECT {TITLE}, {ARTIST}, {YEAR}, {LABEL}, {ARTWORK_URL}
            FROM {TRACK_METADATA}
            WHERE {TRACK_ID} = ?1"
        ))?;

        let mut rows = stmt.query(params![&track_id.to_string()])?;
        let row = if let Some(row) = rows.next()? {
            row
        } else {
            return Ok(None);
        };

        Ok(Some(TrackMetadata {
            title: row.get(0)?,
            artist: row.get(1)?,
            year: row.get(2)?,
            label: row.get(3)?,
            artwork: row.get::<_, Option<String>>(4)?.map(ArtworkRef),
        }))
    }

    /// Looks up a track with given file location
    fn _find_track_by_file(
        tx: &mut Transaction,
        file: &FileWithMeta,
    ) -> Result<Option<(TrackId, HashedFile)>, StorageError> {
        let loc_row = LocationRow::from_location(file.loc.clone())?;

        let result = {
            let mut stmt = tx.prepare(&format!(
                "SELECT {TRACK_ID}, {FILE_HASH}
             FROM {FILES}
             WHERE {USB_LABEL} = ?1 AND {PATH} = ?2
             LIMIT 1"
            ))?;

            // query_row returns Optional values cleanly if we catch Optional results or query gracefully
            let mut rows = stmt.query([&loc_row.usb_label, &loc_row.path])?;

            if let Some(row) = rows.next()? {
                let track_id_raw: i64 = row.get(0)?;
                let hash_str: String = row.get(1)?;

                Some((track_id_raw, hash_str))
            } else {
                None
            }
        };

        // Map the database string hash and integer ID into the strongly-typed structures
        match result {
            Some((track_id, hash_str)) => {
                let hash = FileHash::from_hex(&hash_str).map_err(|e| {
                    StorageError::Internal(anyhow!("Database contains invalid file hash {e}"))
                })?;

                let hashed_file = HashedFile {
                    hash,
                    file: file.clone(),
                };

                Ok(Some((track_id, hashed_file)))
            }
            None => Ok(None),
        }
    }

    /// retrieves file of the track, checking that it is a valid music file in the file system
    ///
    /// If multiple paths point to the same track, chooses one of them.
    pub fn find_track_file(
        &mut self,
        track_id: TrackId,
    ) -> Result<(TrackId, PathBuf, Location), StorageError> {
        let paths = (|| {
            let mut stmt = self.db.prepare(&format!(
                "SELECT {USB_LABEL}, {PATH} FROM files WHERE {TRACK_ID} = ?1"
            ))?;

            Ok(stmt
                .query_map(params![track_id.to_string()], |row| {
                    let usb_label = row.get::<_, String>(0)?;
                    let path = row.get::<_, String>(1)?;
                    Ok(LocationRow { usb_label, path }.into())
                })?
                .collect::<Result<Vec<_>, _>>()?)
        })()
        .map_err(StorageError::Database)?;

        if paths.is_empty() {
            return Err(StorageError::TrackNotFound(track_id.to_string()));
        }

        let mut unmounted_locations = vec![];

        for loc in paths {
            let path = self.fs.loc_resolver.resolve(&loc);
            match path {
                Ok(p) => {
                    if is_valid_music_path(&p) {
                        return Ok((track_id, p, loc));
                    }
                }
                Err(e) => match e {
                    ResolveError::UsbNotFound { label } => unmounted_locations.push(label),
                    ResolveError::SystemQueryFail(..) => {
                        return Err(StorageError::Internal(anyhow!(
                            "Error while resolving location {loc}: {e}"
                        )));
                    }
                    ResolveError::WindowsError(..) => {
                        return Err(StorageError::Internal(anyhow!(
                            "Error while resolving location {loc}: {e}"
                        )));
                    }
                },
            }
        }
        Err(StorageError::InvalidTrackFile {
            track: track_id,
            extra: if !unmounted_locations.is_empty() {
                format!("following drive labels are unmounted: {unmounted_locations:?}")
            } else {
                "".to_string()
            },
        })
    }

    fn _resolve_track(tx: &mut Transaction, card_id: CardId) -> Result<TrackId, StorageError> {
        let card_str = card_id.to_string();
        // Parse into a valid integer ID if possible, otherwise default to an invalid ID like -1
        let parsed_id = card_str.parse::<i64>().unwrap_or(-1);

        // LEFT JOIN ensures tracks without card mappings are still accessible via their raw ID
        let query = format!(
            "SELECT t.{TRACK_ID}
             FROM {TRACKS} t
             LEFT JOIN {CARD_MAPPINGS} cm ON t.{TRACK_ID} = cm.{TRACK_ID}
             WHERE cm.{CARD_ID} = ?1 OR t.{TRACK_ID} = ?2
             LIMIT 1"
        );

        let mut stmt = tx.prepare_cached(&query)?;
        let track_id: Option<TrackId> = stmt
            .query_row(rusqlite::params![&card_str, parsed_id], |row| row.get(0))
            .optional()?;

        drop(stmt);

        match track_id {
            Some(id) => Ok(id),
            None => Err(StorageError::TrackNotFound(card_id)),
        }
    }

    /// Finds track id based on card_id alias
    ///
    /// If given id is a valid track id, tries it as it is as well
    pub fn resolve_track(&mut self, card_id: CardId) -> Result<TrackId, StorageError> {
        let mut tx = self.db.transaction()?;
        let res = Self::_resolve_track(&mut tx, card_id)?;
        tx.commit()?;
        Ok(res)
    }

    pub fn find_track_file_with_meta(
        &mut self,
        track: TrackId,
    ) -> Result<(PathBuf, Location, Option<TrackMetadata>), StorageError> {
        let (_, path, loc) = self.find_track_file(track)?;
        let meta = self.get_track_metadata(track)?;
        Ok((path, loc, meta))
    }

    /// searches for a file where path, track_id, hash, card_id, artist or title matches the query
    ///
    /// conditionally selects only tracks without meta data
    pub fn find_files(
        &mut self,
        query: &str,
        no_meta: bool,
    ) -> Result<HashMap<TrackId, HashSet<Location>>, StorageError> {
        let tx = self.db.transaction()?;

        let cleaned_query = query.trim().to_lowercase();
        let like_query = format!("%{}%", cleaned_query);

        // 1. Build base query with all required table joins using constants
        let mut sql = format!(
            "SELECT DISTINCT f.{TRACK_ID}, f.{USB_LABEL}, f.{PATH}
             FROM {FILES} f
             LEFT JOIN {TRACK_METADATA} tm ON f.{TRACK_ID} = tm.{TRACK_ID}
             LEFT JOIN {CARD_MAPPINGS} cm ON f.{TRACK_ID} = cm.{TRACK_ID}
             WHERE 1=1"
        );

        // 2. Append conditional filters
        if !cleaned_query.is_empty() {
            sql.push_str(&format!(
                " AND (
                    LOWER(f.{PATH}) LIKE ?1 OR
                    LOWER(f.{TRACK_ID}) LIKE ?1 OR
                    LOWER(f.{FILE_HASH}) LIKE ?1 OR
                    LOWER(cm.{CARD_ID}) LIKE ?1 OR
                    LOWER(tm.{ARTIST}) LIKE ?1 OR
                    LOWER(tm.{TITLE}) LIKE ?1
                )"
            ));
        }

        if no_meta {
            sql.push_str(&format!(" AND tm.{TRACK_ID} IS NULL"));
        }

        // 3. Prepare statement and run execution cleanly via a single branch
        let mut stmt = tx.prepare(&sql)?;

        let params = if !cleaned_query.is_empty() {
            rusqlite::params![like_query]
        } else {
            rusqlite::params![]
        };

        let rows = stmt
            .query_map(params, |row| {
                let track_id: i64 = row.get(0)?;
                let usb_label: String = row.get(1)?;
                let path: String = row.get(2)?;

                let loc: Location = LocationRow { usb_label, path }.into();
                Ok((track_id, loc))
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;

        drop(stmt);
        tx.commit()?;

        // 4. Construct response hash map grouping locations by track ID
        let mut map: HashMap<TrackId, HashSet<Location>> = HashMap::new();
        for (track_id, loc) in rows {
            map.entry(track_id).or_default().insert(loc);
        }

        Ok(map)
    }

    /// Removes dangling track entries from the database.
    ///
    /// A dangling track is a track id that:
    /// - exists in `{TRACKS}`
    /// - has no rows in `{FILES}`
    /// - has no rows in `{TRACK_METADATA}`
    pub fn clean_dangling(&mut self) -> Result<CleanDanglingReport, StorageError> {
        let tx = self.db.transaction()?;

        // --------------------------------------------------
        // Collect dangling track ids
        // --------------------------------------------------

        let dangling_track_ids = {
            let mut stmt = tx.prepare(&format!(
                "
            SELECT t.{TRACK_ID}
            FROM {TRACKS} t
            LEFT JOIN {FILES} f
                ON t.{TRACK_ID} = f.{TRACK_ID}
            LEFT JOIN {TRACK_METADATA} m
                ON t.{TRACK_ID} = m.{TRACK_ID}
            WHERE f.{TRACK_ID} IS NULL
              AND m.{TRACK_ID} IS NULL
            "
            ))?;

            stmt.query_map([], |row| row.get::<_, TrackId>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        // --------------------------------------------------
        // Delete dangling tracks
        // --------------------------------------------------

        let mut removed_tracks = 0;

        for track_id in &dangling_track_ids {
            removed_tracks += tx.execute(
                &format!(
                    "
                DELETE FROM {TRACKS}
                WHERE {TRACK_ID} = ?1
                "
                ),
                params![track_id],
            )?;
        }

        // --------------------------------------------------
        // Record update timestamp
        // --------------------------------------------------

        if removed_tracks > 0 {
            Self::insert_update_time(&tx)?;
        }

        tx.commit()?;

        Ok(CleanDanglingReport { removed_tracks })
    }

    /// removes all files inside specified directory from the database
    /// useful when some files got moved or deleted
    pub fn forget_path(&mut self, path: &Path) -> Result<ForgetReport, StorageError> {
        let tx = self.db.transaction()?;

        let path_prefix = replace_windows_slashes(path);

        let dir_prefix = if path_prefix.ends_with(LOCATION_PATH_SEP) {
            path_prefix.clone()
        } else {
            format!("{}{}%", path_prefix, LOCATION_PATH_SEP)
        };
        // --------------------------------------------------
        // Collect affected track ids BEFORE deletion
        // --------------------------------------------------

        let mut stmt = tx.prepare(&format!(
            "SELECT DISTINCT {TRACK_ID} FROM {FILES}
         WHERE {PATH} = ?1 OR {PATH} LIKE ?2"
        ))?;

        let affected_track_ids = stmt
            .query_map(params![path_prefix, dir_prefix], |row| {
                row.get::<_, TrackId>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;

        drop(stmt);

        let affected_tracks = affected_track_ids.len();

        // --------------------------------------------------
        // Delete entries
        // --------------------------------------------------

        let removed_files = tx.execute(
            &format!(
                "DELETE FROM {FILES}
             WHERE {PATH} = ?1 OR {PATH} LIKE ?2"
            ),
            params![path_prefix, dir_prefix],
        )?;

        // --------------------------------------------------
        // Count removed tracks (tracks with zero files left)
        // --------------------------------------------------

        let mut removed_tracks = 0;

        for track_id in &affected_track_ids {
            let remaining: isize = tx.query_row(
                &format!(
                    "SELECT COUNT(*) FROM {FILES}
                 WHERE {TRACK_ID} = ?1"
                ),
                params![track_id],
                |row| row.get(0),
            )?;

            if remaining == 0 {
                removed_tracks += 1;
            }
        }

        // --------------------------------------------------
        // Record update timestamp
        // --------------------------------------------------
        Self::insert_update_time(&tx)?;

        tx.commit()?;

        Ok(ForgetReport {
            removed_tracks,
            affected_tracks,
            removed_files,
        })
    }

    pub fn update_track_metadata(
        &mut self,
        track_id: TrackId,
        new_meta: MetadataUpdate,
        allow_overwrite: bool,
    ) -> Result<(), StorageError> {
        let tx = self.db.transaction()?;

        // ---------- load current metadata ----------
        let current_meta: Option<TrackMetadata> = (|| {
            let mut stmt = tx.prepare(&format!(
                "SELECT {TITLE}, {ARTIST}, {YEAR}, {LABEL}, {ARTWORK_URL}
             FROM {TRACK_METADATA}
             WHERE {TRACK_ID} = ?1"
            ))?;

            let mut rows = stmt.query(params![track_id.to_string()])?;

            if let Some(row) = rows.next()? {
                Ok::<_, rusqlite::Error>(Some(TrackMetadata {
                    title: row.get(0)?,
                    artist: row.get(1)?,
                    year: row.get(2)?,
                    label: row.get(3)?,
                    artwork: row.get::<_, Option<String>>(4)?.map(ArtworkRef),
                }))
            } else {
                Ok(None)
            }
        })()?;

        let merged = Self::update_meta(track_id, current_meta, new_meta, allow_overwrite)?;

        // ---------- upsert ----------
        let _ = tx
            .execute(
                &format!(
                    "INSERT INTO {TRACK_METADATA}
            ({TRACK_ID}, {TITLE}, {ARTIST}, {YEAR}, {LABEL}, {ARTWORK_URL})
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT({TRACK_ID}) DO UPDATE SET
                {TITLE} = excluded.{TITLE},
                {ARTIST} = excluded.{ARTIST},
                {YEAR} = excluded.{YEAR},
                {LABEL} = excluded.{LABEL},
                {ARTWORK_URL} = excluded.{ARTWORK_URL}
            "
                ),
                params![
                    track_id.to_string(),
                    merged.title,
                    merged.artist,
                    merged.year,
                    merged.label,
                    merged.artwork.map(|a| a.0),
                ],
            )
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(error, _)
                    if error.code == ErrorCode::ConstraintViolation =>
                {
                    StorageError::TrackNotFound(track_id.to_string())
                }
                e => StorageError::Database(e),
            })?;
        Self::insert_update_time(&tx)?;

        tx.commit()?;

        Ok(())
    }

    fn update_meta(
        track: TrackId,
        old: Option<TrackMetadata>,
        new: MetadataUpdate,
        allow_overwrite: bool,
    ) -> Result<TrackMetadata, StorageError> {
        // ---------- Step 3: conflict detection ----------
        if let Some(existing) = &old {
            if !allow_overwrite {
                let conflict = new.title.is_some()
                    || new.artist.is_some()
                    || (existing.year.is_some() && new.year.is_some())
                    || (existing.label.is_some() && new.label.is_some())
                    || (existing.artwork.is_some() && new.artwork.is_some());

                if conflict {
                    return Err(StorageError::MetadataOverwriteDenied(track));
                }
            }
        }

        fn prioritize<T>(high: Option<T>, low: Option<T>) -> Option<T> {
            high.or(low)
        }

        let mut merged_meta = if let Some(old) = old {
            old
        } else {
            TrackMetadata {
                title: new
                    .title
                    .clone()
                    .ok_or(StorageError::RequiredMetaMissing(track))?,
                artist: new
                    .artist
                    .clone()
                    .ok_or(StorageError::RequiredMetaMissing(track))?,
                year: None,
                label: None,
                artwork: None,
            }
        };

        if allow_overwrite {
            merged_meta.title = new.title.unwrap_or(merged_meta.title);
            merged_meta.artist = new.artist.unwrap_or(merged_meta.artist);
            merged_meta.year = prioritize(new.year, merged_meta.year);
            merged_meta.label = prioritize(new.label, merged_meta.label);
            merged_meta.artwork = prioritize(new.artwork, merged_meta.artwork);
        } else {
            merged_meta.year = prioritize(merged_meta.year, new.year);
            merged_meta.label = prioritize(merged_meta.label, new.label);
            merged_meta.artwork = prioritize(merged_meta.artwork, new.artwork);
        }
        Ok(merged_meta)
    }
}

/// DB format of storing file location
#[derive(Debug)]
struct LocationRow {
    /// present if file is stored on usb, empty otherwise
    usb_label: String,
    /// relative path if stored on usb, absolute otherwise
    path: String,
}

impl LocationRow {
    pub fn is_usb(&self) -> bool {
        !self.usb_label.is_empty()
    }
}

impl LocationRow {
    pub fn from_location(value: Location) -> Result<LocationRow, StorageError> {
        Ok(match value {
            Location::File { path } => LocationRow {
                usb_label: String::new(),
                path: replace_windows_slashes(&path),
            },
            Location::Usb { label, path } => {
                if label.is_empty() {
                    return Err(StorageError::Internal(anyhow!(
                        "location usb label can't be empty ({path:?})"
                    )));
                } else {
                    LocationRow {
                        usb_label: label,
                        path: replace_windows_slashes(&path),
                    }
                }
            }
        })
    }
}

impl Into<Location> for LocationRow {
    fn into(self) -> Location {
        let is_usb = self.is_usb();
        let path = PathBuf::from(self.path);
        if is_usb {
            Location::Usb {
                label: self.usb_label,
                path,
            }
        } else {
            Location::File { path }
        }
    }
}

#[derive(Debug)]
pub struct MetadataUpdate {
    pub artist: Option<String>,
    pub title: Option<String>,
    pub year: Option<u32>,
    pub label: Option<String>,
    pub artwork: Option<ArtworkRef>,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs::{self},
        path::{Path, PathBuf},
    };

    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use crate::{
        config::LibrarySource,
        error::StorageError,
        file_hash::FileHash,
        fs::{FileWithMeta, HashedFile},
        location::Location,
        operations::{MetadataUpdate, Storage, replace_windows_slashes},
        schema::{self, *},
        track::TrackId,
        usb::LocationResolver,
    };

    fn file_size(path: &Path) -> i64 {
        let meta = std::fs::metadata(path).unwrap();
        let size = meta.len() as i64;
        size
    }

    fn mock_hash(x: i32) -> FileHash {
        let bytes = x.to_be_bytes();
        FileHash::from_bytes(&bytes)
    }

    fn mock_hash_str(x: i32) -> String {
        mock_hash(x).to_hex()
    }

    fn setup_storage(tmp_dir: &Path) -> anyhow::Result<Storage> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        Ok(Storage::from_existing_conn(
            conn,
            LibrarySource {
                roots: vec![Location::File {
                    path: tmp_dir.to_path_buf(),
                }],
                follow_symlinks: false,
                ignored_dirs: vec![],
            },
        ))
    }

    fn setup_clean_storage() -> anyhow::Result<Storage> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        Ok(Storage::from_existing_conn(
            conn,
            LibrarySource {
                roots: vec![],
                follow_symlinks: false,
                ignored_dirs: vec![],
            },
        ))
    }

    /// Helper to seed tracks in tests, returning the generated IDs in order
    fn insert_tracks(conn: &mut Connection, count: usize) -> Vec<TrackId> {
        let tx = conn.transaction().unwrap();
        let mut generated_ids = Vec::with_capacity(count);

        {
            let mut stmt = tx
                .prepare(&format!("INSERT INTO {TRACKS} ({TRACK_ID}) VALUES (NULL)"))
                .unwrap();

            for _ in 0..count {
                stmt.execute([]).unwrap();

                // Snatch the ID SQLite just minted
                let id = tx.last_insert_rowid();
                generated_ids.push(id);
            }
        }

        tx.commit().unwrap();
        generated_ids
    }

    fn insert_fake_files<S: AsRef<str>>(
        conn: &Connection,
        tracks: impl IntoIterator<Item = (TrackId, S, i64)>,
        usb_label: Option<String>,
    ) {
        for (track, path, fs) in tracks {
            insert_file(&conn, track, path.as_ref(), &usb_label, fs);
        }
    }

    fn insert_real_files<S: AsRef<str>>(
        conn: &Connection,
        tracks: impl IntoIterator<Item = (TrackId, S)>,
        usb_label: Option<String>,
    ) {
        for (track, path) in tracks {
            let p: &str = path.as_ref();
            let fs = file_size(p.as_ref());
            insert_file(&conn, track, path.as_ref(), &usb_label, fs);
        }
    }

    #[test]
    fn test_resolve_track_success() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        // Provision an internal track ID to link against
        let tracks = insert_tracks(&mut conn, 1);
        let expected_track_id = tracks[0];
        let card_id = "RFID_SUCCESS_123";

        // Manually seed the card mapping row
        conn.execute(
            &format!("INSERT INTO {CARD_MAPPINGS} ({CARD_ID}, {TRACK_ID}) VALUES (?1, ?2)"),
            rusqlite::params![card_id, expected_track_id],
        )?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // Act
        let resolved_id = storage.resolve_track(card_id.into())?;
        let resolved_id2 = storage.resolve_track(expected_track_id.to_string())?;

        // Assert
        assert_eq!(resolved_id, expected_track_id);
        assert_eq!(resolved_id2, expected_track_id);

        Ok(())
    }

    #[test]
    fn test_resolve_trackid_itself() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        // Provision an internal track ID to link against
        let tracks = insert_tracks(&mut conn, 1);
        let expected_track_id = tracks[0];

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // Act
        let resolved_id = storage.resolve_track(expected_track_id.to_string())?;

        // Assert
        assert_eq!(resolved_id, expected_track_id);

        Ok(())
    }

    #[test]
    fn test_resolve_track_not_found() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());
        let missing_card_id = "RFID_MISSING_999";

        // Act
        let result = storage.resolve_track(missing_card_id.into());

        // Assert
        assert!(result.is_err(), "Expected an error for an unmapped card ID");

        match result {
            Err(StorageError::TrackNotFound(returned_card_id)) => {
                assert_eq!(returned_card_id.to_string(), missing_card_id);
            }
            _ => panic!("Expected StorageError::TrackNotFound variant"),
        }

        Ok(())
    }

    #[test]
    fn test_merge_tracks() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        // Provision 2 tracks: 0 will be Master, 1 will be Slave
        let tracks = insert_tracks(&mut conn, 2);
        let master = tracks[0];
        let slave = tracks[1];

        // Seed Files
        insert_fake_files(
            &mut conn,
            vec![
                (master, "old_low_quality.mp3", MOCKED_FILE_SIZE),
                (slave, "new_high_quality.flac", MOCKED_FILE_SIZE),
            ],
            None,
        );

        // Seed a Card Mapping to the Slave track
        conn.execute(
            &format!("INSERT INTO {CARD_MAPPINGS} ({CARD_ID}, {TRACK_ID}) VALUES (?1, ?2)"),
            rusqlite::params!["SLAVE_CARD_RFID", slave],
        )?;

        // Seed Metadata for both (Master has good metadata, Slave has none or dummy)
        conn.execute(
            &format!(
                "INSERT INTO {TRACK_METADATA} ({TRACK_ID}, {TITLE}, {ARTIST}) VALUES (?1, ?2, ?3)"
            ),
            rusqlite::params![master, "Good Title", "Great Artist"],
        )?;
        conn.execute(
            &format!(
                "INSERT INTO {TRACK_METADATA} ({TRACK_ID}, {TITLE}, {ARTIST}) VALUES (?1, ?2, ?3)"
            ),
            rusqlite::params![slave, "Dummy Title", "Dummy Artist"],
        )?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // Act: Merge slave into master
        assert!(
            storage.merge_tracks(master, slave, false).is_err(),
            "expected failure because slave had metadata"
        );
        storage.merge_tracks(master, slave, true)?;

        // Assert 1: Both files should now belong to the master track ID
        let mut stmt = storage.db.prepare(&format!(
            "SELECT {PATH} FROM {FILES} WHERE {TRACK_ID} = ?1 ORDER BY {PATH}"
        ))?;
        let files: Vec<String> = stmt
            .query_map([master], |r| r.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(files.len(), 2);
        assert_eq!(files[0], "new_high_quality.flac");
        assert_eq!(files[1], "old_low_quality.mp3");

        // Assert 2: The card mapping should have transferred seamlessly to the master track
        let card_track_id: i64 = storage.db.query_row(
            &format!("SELECT {TRACK_ID} FROM {CARD_MAPPINGS} WHERE {CARD_ID} = ?1"),
            ["SLAVE_CARD_RFID"],
            |r| r.get(0),
        )?;
        assert_eq!(card_track_id, master);

        // Assert 3: Slave track and its metadata are completely gone
        let slave_track_exists: i64 = storage.db.query_row(
            &format!("SELECT COUNT(*) FROM {TRACKS} WHERE {TRACK_ID} = ?1"),
            [slave],
            |r| r.get(0),
        )?;
        assert_eq!(slave_track_exists, 0);

        let slave_meta_exists: i64 = storage.db.query_row(
            &format!("SELECT COUNT(*) FROM {TRACK_METADATA} WHERE {TRACK_ID} = ?1"),
            [slave],
            |r| r.get(0),
        )?;
        assert_eq!(slave_meta_exists, 0);

        Ok(())
    }

    #[test]
    fn test_add_file_to_track_fails_if_master_missing() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("song_hq.mp3");
        std::fs::write(&path, b"audio_data")?;

        let mut storage = setup_storage(dir.path())?;

        let result = storage.add_file_to_track(99999, &path);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_add_file_to_track_success() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("song_hq.mp3");
        std::fs::write(&path, b"audio_high_res")?;

        let mut storage = setup_storage(dir.path())?;

        // 1. Manually insert an empty track row into the ledger to get a master ID
        storage
            .db
            .execute("INSERT INTO tracks DEFAULT VALUES", [])?;
        let master_id: i64 = storage.db.last_insert_rowid();

        // 2. Act: Link our new physical file directly to that master ID
        storage.add_file_to_track(master_id, &path)?;

        // 3. Assert: Verify the file row points to our master ID
        let mut stmt = storage
            .db
            .prepare("SELECT track_id, path FROM files LIMIT 1")?;

        let (linked_track_id, file_path) = stmt.query_row([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        assert_eq!(linked_track_id, master_id);
        assert!(file_path.ends_with("song_hq.mp3"));

        Ok(())
    }

    #[test]
    fn test_update_db_with_new_files() -> anyhow::Result<()> {
        let dir = tempdir()?;

        // --- create real files ---
        let path1 = dir.path().join("a.mp3");
        let path2 = dir.path().join("b.mp3");

        std::fs::write(&path1, b"audio_a")?;
        std::fs::write(&path2, b"audio_b")?;

        let mut storage = setup_storage(dir.path())?;

        // IMPORTANT:
        // insert tracks but NO file rows yet
        let track1 = FileHash::from_file(&path1)?;
        let track2 = FileHash::from_file(&path2)?;
        // --- run update ---
        let result = storage.update_db_with_new_files()?;

        // --- verify return value ---
        assert_eq!(result.len(), 2);

        let hashes: HashSet<_> = result
            .iter()
            .flat_map(|h| h.1.clone().into_iter())
            .map(|f| f.hash)
            .collect();
        assert!(hashes.contains(&track1));
        assert!(hashes.contains(&track2));

        // --- verify DB state ---
        let mut stmt = storage
            .db
            .prepare("SELECT file_hash, path FROM files ORDER BY path")?;

        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(rows.len(), 2);

        assert!(
            rows.iter()
                .any(|(id, p)| id == &track1.to_string() && p.ends_with("a.mp3"))
        );
        assert!(
            rows.iter()
                .any(|(id, p)| id == &track2.to_string() && p.ends_with("b.mp3"))
        );

        Ok(())
    }

    #[test]
    fn test_insert_files_fresh_tracks() -> anyhow::Result<()> {
        let mut storage = setup_clean_storage()?;

        let file_a = HashedFile::new(
            mock_hash(1),
            FileWithMeta {
                loc: Location::from_path("a.mp3"),
                file_size: 100,
            },
        );
        let file_b = HashedFile::new(
            mock_hash(2),
            FileWithMeta {
                loc: Location::from_path("b.mp3"),
                file_size: 200,
            },
        );

        // Path 1: Insert completely brand new files
        let result = storage.insert_files([file_a.clone(), file_b.clone()])?;

        // Should return both items under 2 distinct generated track IDs
        assert_eq!(result.len(), 2);

        // Verify update time was bumped because rows were inserted
        let count: i64 =
            storage
                .db
                .query_row(&format!("SELECT COUNT(*) FROM {UPDATES}"), [], |r| r.get(0))?;
        assert_eq!(count, 1);

        Ok(())
    }

    #[test]
    fn test_insert_files_reuses_track_id_for_matching_hashes() -> anyhow::Result<()> {
        let mut storage = setup_clean_storage()?;
        let shared_hash = mock_hash(1);

        let file_a = HashedFile::new(
            shared_hash.clone(),
            FileWithMeta {
                loc: Location::from_path("a.mp3"),
                file_size: 100,
            },
        );
        let file_b = HashedFile::new(
            shared_hash.clone(),
            FileWithMeta {
                loc: Location::from_path("b.mp3"),
                file_size: 100,
            },
        );

        // Path 2: Distinct locations, but identical file content hashes
        let result = storage.insert_files([file_a, file_b])?;

        // Should group both files under exactly ONE TrackId entry
        assert_eq!(result.len(), 1);
        let (_, grouped_files) = result.iter().next().unwrap();
        assert_eq!(grouped_files.len(), 2);

        Ok(())
    }

    #[test]
    fn test_insert_files_ignores_duplicate_locations() -> anyhow::Result<()> {
        let mut storage = setup_clean_storage()?;

        let file_original = HashedFile::new(
            mock_hash(1),
            FileWithMeta {
                loc: Location::from_path("collision.mp3"),
                file_size: 100,
            },
        );
        // Different hash, but exact same target location path
        let file_conflict = HashedFile::new(
            mock_hash(2),
            FileWithMeta {
                loc: Location::from_path("collision.mp3"),
                file_size: 999,
            },
        );

        // Seed the first file safely
        storage.insert_files([file_original])?;

        // Path 3: Attempt to insert to a primary key location that already exists
        let result = storage.insert_files([file_conflict])?;

        // Should be completely ignored by `INSERT OR IGNORE` and excluded from return map
        assert!(
            result.is_empty(),
            "Conflicting locations must be skipped and omitted from return payload"
        );

        // DB state verification: Total file count in DB should still be exactly 1
        let total_files: i64 =
            storage
                .db
                .query_row(&format!("SELECT COUNT(*) FROM {FILES}"), [], |r| r.get(0))?;
        assert_eq!(total_files, 1);

        Ok(())
    }

    #[test]
    fn test_get_or_create_track_id() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;
        let tx = conn.transaction()?;

        let hash_a = mock_hash(1);
        let hash_b = mock_hash(2);

        // 1. Fresh hashes must create unique, new track IDs
        let id_a1 = Storage::get_or_create_track_id(&tx, &hash_a)?;
        let id_b = Storage::get_or_create_track_id(&tx, &hash_b)?;
        assert_ne!(id_a1, id_b);

        // 2. Link hash_a to its track ID in the files table
        tx.execute(
        &format!("INSERT INTO {FILES} ({USB_LABEL}, {PATH}, {TRACK_ID}, {FILE_SIZE}, {FILE_HASH}) VALUES (?1, ?2, ?3, ?4, ?5)"),
            rusqlite::params!["USB", "a.mp3", id_a1, 100, &hash_a.to_string()],
        )?;

        // 3. Querying hash_a again must reuse that exact track ID
        let id_a2 = Storage::get_or_create_track_id(&tx, &hash_a)?;
        assert_eq!(id_a1, id_a2);

        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_insert_tracks() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let file1 = FileWithMeta {
            loc: Location::from_path("a.mp3"),
            file_size: 100,
        };
        let file2 = FileWithMeta {
            loc: Location::from_path("b.mp3"),
            file_size: 200,
        };

        let track1 = mock_hash(1);
        let track2 = mock_hash(2);

        // 1. Run the insert and capture the generated Track IDs from the returned map
        let result = storage.insert_files([
            HashedFile::new(track1.clone(), file1.clone()),
            HashedFile::new(track2.clone(), file2.clone()),
        ])?;

        // Find which track ID belongs to which hash dynamically
        let id1 = result
            .iter()
            .find(|(_, files)| files.iter().any(|f| f.hash == track1))
            .map(|(id, _)| *id)
            .unwrap();
        let id2 = result
            .iter()
            .find(|(_, files)| files.iter().any(|f| f.hash == track2))
            .map(|(id, _)| *id)
            .unwrap();

        // 2. Verify DB state
        let query =
            format!("SELECT {TRACK_ID}, {PATH}, {FILE_SIZE} FROM {FILES} WHERE {TRACK_ID} = ?1");
        let mut stmt = storage.db.prepare(&query)?;

        // Check file 1 row
        let row1: (i64, String, i64) =
            stmt.query_row([id1], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        assert_eq!(row1.0, id1);
        assert_eq!(row1.1, "a.mp3");
        assert_eq!(row1.2, 100);

        // Check file 2 row
        let row2: (i64, String, i64) =
            stmt.query_row([id2], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        assert_eq!(row2.0, id2);
        assert_eq!(row2.1, "b.mp3");
        assert_eq!(row2.2, 200);

        Ok(())
    }

    #[test]
    fn test_get_track_success() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let dir = tempdir()?;
        let file_path = dir.path().join("song.mp3");

        // Create valid music file
        fs::write(&file_path, b"x")?;

        let tracks = insert_tracks(&mut conn, 1);
        insert_fake_files(
            &mut conn,
            [(
                tracks[0],
                &replace_windows_slashes(&file_path),
                MOCKED_FILE_SIZE,
            )],
            None,
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let (track, path, _) = storage.find_track_file(tracks[0])?;

        assert_eq!(track, tracks[0]);
        assert_eq!(path, file_path);

        Ok(())
    }

    #[test]
    fn test_get_track_success_usb() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let temp = tempdir()?;

        // simulate USB mount root
        let usb_mount = temp.path().join("usb");
        std::fs::create_dir_all(&usb_mount)?;

        // actual file inside USB
        let file_path = usb_mount.join("song.mp3");
        std::fs::write(&file_path, b"x")?;

        // insert USB location into DB
        let usb_label = "DJ_USB";

        let tracks = insert_tracks(&mut conn, 1);
        insert_fake_files(
            &mut conn,
            [(tracks[0], "song.mp3", MOCKED_FILE_SIZE)],
            Some(usb_label.to_string()),
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // mock resolver
        storage.fs.loc_resolver =
            LocationResolver::test_resolver([(usb_label.to_string(), usb_mount.clone())]);

        let (track, path, loc) = storage.find_track_file(tracks[0])?;

        assert_eq!(track, tracks[0]);
        assert_eq!(path, file_path);

        match loc {
            Location::Usb { label, path } => {
                assert_eq!(label, usb_label);
                assert_eq!(path, PathBuf::from("song.mp3"));
            }
            _ => panic!("expected USB location"),
        }

        Ok(())
    }

    #[test]
    fn test_get_track_invalid_paths() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let dir = tempdir()?;
        let bad_path = dir.path().join("song.txt"); // invalid extension

        fs::write(&bad_path, b"x")?;

        let track_id = insert_tracks(&mut conn, 1)[0];
        insert_fake_files(
            &mut conn,
            [(
                track_id,
                &replace_windows_slashes(&bad_path),
                MOCKED_FILE_SIZE,
            )],
            None,
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let err = storage.find_track_file(track_id).unwrap_err();

        assert!(matches!(err, StorageError::InvalidTrackFile { .. }));

        Ok(())
    }

    #[test]
    fn test_get_track_multiple_paths_picks_valid() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let dir = tempdir()?;

        let bad = dir.path().join("bad.txt");
        let good = dir.path().join("good.mp3");

        fs::write(&bad, b"x")?;
        fs::write(&good, b"x")?;

        let track_id = insert_tracks(&mut conn, 1)[0];
        insert_fake_files(
            &mut conn,
            [
                (track_id, replace_windows_slashes(&bad), MOCKED_FILE_SIZE),
                (track_id, replace_windows_slashes(&good), MOCKED_FILE_SIZE),
            ],
            None,
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let (_, path, _) = storage.find_track_file(track_id)?;

        assert_eq!(path, good);

        Ok(())
    }

    #[test]
    fn test_get_track_not_in_db() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let err = storage.find_track_file(0).unwrap_err();

        assert!(matches!(err, StorageError::TrackNotFound(..)));

        Ok(())
    }

    #[test]
    fn test_get_track_metadata() {
        // ---------- Setup in-memory DB ----------
        let temp_dir = tempdir().unwrap();
        let mut storage = setup_storage(temp_dir.path()).unwrap();
        // ---------- Insert test data ----------
        let track_id = insert_tracks(&mut storage.db, 1)[0];

        storage
            .db
            .execute(
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
            )
            .unwrap();

        let meta = storage.get_track_metadata(track_id.into()).unwrap();

        // ---------- Assertions ----------
        let metadata = meta.expect("Metadata should be present");
        assert_eq!(metadata.title, "Test Song");
        assert_eq!(metadata.artist, "Test Artist");
        assert_eq!(metadata.year, Some(2026));
        assert_eq!(metadata.label.as_deref(), Some("Test Label"));
        assert_eq!(
            metadata.artwork.as_ref().map(|a| a.0.as_str()),
            Some("cover.jpg")
        );
    }

    fn assert_files<I>(results: &HashMap<TrackId, HashSet<Location>>, expected: I)
    where
        I: IntoIterator<Item = (TrackId, Vec<&'static str>)>,
    {
        for (id, files) in expected {
            let expected_set: HashSet<String> = files.into_iter().map(|s| s.to_string()).collect();
            let actual_set: HashSet<String> = results[&id].iter().map(|l| l.to_string()).collect();
            assert_eq!(
                actual_set, expected_set,
                "Files for track {:?} do not match exactly",
                id
            );
        }
    }

    #[test]
    fn test_find_files() {
        let mut conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        let tracks = insert_tracks(&mut conn, 3);

        let data = vec![
            (tracks[0], "Some Artist - Track Name.mp3", MOCKED_FILE_SIZE),
            (tracks[1], "AnotherArtist_Track Name.flac", MOCKED_FILE_SIZE),
            (
                tracks[2],
                "completely-different-track.mp3",
                MOCKED_FILE_SIZE,
            ),
        ];

        insert_fake_files(&mut conn, data, None);

        let mut storage = Storage::from_existing_conn(conn, LibrarySource::default());

        // Search for a liberal match
        let results = storage.find_files("track name", false).unwrap();
        assert_files(
            &results,
            [
                (tracks[0], vec!["Some Artist - Track Name.mp3"]),
                (tracks[1], vec!["AnotherArtist_Track Name.flac"]),
            ],
        );

        // Search with different casing and spaces
        let results2 = storage.find_files("another", false).unwrap();

        assert_files(
            &results2,
            [(tracks[1], vec!["AnotherArtist_Track Name.flac"])],
        );

        // Search for trackid
        let results3 = storage.find_files(&mock_hash_str(3), false).unwrap();
        assert_files(
            &results3,
            [(tracks[2], vec!["completely-different-track.mp3"])],
        );

        // Search for non-existent track
        let results4 = storage.find_files("nonexistent", false).unwrap();
        assert!(results4.is_empty());
    }

    #[test]
    fn test_find_files_metadata_and_no_meta() {
        let mut conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        // --- Insert tracks ---
        let tracks = insert_tracks(&mut conn, 3);

        // --- Insert files ---
        insert_fake_files(
            &mut conn,
            vec![
                (tracks[0], "foo.mp3", MOCKED_FILE_SIZE),
                (tracks[1], "bar.mp3", MOCKED_FILE_SIZE),
                (tracks[2], "baz.mp3", MOCKED_FILE_SIZE),
            ],
            None,
        );

        // --- Insert metadata manually (ONLY for 1 and 2) ---
        conn.execute(
            "INSERT INTO track_metadata (track_id, title, artist, year, label, artwork_url)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL)",
            rusqlite::params![tracks[0], "Cool Track", "DJ Alpha"],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO track_metadata (track_id, title, artist, year, label, artwork_url)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL)",
            rusqlite::params![tracks[1], "Another Banger", "Beta Artist"],
        )
        .unwrap();

        let mut storage = Storage::from_existing_conn(conn, LibrarySource::default());

        // --- Search by artist ---
        let results = storage.find_files("alpha", false).unwrap();
        assert_files(&results, [(tracks[0], vec!["foo.mp3"])]);

        // --- Search by title ---
        let results = storage.find_files("banger", false).unwrap();
        assert_files(&results, [(tracks[1], vec!["bar.mp3"])]);

        // --- no_meta: should return ONLY track 3 ---
        let results = storage.find_files("", true).unwrap();
        assert_files(&results, [(tracks[2], vec!["baz.mp3"])]);

        // --- combined: query + no_meta (should be empty here) ---
        let results = storage.find_files("cool", true).unwrap();
        assert!(results.is_empty());

        // metadata exists but doesn't match query
        let results = storage.find_files("gamma", false).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_files_by_card_id() -> anyhow::Result<()> {
        let mut conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        let tracks = insert_tracks(&mut conn, 2);

        insert_fake_files(
            &mut conn,
            vec![
                (tracks[0], "card_mapped_1.mp3", MOCKED_FILE_SIZE),
                (tracks[1], "card_mapped_2.mp3", MOCKED_FILE_SIZE),
            ],
            None,
        );

        // Link card IDs to tracks
        conn.execute(
            &format!("INSERT INTO {CARD_MAPPINGS} ({CARD_ID}, {TRACK_ID}) VALUES (?1, ?2)"),
            rusqlite::params!["RFID_CARD_XYZ_123", tracks[0]],
        )?;
        conn.execute(
            &format!("INSERT INTO {CARD_MAPPINGS} ({CARD_ID}, {TRACK_ID}) VALUES (?1, ?2)"),
            rusqlite::params!["RFID_CARD_ABC_789", tracks[1]],
        )?;

        let mut storage = Storage::from_existing_conn(conn, LibrarySource::default());

        // Test exact Card ID match
        let results = storage.find_files("RFID_CARD_XYZ_123", false)?;
        assert_files(&results, [(tracks[0], vec!["card_mapped_1.mp3"])]);

        // Test case-insensitive/partial card ID match
        let results = storage.find_files("abc", false)?;
        assert_files(&results, [(tracks[1], vec!["card_mapped_2.mp3"])]);

        Ok(())
    }

    #[test]
    fn test_find_files_empty_query_returns_all() -> anyhow::Result<()> {
        let mut conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        let tracks = insert_tracks(&mut conn, 2);

        insert_fake_files(
            &mut conn,
            vec![
                (tracks[0], "file_a.mp3", MOCKED_FILE_SIZE),
                (tracks[1], "file_b.mp3", MOCKED_FILE_SIZE),
            ],
            None,
        );

        let mut storage = Storage::from_existing_conn(conn, LibrarySource::default());

        // Empty query string should match everything
        let results = storage.find_files("", false)?;
        assert_files(
            &results,
            [
                (tracks[0], vec!["file_a.mp3"]),
                (tracks[1], vec!["file_b.mp3"]),
            ],
        );

        Ok(())
    }

    static MOCKED_FILE_SIZE: i64 = 228;

    fn insert_file(
        conn: &Connection,
        track_id: i64,
        path: &str,
        usb_label: &Option<String>,
        file_size: i64,
    ) {
        let hash = mock_hash(track_id as i32);
        conn.execute(
            &format!(
                "INSERT INTO {FILES} ({TRACK_ID}, {FILE_HASH}, {USB_LABEL}, {PATH}, {FILE_SIZE}) VALUES (?1, ?2, ?3, ?4, ?5)"
            ),
            params![
                track_id,
                hash.to_string(),
                usb_label.clone().unwrap_or(String::new()),
                path,
                file_size
            ],
        )
        .unwrap();
    }

    #[test]
    fn test_forget_path_removes_files_and_tracks() {
        let conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        let storage = Storage::from_existing_conn(conn, LibrarySource::default());
        let mut storage = storage;

        let tracks = insert_tracks(&mut storage.db, 3);
        let track_files = [
            (tracks[0], "/music/track_a1.mp3", MOCKED_FILE_SIZE),
            (tracks[0], "/music/subdir/track_a2.mp3", MOCKED_FILE_SIZE),
            (tracks[1], "/music/track_b.mp3", MOCKED_FILE_SIZE),
            (tracks[2], "/hello/track_c.mp3", MOCKED_FILE_SIZE), // outside deleted path
            (tracks[0], "/hello/track_a3.mp3", MOCKED_FILE_SIZE), // outside deleted path
        ];
        insert_fake_files(&storage.db, track_files, None);

        // Forget top-level directory
        let path_to_forget = Path::new("/music");
        let report = storage.forget_path(path_to_forget).unwrap();

        assert_eq!(report.removed_files, 3); // a1 + a2 + b
        assert_eq!(report.affected_tracks, 2); // a + b
        assert_eq!(report.removed_tracks, 1); // b

        // Remaining DB entries
        let remaining: Vec<TrackId> = storage
            .db
            .prepare("SELECT track_id FROM files")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(remaining.len() == 2);
    }

    #[test]
    fn test_forget_windows() {
        let conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        let storage = Storage::from_existing_conn(conn, LibrarySource::default());
        let mut storage = storage;

        let track = insert_tracks(&mut storage.db, 1)[0];
        let track_files = [
            (track, "C:/music/track_a1.mp3", MOCKED_FILE_SIZE),
            (track, "C:/music/subdir/track_a2.mp3", MOCKED_FILE_SIZE),
        ];
        insert_fake_files(&storage.db, track_files, None);

        let path_to_forget = Path::new("C:\\music\\subdir");
        let report = storage.forget_path(path_to_forget).unwrap();

        assert_eq!(report.removed_files, 1);
        assert_eq!(report.affected_tracks, 1);
        assert_eq!(report.removed_tracks, 0);

        // Remaining DB entries
        let remaining: Vec<String> = storage
            .db
            .prepare("SELECT path FROM files")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(remaining, vec!["C:/music/track_a1.mp3"]);
    }

    #[test]
    fn test_forget_path_empty_dir_no_crash() {
        let conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        let storage = Storage::from_existing_conn(conn, LibrarySource::default());
        let mut storage = storage;

        // Forget a directory that doesn't exist
        let path_to_forget = Path::new("/nonexistent");
        let report = storage.forget_path(path_to_forget).unwrap();

        assert_eq!(report.removed_files, 0);
        assert_eq!(report.affected_tracks, 0);
        assert_eq!(report.removed_tracks, 0);
    }

    mod update_meta_tests {
        use crate::{
            operations::MetadataUpdate,
            track::{ArtworkRef, TrackMetadata},
        };

        use super::*;

        fn tid() -> TrackId {
            1
        }

        fn old_meta() -> TrackMetadata {
            TrackMetadata {
                title: "Old Title".into(),
                artist: "Old Artist".into(),
                year: Some(2000),
                label: Some("Old Label".into()),
                artwork: Some(ArtworkRef("old.jpg".into())),
            }
        }

        #[test]
        fn insert_new_metadata_success() {
            let new = MetadataUpdate {
                title: Some("New Title".into()),
                artist: Some("New Artist".into()),
                year: Some(2020),
                label: None,
                artwork: None,
            };

            let meta = Storage::update_meta(tid(), None, new, false).unwrap();

            assert_eq!(meta.title, "New Title");
            assert_eq!(meta.artist, "New Artist");
            assert_eq!(meta.year, Some(2020));
        }

        #[test]
        fn insert_missing_required_fails() {
            let new = MetadataUpdate {
                title: Some("Title".into()),
                artist: None,
                year: None,
                label: None,
                artwork: None,
            };

            let err = Storage::update_meta(tid(), None, new, false).unwrap_err();

            assert!(matches!(err, StorageError::RequiredMetaMissing(_)));
        }

        #[test]
        fn merge_without_overwrite_fills_missing() {
            let mut old = old_meta();
            old.year = None;

            let new = MetadataUpdate {
                title: None,
                artist: None,
                year: Some(2023),
                label: None,
                artwork: None,
            };

            let meta = Storage::update_meta(tid(), Some(old), new, false).unwrap();

            assert_eq!(meta.year, Some(2023));
        }

        #[test]
        fn merge_without_overwrite_conflict_optional() {
            let new = MetadataUpdate {
                title: None,
                artist: None,
                year: Some(2025),
                label: None,
                artwork: None,
            };

            let err = Storage::update_meta(tid(), Some(old_meta()), new, false).unwrap_err();

            assert!(matches!(err, StorageError::MetadataOverwriteDenied(_)));
        }

        #[test]
        fn merge_without_overwrite_conflict_title() {
            let new = MetadataUpdate {
                title: Some("New".into()),
                artist: None,
                year: None,
                label: None,
                artwork: None,
            };

            let err = Storage::update_meta(tid(), Some(old_meta()), new, false).unwrap_err();

            assert!(matches!(err, StorageError::MetadataOverwriteDenied(_)));
        }

        #[test]
        fn overwrite_optional_fields() {
            let new = MetadataUpdate {
                title: None,
                artist: None,
                year: Some(2030),
                label: Some("New Label".into()),
                artwork: None,
            };

            let meta = Storage::update_meta(tid(), Some(old_meta()), new, true).unwrap();

            assert_eq!(meta.year, Some(2030));
            assert_eq!(meta.label.as_deref(), Some("New Label"));
        }

        #[test]
        fn overwrite_title_artist() {
            let new = MetadataUpdate {
                title: Some("New Title".into()),
                artist: Some("New Artist".into()),
                year: None,
                label: None,
                artwork: None,
            };

            let meta = Storage::update_meta(tid(), Some(old_meta()), new, true).unwrap();

            assert_eq!(meta.title, "New Title");
            assert_eq!(meta.artist, "New Artist");
        }

        #[test]
        fn overwrite_keeps_old_when_none() {
            let old = old_meta();

            let new = MetadataUpdate {
                title: None,
                artist: None,
                year: None,
                label: None,
                artwork: None,
            };

            let meta = Storage::update_meta(tid(), Some(old.clone()), new, true).unwrap();

            assert_eq!(meta.year, old.year);
            assert_eq!(meta.label, old.label);
        }

        #[test]
        fn noop_update_returns_old() {
            let old = old_meta();

            let new = MetadataUpdate {
                title: None,
                artist: None,
                year: None,
                label: None,
                artwork: None,
            };

            let meta = Storage::update_meta(tid(), Some(old.clone()), new, false).unwrap();

            assert_eq!(meta.year, old.year);
            assert_eq!(meta.label, old.label);
        }
    }

    #[test]
    fn test_update_track_metadata_track_missing() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let update = MetadataUpdate {
            title: Some("Test Title".into()),
            artist: Some("artist".into()),
            year: None,
            label: None,
            artwork: None,
        };

        let result = storage.update_track_metadata(42, update, false);

        assert!(matches!(
            result,
            Err(StorageError::TrackNotFound(id)) if id == "42".to_string()
        ));

        Ok(())
    }

    #[test]
    fn test_update_track_metadata_insert_new_metadata() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let track = insert_tracks(&mut conn, 1)[0];

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let update = MetadataUpdate {
            title: Some("Song A".into()),
            artist: Some("Artist A".into()),
            year: Some(1999),
            label: None,
            artwork: None,
        };

        storage.update_track_metadata(track, update, false)?;

        // Verify
        let meta = storage.get_track_metadata(track)?;
        let meta = meta.unwrap();
        assert_eq!(meta.title, "Song A");
        assert_eq!(meta.artist, "Artist A");

        Ok(())
    }

    #[test]
    fn test_update_track_metadata_reject_overwrite() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let track = insert_tracks(&mut conn, 1)[0];

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // First insert
        storage.update_track_metadata(
            track,
            MetadataUpdate {
                title: Some("Original".into()),
                artist: Some("helo".into()),
                year: None,
                label: None,
                artwork: None,
            },
            false,
        )?;

        // Attempt overwrite without permission
        let result = storage.update_track_metadata(
            track,
            MetadataUpdate {
                title: Some("New Title".into()),
                artist: Some("test".into()),
                year: None,
                label: None,
                artwork: None,
            },
            false,
        );

        assert!(matches!(
            result,
            Err(StorageError::MetadataOverwriteDenied { .. })
        ));

        Ok(())
    }

    #[test]
    fn test_update_track_metadata_allow_overwrite() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let track = insert_tracks(&mut conn, 1)[0];

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        storage.update_track_metadata(
            track,
            MetadataUpdate {
                title: Some("Original".into()),
                artist: Some("blabla".into()),
                year: None,
                label: None,
                artwork: None,
            },
            false,
        )?;

        storage.update_track_metadata(
            track,
            MetadataUpdate {
                title: Some("Updated".into()),
                artist: None,
                year: None,
                label: None,
                artwork: None,
            },
            true,
        )?;

        let meta = storage.get_track_metadata(track)?;
        assert_eq!(meta.unwrap().title, "Updated");

        Ok(())
    }

    mod check_tests {
        use tempfile::tempdir;

        use crate::{
            location::{Location, replace_windows_slashes},
            operations::tests::{
                MOCKED_FILE_SIZE, insert_fake_files, insert_real_files, insert_tracks, mock_hash,
                setup_storage,
            },
        };

        #[test]
        fn test_check_new_no_new_files() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            std::fs::write(&path, b"x")?;

            let tracks = insert_tracks(&mut storage.db, 1);
            insert_real_files(
                &mut storage.db,
                [(tracks[0], replace_windows_slashes(&path))],
                None,
            );

            let diff = storage.check_new()?;
            assert!(diff.is_empty());

            Ok(())
        }

        #[test]
        fn test_check_new_detects_new_track() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("new_song.mp3");
            std::fs::write(&path, b"x")?;

            // DB is empty

            let diff = storage.check_new()?.into_iter().collect::<Vec<_>>();
            assert!(diff.len() == 1);
            let only = &diff[0];
            assert_eq!(only.loc, Location::from_path(path));

            Ok(())
        }

        #[test]
        fn test_check_new_detects_additional_location() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path1 = dir.path().join("song1.mp3");
            let path2 = dir.path().join("song2.mp3");

            std::fs::write(&path1, b"x")?;
            std::fs::write(&path2, b"x")?;

            // DB only knows about first path
            let track_id = insert_tracks(&mut storage.db, 1)[0];
            insert_real_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&path1))],
                None,
            );

            let diff = storage.check_new()?.into_iter().collect::<Vec<_>>();

            assert_eq!(diff.len(), 1);
            assert_eq!(diff[0].loc, Location::from_path(path2));

            Ok(())
        }

        #[test]
        fn test_check_new_ignores_missing_fs_tracks() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");

            let track_id = insert_tracks(&mut storage.db, 1)[0];
            insert_fake_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&path), MOCKED_FILE_SIZE)],
                None,
            );

            let diff = storage.check_new()?;

            assert!(diff.is_empty());

            Ok(())
        }

        #[test]
        fn test_check_missing_no_missing_tracks() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            std::fs::write(&path, b"x")?;

            let track_id = insert_tracks(&mut storage.db, 1)[0];
            insert_real_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&path))],
                None,
            );

            let diff = storage.check_missing()?;
            assert!(diff.is_empty());

            Ok(())
        }

        #[test]
        fn test_check_missing_detects_fully_missing_track() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            // file NOT created

            let track_id = insert_tracks(&mut storage.db, 1)[0];

            insert_fake_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&path), MOCKED_FILE_SIZE)],
                None,
            );

            let diff = storage.check_missing()?;

            assert_eq!(diff.len(), 1);
            assert!(diff.contains_key(&track_id));
            let locs = diff.get(&track_id).unwrap().into_iter().collect::<Vec<_>>();
            assert_eq!(locs.len(), 1);
            assert_eq!(locs[0].loc, Location::from_path(path));

            Ok(())
        }

        #[test]
        fn test_check_missing_detects_renamed_track() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            let renamed = dir.path().join("renamed.mp3");

            std::fs::write(&renamed, b"x")?;

            let track_id = insert_tracks(&mut storage.db, 1)[0];

            insert_fake_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&path), MOCKED_FILE_SIZE)],
                None,
            );

            let diff = storage.check_missing()?;

            assert_eq!(diff.len(), 1);
            assert!(diff.contains_key(&track_id));
            let locs = diff.get(&track_id).unwrap().into_iter().collect::<Vec<_>>();
            assert_eq!(locs.len(), 1);
            assert_eq!(locs[0].loc, Location::from_path(path));

            Ok(())
        }

        #[test]
        fn test_check_stale_no_stale_tracks() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            std::fs::write(&path, b"x")?;

            let track_id = insert_tracks(&mut storage.db, 1)[0];

            insert_real_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&path))],
                None,
            );

            let stale = storage.check_stale()?;

            assert!(stale.metadata_only.is_empty());
            assert!(stale.dangling.is_empty());

            Ok(())
        }

        #[test]
        fn test_check_stale_detects_dangling_track() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let track_id = insert_tracks(&mut storage.db, 1)[0];

            let stale = storage.check_stale()?;

            assert!(stale.metadata_only.is_empty());

            assert_eq!(stale.dangling.len(), 1);
            assert_eq!(stale.dangling[0], track_id);

            Ok(())
        }

        #[test]
        fn test_check_stale_detects_metadata_only_track() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let track_id = insert_tracks(&mut storage.db, 1)[0];

            storage
                .db
                .execute(
                    r#"
            INSERT INTO track_metadata (track_id, title, artist)
            VALUES (?1, ?2, ?3)
            "#,
                    [&track_id.to_string(), "Test Song", "Test Artist"],
                )
                .unwrap();

            let stale = storage.check_stale()?;

            assert!(stale.dangling.is_empty());

            assert_eq!(stale.metadata_only.len(), 1);
            assert_eq!(stale.metadata_only[0], track_id);

            Ok(())
        }

        #[test]
        fn test_check_stale_detects_only_truly_stale_tracks() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let existing = dir.path().join("existing.mp3");
            std::fs::write(&existing, b"x")?;

            let tracks = insert_tracks(&mut storage.db, 3);
            let good_track = tracks[0];
            let dangling_track = tracks[1];
            let metadata_only_track = tracks[2];

            insert_real_files(
                &mut storage.db,
                [(good_track, replace_windows_slashes(&existing))],
                None,
            );

            storage
                .db
                .execute(
                    r#"
            INSERT INTO track_metadata (track_id, title, artist)
            VALUES (?1, ?2, ?3)
            "#,
                    [&metadata_only_track.to_string(), "Test Song", "Test Artist"],
                )
                .unwrap();

            let stale = storage.check_stale()?;

            assert_eq!(stale.metadata_only.len(), 1);
            assert_eq!(stale.metadata_only[0], metadata_only_track);

            assert_eq!(stale.dangling.len(), 1);
            assert_eq!(stale.dangling[0], dangling_track);

            Ok(())
        }
    }

    #[test]
    fn test_clean_dangling_does_not_delete_file_only_or_metadata_only_tracks() -> anyhow::Result<()>
    {
        let dir = tempdir()?;
        let mut storage = setup_storage(dir.path())?;

        let existing = dir.path().join("existing.mp3");
        std::fs::write(&existing, b"x")?;

        let tracks = insert_tracks(&mut storage.db, 4);
        let (good_track, dangling_track, metadata_only_track, file_only_track) =
            (tracks[0], tracks[1], tracks[2], tracks[3]);

        // Good track: file exists
        insert_real_files(
            &mut storage.db,
            [(good_track, replace_windows_slashes(&existing))],
            None,
        );

        // File-only track: file exists but NO metadata
        let file_only_path = dir.path().join("file_only.mp3");
        std::fs::write(&file_only_path, b"y")?;

        insert_real_files(
            &mut storage.db,
            [(file_only_track, replace_windows_slashes(&file_only_path))],
            None,
        );

        // Metadata-only track
        storage.db.execute(
            r#"
        INSERT INTO track_metadata (track_id, title, artist)
        VALUES (?1, ?2, ?3)
        "#,
            params![&metadata_only_track, "Test Song", "Test Artist"],
        )?;

        // --------------------------------------------------
        // Act
        // --------------------------------------------------

        let report = storage.clean_dangling()?;

        // Only truly dangling track should be removed
        assert_eq!(report.removed_tracks, 1);

        let stale = storage.check_stale()?;

        // metadata-only MUST survive
        assert_eq!(stale.metadata_only.len(), 1);
        assert_eq!(stale.metadata_only[0], metadata_only_track);

        // file-only MUST NOT be touched (not dangling)
        assert!(!stale.metadata_only.contains(&file_only_track));
        assert!(!stale.dangling.contains(&file_only_track));

        // good track must remain valid
        assert!(!stale.metadata_only.contains(&good_track));
        assert!(!stale.dangling.contains(&good_track));

        // dangling must be gone
        assert!(!stale.dangling.contains(&dangling_track));

        Ok(())
    }

    mod usb_conversion {
        use std::path::PathBuf;

        use crate::{error::StorageError, location::Location, operations::LocationRow};

        #[test]
        fn empty_usb_label_error() {
            let location = Location::Usb {
                label: "".to_string(),
                path: PathBuf::from("hello"),
            };

            assert!(matches!(
                LocationRow::from_location(location).unwrap_err(),
                StorageError::Internal(..)
            ));
        }

        #[test]
        fn test_location_file_roundtrip() {
            let original = Location::File {
                path: PathBuf::from("/home/user/music/song.mp3"),
            };

            let row: LocationRow = LocationRow::from_location(original.clone()).unwrap();
            let restored: Location = row.into();

            match restored {
                Location::File { path } => {
                    assert_eq!(path, PathBuf::from("/home/user/music/song.mp3"));
                }
                Location::Usb { .. } => panic!("expected File variant, got Usb"),
            }
        }

        #[test]
        fn test_location_usb_roundtrip() {
            let original = Location::Usb {
                label: "DJ_USB".to_string(),
                path: PathBuf::from("music/song.mp3"),
            };

            let row: LocationRow = LocationRow::from_location(original.clone()).unwrap();
            let restored: Location = row.into();

            match restored {
                Location::Usb { label, path } => {
                    assert_eq!(label, "DJ_USB");
                    assert_eq!(path, PathBuf::from("music/song.mp3"));
                }
                Location::File { .. } => panic!("expected Usb variant, got File"),
            }
        }
    }
}
