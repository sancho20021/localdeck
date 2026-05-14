use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::anyhow;
use chrono::{DateTime, Local};

use crate::{
    TrackId,
    config::{Config, Database, LibrarySource},
    db::{self, DBConfig, SecondsSinceUnix, i64_seconds_to_local_time, system_time_to_i64},
    error::StorageError,
    fs::{FileStorage, FileWithMeta, FsSnapshot, HashedFile, is_valid_music_path},
    location::{LOCATION_PATH_SEP, Location, replace_windows_slashes},
    schema::{columns, tables},
    track::{ArtworkRef, Track, TrackMetadata},
    usb::ResolveError,
};

use columns::*;
use rusqlite::{ErrorCode, Transaction, params};
use tables::*;

#[derive(Debug)]
pub struct DBSnapshot {
    pub updated_at: DateTime<Local>,
    pub files: Vec<HashedFile>,
}

/// Main structure that implements all storage logic
pub struct Storage {
    pub(crate) db: rusqlite::Connection,
    source: LibrarySource,
    fs: FileStorage,
}

#[derive(Debug)]
pub struct TrackListEntry {
    pub track_id: TrackId,
    pub available_files: Vec<Location>,
    pub unavailable_files: Vec<Location>,
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
        let mut fs = FileStorage::new();
        let db_config = match config.database {
            Database::InMemory => DBConfig::InMemory,
            Database::OnDisk { location } => DBConfig::OnDisk {
                location: fs.loc_resolver.resolve(&location).map_err(|e| {
                    StorageError::Internal(anyhow!("Failed to resolve DB location: {e}"))
                })?,
            },
        };

        let db: rusqlite::Connection = db::open(db_config)?;
        Ok(Self {
            db,
            source: config.library_source,
            fs: FileStorage::new(),
        })
    }

    #[cfg(test)]
    fn from_existing_conn(db: rusqlite::Connection, lib_config: LibrarySource) -> Self {
        Self {
            db,
            source: lib_config,
            fs: FileStorage::new(),
        }
    }

    pub fn scan_db(&mut self) -> Result<DBSnapshot, StorageError> {
        println!("Scanning music on database...");
        let tx = self.db.transaction()?;

        let (files, updated_at) = {
            let mut stmt = tx.prepare(&format!(
                "SELECT {TRACK_ID}, {USB_LABEL}, {PATH}, {FILE_SIZE} FROM {FILES}"
            ))?;
            let files = stmt
                .query_map([], |row| {
                    let track_id_hex: String = row.get(0)?;
                    let usb_label: String = row.get(1)?;
                    let path: String = row.get(2)?;
                    let file_size: i64 = row.get(3)?;

                    Ok((
                        TrackId::from_hex(&track_id_hex).map_err(StorageError::InvalidTrackId),
                        FileWithMeta {
                            loc: LocationRow { usb_label, path }.into(),
                            file_size,
                        },
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let updated_at: SecondsSinceUnix = tx.query_row(
                &format!("SELECT COALESCE(MAX({UPDATED_AT}), 0) FROM {UPDATES}"),
                [],
                |row| row.get(0),
            )?;
            (files, updated_at)
        };

        tx.commit()?;

        let files = files
            .into_iter()
            .map(|(track, file)| Ok(HashedFile::new(track?, file)))
            .collect::<Result<Vec<_>, StorageError>>()?;

        let updated_at = i64_seconds_to_local_time(updated_at).map_err(|e| {
            StorageError::Internal(e.context("database update times contains invalid time"))
        })?;

        Ok(DBSnapshot { updated_at, files })
    }

    pub fn scan_metadata(&mut self) -> Result<Vec<Track>, StorageError> {
        let tx = self.db.transaction()?; // rusqlite::Error propagates here

        let mut stmt = tx.prepare(
            "SELECT track_id, title, artist, year, label, artwork_url FROM track_metadata",
        )?;

        // query_map returns Result<Rows<Result<Track, StorageError>>, rusqlite::Error>
        let rows = stmt.query_map([], |row| {
            let track_id_hex: String = row.get(0)?;

            // explicitly handle TrackId conversion
            let track_id = match TrackId::from_hex(&track_id_hex) {
                Ok(id) => id,
                Err(e) => return Ok(Err(StorageError::InvalidTrackId(e))), // store error explicitly
            };

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

    /// Inserts new tracks entries into the database
    ///
    /// Files already present in database will raise error
    fn insert_tracks(
        &mut self,
        tracks_with_files: impl IntoIterator<Item = (TrackId, FileWithMeta)>,
    ) -> Result<(), StorageError> {
        let tx = self.db.transaction()?; // start a transaction
        for (track_id, file) in tracks_with_files {
            let track_id_str = track_id.to_string();

            // ---------- Insert track if it does not exist ----------
            tx.execute(
                "INSERT OR IGNORE INTO tracks (track_id) VALUES (?1)",
                params![&track_id_str],
            )?;

            let loc: LocationRow = LocationRow::from_location(file.loc.clone())?;

            // ---------- Insert file (must NOT already exist) ----------
            tx.execute(
                &format!(
                    "INSERT INTO {FILES} ({USB_LABEL}, {PATH}, {TRACK_ID}, {FILE_SIZE})
                            VALUES (?1, ?2, ?3, ?4)"
                ),
                params![&loc.usb_label, &loc.path, &track_id_str, file.file_size,],
            )
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(ref err, _)
                    if err.code == ErrorCode::ConstraintViolation =>
                {
                    StorageError::DuplicateLocation {
                        path: file.loc,
                        hint: "Attempted to insert a location that already exists. \
                           This usually means the file content changed without renaming. \
                           Consider forgetting the old entry and running update again."
                            .into(),
                    }
                }
                e => StorageError::Database(e),
            })?;
        }
        Self::insert_update_time(&tx)?;

        tx.commit()?; // commit everything at once
        Ok(())
    }

    /// Recursively scans all music files in the library source. Retrieves their paths and metadata
    fn scan_fs(&mut self) -> Result<FsSnapshot, StorageError> {
        println!("Scanning music on file system...");
        let fs = self.fs.scan(&self.source)?;
        Ok(fs)
    }

    /// checks for new music files not present in database
    pub fn check_new(&mut self) -> Result<HashSet<FileWithMeta>, StorageError> {
        let fs = self.scan_fs()?;
        let db: HashSet<FileWithMeta> = self
            .scan_db()?
            .files
            .into_iter()
            .map(|fm| fm.file)
            .collect();
        Ok(fs.difference(&db).cloned().collect())
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
                let track_id_hex: String = row.get(0)?;
                let has_metadata: bool = row.get(1)?;

                Ok((track_id_hex, has_metadata))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        tx.commit()?;

        let mut result = StaleTracks::default();

        for (track_id_hex, has_metadata) in stale_rows {
            let track_id = TrackId::from_hex(&track_id_hex).map_err(|e| {
                StorageError::InvalidTrackId(format!(
                    "Database contains invalid track id in stale track query: {e}"
                ))
            })?;

            if has_metadata {
                result.metadata_only.push(track_id);
            } else {
                result.dangling.push(track_id);
            }
        }

        Ok(result)
    }

    pub fn update_db_with_new_files(&mut self) -> Result<Vec<HashedFile>, StorageError> {
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
            let id = TrackId::from_file(&path)?;
            Ok((id, f))
        }).collect::<Result<Vec<_>, _>>()?;
        self.insert_tracks(with_hash.clone())?;
        Ok(with_hash
            .into_iter()
            .map(|(id, f)| HashedFile {
                track_id: id,
                file: f,
            })
            .collect())
    }

    /// checks for tracks without available files.
    ///
    /// ignores tracks that have at least one available file
    pub fn check_missing(
        &mut self,
    ) -> Result<HashMap<TrackId, HashSet<FileWithMeta>>, StorageError> {
        let fs = self.scan_fs()?;
        let db = self.scan_db()?.files;

        let mut track_db_locs: HashMap<TrackId, HashSet<FileWithMeta>> = Default::default();
        let mut seen_tracks: HashSet<TrackId> = Default::default();
        for hf in db {
            if fs.contains(&hf.file) {
                seen_tracks.insert(hf.track_id);
            }
            track_db_locs
                .entry(hf.track_id)
                .or_insert(Default::default())
                .insert(hf.file);
        }
        for track in seen_tracks {
            track_db_locs.remove(&track);
        }
        Ok(track_db_locs)
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
            return Err(StorageError::TrackNotFound(track_id));
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

    pub fn find_track_file_with_meta(
        &mut self,
        track: TrackId,
    ) -> Result<(PathBuf, Location, Option<TrackMetadata>), StorageError> {
        let (_, path, loc) = self.find_track_file(track)?;
        let meta = self.get_track_metadata(track)?;
        Ok((path, loc, meta))
    }

    /// searches for a file where path, track_id, artist or title matches the query
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

        let mut sql = format!(
            "
            SELECT f.{TRACK_ID}, f.{USB_LABEL}, f.{PATH}
            FROM {FILES} f
            LEFT JOIN {TRACK_METADATA} tm
                ON f.{TRACK_ID} = tm.{TRACK_ID}
            WHERE 1=1
            "
        );

        // Apply search filter
        if !cleaned_query.is_empty() {
            sql.push_str(&format!(
                "
        AND (
            LOWER(f.{PATH}) LIKE ?1 OR
            LOWER(f.{TRACK_ID}) LIKE ?1 OR
            LOWER(tm.{ARTIST}) LIKE ?1 OR
            LOWER(tm.{TITLE}) LIKE ?1
        )
        "
            ));
        }

        // Apply no_meta filter
        if no_meta {
            sql.push_str(" AND tm.track_id IS NULL ");
        }

        let mut stmt = tx.prepare(&sql)?;

        let results = if !cleaned_query.is_empty() {
            stmt.query_map([like_query], |row| {
                let track_id_hex: String = row.get(0)?;
                let usb_label: String = row.get(1)?;
                let path: String = row.get(2)?;

                match TrackId::from_hex(&track_id_hex) {
                    Ok(track_id) => {
                        let loc: Location = LocationRow { usb_label, path }.into();
                        Ok(Some((track_id, loc)))
                    }
                    Err(_) => {
                        log::warn!("Corrupted track_id '{}' in table {}", track_id_hex, FILES);
                        Ok(None)
                    }
                }
            })?
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map([], |row| {
                let track_id_hex: String = row.get(0)?;
                let usb_label: String = row.get(1)?;
                let path: String = row.get(2)?;

                match TrackId::from_hex(&track_id_hex) {
                    Ok(track_id) => {
                        let loc: Location = LocationRow { usb_label, path }.into();
                        Ok(Some((track_id, loc)))
                    }
                    Err(_) => {
                        log::warn!("Corrupted track_id '{}' in table {}", track_id_hex, FILES);
                        Ok(None)
                    }
                }
            })?
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, _>>()?
        };

        drop(stmt);
        tx.commit()?;

        // build map
        let mut map: HashMap<TrackId, HashSet<Location>> = HashMap::new();

        for (key, value) in results {
            map.entry(key).or_default().insert(value);
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

            stmt.query_map([], |row| row.get::<_, String>(0))?
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
                row.get::<_, String>(0)
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

        // ---------- Step 2: load current metadata ----------
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

        // ---------- Step 5: upsert ----------
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
                    StorageError::TrackNotFound(track_id)
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
        TrackId,
        config::LibrarySource,
        db::i64_seconds_to_local_time,
        error::StorageError,
        fs::FileWithMeta,
        location::Location,
        operations::{MetadataUpdate, Storage, replace_windows_slashes},
        schema::{self, *},
        usb::LocationResolver,
    };

    fn file_size(path: &Path) -> i64 {
        let meta = std::fs::metadata(path).unwrap();
        let size = meta.len() as i64;
        size
    }

    fn mock_trackid(x: i32) -> TrackId {
        let bytes = x.to_be_bytes();
        TrackId::from_bytes(&bytes)
    }

    fn mock_trackid_str(x: i32) -> String {
        mock_trackid(x).to_hex()
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

    fn insert_tracks(conn: &Connection, tracks: impl IntoIterator<Item = TrackId>) {
        for track in tracks {
            conn.execute(
                &format!("INSERT INTO {TRACKS} ({TRACK_ID}) VALUES (?1)"),
                params![track.to_string()],
            )
            .unwrap();
        }
    }

    fn insert_fake_files<S: AsRef<str>>(
        conn: &Connection,
        tracks: impl IntoIterator<Item = (TrackId, S, i64)>,
        usb_label: Option<String>,
    ) {
        for (track, path, fs) in tracks {
            insert_file(&conn, &track.to_string(), path.as_ref(), &usb_label, fs);
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
            insert_file(&conn, &track.to_string(), path.as_ref(), &usb_label, fs);
        }
    }

    #[test]
    fn test_scan_db() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;

        schema::init(&conn)?;

        insert_tracks(&mut conn, [mock_trackid(1)]);
        insert_fake_files(
            &mut conn,
            [(mock_trackid(1), "song.mp3", MOCKED_FILE_SIZE)],
            None,
        );

        conn.execute(
            &format!("INSERT INTO {UPDATES} ({UPDATED_AT}) VALUES (?1)"),
            params![200],
        )?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let snapshot = storage.scan_db()?;

        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].file.loc, Location::from_path("song.mp3"));
        assert_eq!(snapshot.updated_at, i64_seconds_to_local_time(200).unwrap());

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
        let track1 = TrackId::from_file(&path1)?;
        let track2 = TrackId::from_file(&path2)?;

        // --- run update ---
        let result = storage.update_db_with_new_files()?;

        // --- verify return value ---
        assert_eq!(result.len(), 2);

        let ids: Vec<_> = result.iter().map(|h| h.track_id.clone()).collect();
        assert!(ids.contains(&track1));
        assert!(ids.contains(&track2));

        // --- verify DB state ---
        let mut stmt = storage
            .db
            .prepare("SELECT track_id, path FROM files ORDER BY track_id")?;

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

        let mut track1 = mock_trackid(1);
        let mut track2 = mock_trackid(2);

        if track1.to_string() > track2.to_string() {
            std::mem::swap(&mut track1, &mut track2);
        }

        // insert
        storage.insert_tracks([(track1, file1.clone()), (track2, file2.clone())])?;

        // --- verify DB state directly ---
        let mut stmt = storage
            .db
            .prepare("SELECT track_id, path, file_size FROM files ORDER BY track_id")?;

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(rows.len(), 2);

        // track 1
        assert_eq!(rows[0].0, track1.to_string());
        assert_eq!(rows[0].1, "a.mp3");
        assert_eq!(rows[0].2, 100);

        // track 2
        assert_eq!(rows[1].0, track2.to_string());
        assert_eq!(rows[1].1, "b.mp3");
        assert_eq!(rows[1].2, 200);

        Ok(())
    }

    #[test]
    fn test_insert_tracks_duplicate_path() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());
        let file = FileWithMeta {
            loc: Location::from_path("a.mp3"),
            file_size: 39,
        };
        let err = storage
            .insert_tracks([(mock_trackid(1), file.clone()), (mock_trackid(2), file)])
            .unwrap_err();
        assert!(matches!(err, StorageError::DuplicateLocation { .. }));
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

        let track_id = mock_trackid(1);

        insert_tracks(&mut conn, [track_id]);
        insert_fake_files(
            &mut conn,
            [(
                track_id,
                &replace_windows_slashes(&file_path),
                MOCKED_FILE_SIZE,
            )],
            None,
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let (track, path, _) = storage.find_track_file(track_id)?;

        assert_eq!(track, track_id);
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

        let track_id = mock_trackid(1);

        // insert USB location into DB
        let usb_label = "DJ_USB";

        insert_tracks(&mut conn, [track_id]);
        insert_fake_files(
            &mut conn,
            [(track_id, "song.mp3", MOCKED_FILE_SIZE)],
            Some(usb_label.to_string()),
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // mock resolver
        storage.fs.loc_resolver =
            LocationResolver::test_resolver([(usb_label.to_string(), usb_mount.clone())]);

        let (track, path, loc) = storage.find_track_file(track_id)?;

        assert_eq!(track, track_id);
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

        let track_id = mock_trackid(3);

        insert_tracks(&mut conn, [track_id]);
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

        let track_id = mock_trackid(5);

        insert_tracks(&mut conn, [track_id]);
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

        let track_id = mock_trackid(42);

        let err = storage.find_track_file(track_id).unwrap_err();

        assert!(matches!(err, StorageError::TrackNotFound(..)));

        Ok(())
    }

    #[test]
    fn test_get_track_metadata() {
        // ---------- Setup in-memory DB ----------
        let temp_dir = tempdir().unwrap();
        let mut storage = setup_storage(temp_dir.path()).unwrap();
        // ---------- Insert test data ----------
        let track_id = mock_trackid(123);

        insert_tracks(&mut storage.db, [track_id]);

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

        // Insert some test rows
        let data = vec![
            (
                mock_trackid(1),
                "Some Artist - Track Name.mp3",
                MOCKED_FILE_SIZE,
            ),
            (
                mock_trackid(2),
                "AnotherArtist_Track Name.flac",
                MOCKED_FILE_SIZE,
            ),
            (
                mock_trackid(3),
                "completely-different-track.mp3",
                MOCKED_FILE_SIZE,
            ),
        ];

        insert_tracks(
            &mut conn,
            [mock_trackid(1), mock_trackid(2), mock_trackid(3)],
        );
        insert_fake_files(&mut conn, data, None);

        let mut storage = Storage::from_existing_conn(conn, LibrarySource::default());

        // Search for a liberal match
        let results = storage.find_files("track name", false).unwrap();
        assert_files(
            &results,
            [
                (mock_trackid(1), vec!["Some Artist - Track Name.mp3"]),
                (mock_trackid(2), vec!["AnotherArtist_Track Name.flac"]),
            ],
        );

        // Search with different casing and spaces
        let results2 = storage.find_files("another", false).unwrap();
        assert_files(
            &results2,
            [(mock_trackid(2), vec!["AnotherArtist_Track Name.flac"])],
        );

        // Search for trackid
        let results3 = storage.find_files(&mock_trackid_str(3), false).unwrap();
        assert_files(
            &results3,
            [(mock_trackid(3), vec!["completely-different-track.mp3"])],
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
        let tracks = [mock_trackid(1), mock_trackid(2), mock_trackid(3)];
        insert_tracks(&mut conn, tracks);

        // --- Insert files ---
        insert_fake_files(
            &mut conn,
            vec![
                (mock_trackid(1), "foo.mp3", MOCKED_FILE_SIZE),
                (mock_trackid(2), "bar.mp3", MOCKED_FILE_SIZE),
                (mock_trackid(3), "baz.mp3", MOCKED_FILE_SIZE),
            ],
            None,
        );

        // --- Insert metadata manually (ONLY for 1 and 2) ---
        conn.execute(
            "INSERT INTO track_metadata (track_id, title, artist, year, label, artwork_url)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL)",
            rusqlite::params![mock_trackid_str(1), "Cool Track", "DJ Alpha"],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO track_metadata (track_id, title, artist, year, label, artwork_url)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL)",
            rusqlite::params![mock_trackid_str(2), "Another Banger", "Beta Artist"],
        )
        .unwrap();

        let mut storage = Storage::from_existing_conn(conn, LibrarySource::default());

        // --- Search by artist ---
        let results = storage.find_files("alpha", false).unwrap();
        assert_files(&results, [(mock_trackid(1), vec!["foo.mp3"])]);

        // --- Search by title ---
        let results = storage.find_files("banger", false).unwrap();
        assert_files(&results, [(mock_trackid(2), vec!["bar.mp3"])]);

        // --- no_meta: should return ONLY track 3 ---
        let results = storage.find_files("", true).unwrap();
        assert_files(&results, [(mock_trackid(3), vec!["baz.mp3"])]);

        // --- combined: query + no_meta (should be empty here) ---
        let results = storage.find_files("cool", true).unwrap();
        assert!(results.is_empty());

        // metadata exists but doesn't match query
        let results = storage.find_files("gamma", false).unwrap();
        assert!(results.is_empty());
    }

    static MOCKED_FILE_SIZE: i64 = 228;

    fn insert_file(
        conn: &Connection,
        track_id: &str,
        path: &str,
        usb_label: &Option<String>,
        file_size: i64,
    ) {
        conn.execute(
            &format!(
                "INSERT INTO {FILES} (track_id, usb_label, path, file_size) VALUES (?1, ?2, ?3, ?4)"
            ),
            params![
                track_id,
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

        let tracks = [mock_trackid(1), mock_trackid(2), mock_trackid(3)];
        let track_files = [
            (mock_trackid(1), "/music/track_a1.mp3", MOCKED_FILE_SIZE),
            (
                mock_trackid(1),
                "/music/subdir/track_a2.mp3",
                MOCKED_FILE_SIZE,
            ),
            (mock_trackid(2), "/music/track_b.mp3", MOCKED_FILE_SIZE),
            (mock_trackid(3), "/hello/track_c.mp3", MOCKED_FILE_SIZE), // outside deleted path
            (mock_trackid(1), "/hello/track_a3.mp3", MOCKED_FILE_SIZE), // outside deleted path
        ];

        insert_tracks(&storage.db, tracks);
        insert_fake_files(&storage.db, track_files, None);

        // Forget top-level directory
        let path_to_forget = Path::new("/music");
        let report = storage.forget_path(path_to_forget).unwrap();

        assert_eq!(report.removed_files, 3); // a1 + a2 + b
        assert_eq!(report.affected_tracks, 2); // a + b
        assert_eq!(report.removed_tracks, 1); // b

        // Remaining DB entries
        let remaining: Vec<String> = storage
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

        let tracks = [mock_trackid(1)];
        let track_files = [
            (mock_trackid(1), "C:/music/track_a1.mp3", MOCKED_FILE_SIZE),
            (
                mock_trackid(1),
                "C:/music/subdir/track_a2.mp3",
                MOCKED_FILE_SIZE,
            ),
        ];

        insert_tracks(&storage.db, tracks);
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
            mock_trackid(1)
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

        let result = storage.update_track_metadata(mock_trackid(42), update, false);

        assert!(matches!(
            result,
            Err(StorageError::TrackNotFound(id)) if id == mock_trackid(42)
        ));

        Ok(())
    }

    #[test]
    fn test_update_track_metadata_insert_new_metadata() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        insert_tracks(&mut conn, [mock_trackid(1)]);

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let update = MetadataUpdate {
            title: Some("Song A".into()),
            artist: Some("Artist A".into()),
            year: Some(1999),
            label: None,
            artwork: None,
        };

        storage.update_track_metadata(mock_trackid(1), update, false)?;

        // Verify
        let meta = storage.get_track_metadata(mock_trackid(1))?;
        let meta = meta.unwrap();
        assert_eq!(meta.title, "Song A");
        assert_eq!(meta.artist, "Artist A");

        Ok(())
    }

    #[test]
    fn test_update_track_metadata_reject_overwrite() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        insert_tracks(&mut conn, [mock_trackid(1)]);

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // First insert
        storage.update_track_metadata(
            mock_trackid(1),
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
            mock_trackid(1),
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

        insert_tracks(&mut conn, [mock_trackid(1)]);

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        storage.update_track_metadata(
            mock_trackid(1),
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
            mock_trackid(1),
            MetadataUpdate {
                title: Some("Updated".into()),
                artist: None,
                year: None,
                label: None,
                artwork: None,
            },
            true,
        )?;

        let meta = storage.get_track_metadata(mock_trackid(1))?;
        assert_eq!(meta.unwrap().title, "Updated");

        Ok(())
    }

    mod check_tests {
        use tempfile::tempdir;

        use crate::{
            TrackId,
            location::{Location, replace_windows_slashes},
            operations::tests::{
                MOCKED_FILE_SIZE, insert_fake_files, insert_real_files, insert_tracks,
                mock_trackid, setup_storage,
            },
        };

        #[test]
        fn test_check_new_no_new_files() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            std::fs::write(&path, b"x")?;

            let track_id = TrackId::from_file(&path)?;

            insert_tracks(&mut storage.db, [track_id]);
            insert_real_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&path))],
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

            let track_id = TrackId::from_file(&path1)?;

            // DB only knows about first path
            insert_tracks(&mut storage.db, [track_id]);
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
            let track_id = mock_trackid(123); // file not created

            insert_tracks(&mut storage.db, [track_id]);
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

            let track_id = TrackId::from_file(&path)?;

            insert_tracks(&mut storage.db, [track_id]);
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

            let track_id = mock_trackid(123);

            insert_tracks(&mut storage.db, [track_id]);
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
        fn test_check_missing_ignores_partially_available_track() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let available = dir.path().join("song1.mp3");
            let missing = dir.path().join("song2.mp3");

            std::fs::write(&available, b"x")?;

            let track_id = TrackId::from_file(&available)?;

            insert_tracks(&mut storage.db, [track_id]);
            insert_real_files(
                &mut storage.db,
                [(track_id, replace_windows_slashes(&available))],
                None,
            );
            insert_fake_files(
                &mut storage.db,
                [(
                    track_id,
                    replace_windows_slashes(&missing),
                    MOCKED_FILE_SIZE,
                )],
                None,
            );

            let diff = storage.check_missing()?;

            assert!(diff.is_empty());

            Ok(())
        }

        #[test]
        fn test_check_missing_detects_renamed_track() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            let renamed = dir.path().join("renamed.mp3");

            std::fs::write(&renamed, b"x")?;

            let track_id = TrackId::from_file(&renamed)?;

            insert_tracks(&mut storage.db, [track_id]);
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

            let track_id = TrackId::from_file(&path)?;

            insert_tracks(&mut storage.db, [track_id]);

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

            let track_id = mock_trackid(123);

            insert_tracks(&mut storage.db, [track_id]);

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

            let track_id = mock_trackid(555);

            insert_tracks(&mut storage.db, [track_id]);

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

            let good_track = TrackId::from_file(&existing)?;
            let dangling_track = mock_trackid(999);
            let metadata_only_track = mock_trackid(555);

            insert_tracks(
                &mut storage.db,
                [good_track, dangling_track, metadata_only_track],
            );

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

        let good_track = TrackId::from_file(&existing)?;
        let dangling_track = mock_trackid(999);
        let metadata_only_track = mock_trackid(555);
        let file_only_track = mock_trackid(777);

        // Insert all tracks
        insert_tracks(
            &mut storage.db,
            [
                good_track,
                dangling_track,
                metadata_only_track,
                file_only_track,
            ],
        );

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
            [&metadata_only_track.to_string(), "Test Song", "Test Artist"],
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
