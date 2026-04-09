use std::{
    collections::{HashMap, HashSet},
    path::{MAIN_SEPARATOR_STR, Path, PathBuf},
    time::SystemTime,
};

use crate::{
    config::{self, LibrarySource},
    domain::{
        hash::TrackId,
        track::{ArtworkRef, Track, TrackMetadata},
    },
    storage::{
        self,
        db::{self, SecondsSinceUnix, system_time_to_i64},
        error::StorageError,
        fs::{FsSnapshot, ObservedFile},
        schema::{columns, tables},
    },
};

use anyhow::anyhow;
use columns::*;
use rusqlite::{ErrorCode, Transaction, params};
use tables::*;

#[derive(Debug)]
pub struct TrackChange {
    pub db_locations: HashSet<PathBuf>,
    pub fs_locations: HashSet<PathBuf>,
}

impl TrackChange {
    pub fn new_locations(&self) -> HashSet<PathBuf> {
        &self.fs_locations - &self.db_locations
    }

    pub fn deleted_locations(&self) -> HashSet<PathBuf> {
        &self.db_locations - &self.fs_locations
    }

    pub fn is_new(&self) -> bool {
        self.db_locations.is_empty()
    }

    pub fn is_deleted(&self) -> bool {
        self.fs_locations.is_empty()
    }
}

pub type Diff = HashMap<TrackId, TrackChange>;

#[derive(Debug)]
pub struct DBSnapshot {
    pub updated_at: SecondsSinceUnix,
    pub files: Vec<ObservedFile>,
}

/// Main structure that implements all storage logic
pub struct Storage {
    pub(crate) db: rusqlite::Connection,
    source: LibrarySource,
}

#[derive(Debug)]
pub struct TrackListEntry {
    pub track_id: TrackId,
    pub available_files: Vec<PathBuf>,
    pub unavailable_files: Vec<PathBuf>,
}

pub const DB_PATH_SEPARATOR: &str = "/";

#[derive(Debug)]
pub struct ForgetReport {
    /// files removed
    pub removed_files: usize,
    /// tracks where some of its location removed
    pub affected_tracks: usize,
    /// tracks which no longer exist
    pub removed_tracks: usize,
}

impl Storage {
    /// when called, opens a data base connection
    /// and applies migrations
    pub fn new(
        db_config: config::Database,
        lib_config: LibrarySource,
    ) -> Result<Self, StorageError> {
        let db: rusqlite::Connection = db::open(&db_config)?;
        Ok(Self::from_existing_conn(db, lib_config))
    }

    fn from_existing_conn(db: rusqlite::Connection, lib_config: LibrarySource) -> Self {
        Self {
            db,
            source: lib_config,
        }
    }

    pub fn scan_db(&mut self) -> Result<DBSnapshot, StorageError> {
        let tx = self.db.transaction()?;

        let (files, updated_at) = {
            let mut stmt = tx.prepare(&format!("SELECT {TRACK_ID}, {PATH} FROM {FILES}"))?;
            let files = stmt
                .query_map([], |row| {
                    let track_id_hex: String = row.get(0)?;
                    let path: String = row.get(1)?;

                    Ok((
                        TrackId::from_hex(&track_id_hex).map_err(|e| StorageError::Internal(e)),
                        path.into(),
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
            .map(|(track, path)| Ok(ObservedFile::new(track?, path)))
            .collect::<Result<Vec<_>, StorageError>>()?;

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
                Err(e) => return Ok(Err(StorageError::Internal(e))), // store error explicitly
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
    /// Existing tracks or files are not duplicated.
    ///
    /// Returns the number of files actually inserted.
    pub fn insert_tracks(
        &mut self,
        tracks_with_files: impl IntoIterator<Item = (TrackId, PathBuf)>,
    ) -> Result<usize, StorageError> {
        let tx = self.db.transaction()?; // start a transaction
        let mut files_inserted = 0;

        for (track_id, path) in tracks_with_files {
            let track_id_str = track_id.to_string();

            // ---------- Insert track if it does not exist ----------
            tx.execute(
                "INSERT OR IGNORE INTO tracks (track_id) VALUES (?1)",
                params![&track_id_str],
            )?;

            let path_str = path_to_string(&path);

            // ---------- Insert file (must NOT already exist) ----------
            let inserted = tx
                .execute(
                    "INSERT INTO files (path, track_id) VALUES (?1, ?2)",
                    params![&path_str, &track_id_str],
                )
                .map_err(|e| match e {
                    rusqlite::Error::SqliteFailure(ref err, _)
                        if err.code == ErrorCode::ConstraintViolation =>
                    {
                        StorageError::DuplicatePath {
                            path: path.clone(),
                            hint: "Attempted to insert a path that already exists. \
                           This usually means the file content changed without renaming. \
                           Consider forgetting the old entry and running update again."
                                .into(),
                        }
                    }
                    e => StorageError::Database(e),
                })?;

            // rusqlite returns 1 if row inserted, 0 if ignored
            files_inserted += inserted as usize;
        }
        Self::insert_update_time(&tx)?;

        tx.commit()?; // commit everything at once
        Ok(files_inserted)
    }

    /// Updates the database by adding new files from the diff.
    fn _update_db_with_new_files(
        &mut self,
        diff_result: &Diff,
    ) -> Result<Vec<ObservedFile>, StorageError> {
        let new_files = diff_result
            .iter()
            .flat_map(|(id, changes)| {
                changes
                    .new_locations()
                    .into_iter()
                    .map(|path| ObservedFile::new(id.clone(), path))
            })
            .collect::<Vec<_>>();

        let inserted =
            self.insert_tracks(new_files.iter().map(|of| (of.track_id, of.path.clone())))?;
        if inserted != new_files.len() {
            log::error!(
                "number of inserted tracks not equal to expected diff, inserted = {inserted}, expected = {}",
                new_files.len()
            );
        }
        Ok(new_files)
    }

    pub fn update_db_with_new_files(&mut self) -> Result<Vec<ObservedFile>, StorageError> {
        let (_, _, diff_result) = self.status()?;
        self._update_db_with_new_files(&diff_result)
    }

    /// checks for new music files not present in database
    pub fn check_new(&mut self) -> Result<HashMap<TrackId, HashSet<PathBuf>>, StorageError> {
        println!("Scanning music on file system...");
        let fs = FsSnapshot::scan(&self.source)?;
        println!("Scanning music on database...");
        let db = self.scan_db()?;
        let diff = Self::diff(&fs, &db);
        Ok(diff
            .into_iter()
            .map(|(track, change)| (track, change.new_locations()))
            .filter(|(_, locs)| !locs.is_empty())
            .collect())
    }

    /// checks for tracks without available files.
    ///
    /// ignores tracks that have at least one available file
    pub fn check_missing(&mut self) -> Result<HashMap<TrackId, HashSet<PathBuf>>, StorageError> {
        let tracks = self.list_tracks()?;
        Ok(tracks
            .into_iter()
            .filter_map(|t| {
                if t.available_files.is_empty() {
                    Some((t.track_id, t.unavailable_files.into_iter().collect()))
                } else {
                    None
                }
            })
            .collect())
    }

    /// aka git status
    ///
    /// reads files in the file system,
    /// reads file records in the database,
    /// returns both, and difference between the database and the file system
    fn status(&mut self) -> Result<(FsSnapshot, DBSnapshot, Diff), StorageError> {
        println!("Scanning music on file system...");
        let fs = FsSnapshot::scan(&self.source)?;
        println!("Scanning music on database...");
        let db = self.scan_db()?;
        let diff = Self::diff(&fs, &db);
        Ok((fs, db, diff))
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

    /// retrieves location of the track, checking that it is present in the file system
    ///
    /// If multiple locations point to the same track, chooses one of them.
    pub fn find_track_file(
        &mut self,
        track_id: TrackId,
    ) -> Result<(TrackId, PathBuf), StorageError> {
        let paths = (|| {
            let mut stmt = self
                .db
                .prepare("SELECT path FROM files WHERE track_id = ?1")?;

            Ok(stmt
                .query_map(params![track_id.to_string()], |row| {
                    Ok(PathBuf::from(row.get::<_, String>(0)?))
                })?
                .collect::<Result<Vec<_>, _>>()?)
        })()
        .map_err(StorageError::Database)?;

        if paths.is_empty() {
            return Err(StorageError::TrackNotFound(track_id));
        }

        if let Some(path) = paths
            .into_iter()
            .filter(|p| storage::fs::is_valid_music_path(p))
            .next()
        {
            Ok((track_id, path))
        } else {
            Err(StorageError::InvalidTrackFile { track: track_id })
        }
    }

    pub fn find_track_file_with_meta(
        &mut self,
        track: TrackId,
    ) -> Result<(PathBuf, Option<TrackMetadata>), StorageError> {
        let (_, path) = self.find_track_file(track)?;
        let meta = self.get_track_metadata(track)?;
        Ok((path, meta))
    }

    fn diff(fs: &FsSnapshot, ds: &DBSnapshot) -> Diff {
        let fs_files = fs.files.clone().into_iter().collect::<HashSet<_>>();
        let db_files = ds.files.clone().into_iter().collect::<HashSet<_>>();

        let mut locs = Diff::new();

        for file in fs_files {
            if let Some(locs) = locs.get_mut(&file.track_id) {
                locs.fs_locations.insert(file.path);
            } else {
                locs.insert(
                    file.track_id,
                    TrackChange {
                        fs_locations: HashSet::from([file.path]),
                        db_locations: HashSet::new(),
                    },
                );
            }
        }

        for file in db_files {
            if let Some(locs) = locs.get_mut(&file.track_id) {
                locs.db_locations.insert(file.path);
            } else {
                locs.insert(
                    file.track_id,
                    TrackChange {
                        db_locations: HashSet::from([file.path]),
                        fs_locations: HashSet::new(),
                    },
                );
            }
        }

        let unchanged = locs
            .iter()
            .filter_map(|(track, change)| {
                if change.db_locations == change.fs_locations {
                    Some(track.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for track in unchanged {
            locs.remove(&track);
        }

        locs
    }

    /// List tracks with available and unavailable files
    pub fn list_tracks(&mut self) -> Result<Vec<TrackListEntry>, StorageError> {
        let (fs_snapshot, db_snapshot, _) = self.status()?;

        // Map track_id -> available paths on disk
        let mut available_map: HashMap<TrackId, Vec<PathBuf>> = HashMap::new();
        for file in fs_snapshot.files {
            available_map
                .entry(file.track_id)
                .or_default()
                .push(file.path);
        }

        // Map track_id -> all paths recorded in DB
        let mut db_map: HashMap<TrackId, Vec<PathBuf>> = HashMap::new();
        for file in db_snapshot.files {
            db_map.entry(file.track_id).or_default().push(file.path);
        }

        let mut result = Vec::new();

        for (track_id, db_paths) in db_map.into_iter() {
            let available_paths = available_map.get(&track_id).cloned().unwrap_or_default();

            // Compute unavailable paths: DB paths that are not currently available
            let unavailable_paths = db_paths
                .into_iter()
                .filter(|p| !available_paths.contains(p))
                .collect::<Vec<_>>();

            result.push(TrackListEntry {
                track_id,
                available_files: available_paths,
                unavailable_files: unavailable_paths,
            });
        }

        Ok(result)
    }

    /// searches for a file where path, track_id, artist or title matches the query
    ///
    /// conditionally selects only tracks without meta data
    pub fn find_files(
        &mut self,
        query: &str,
        no_meta: bool,
    ) -> Result<HashMap<TrackId, HashSet<String>>, StorageError> {
        let tx = self.db.transaction()?;

        let cleaned_query = query.trim().to_lowercase();
        let like_query = format!("%{}%", cleaned_query);

        let mut sql = format!(
            "
        SELECT f.{TRACK_ID}, f.{PATH}
        FROM {FILES} f
        LEFT JOIN {TRACK_METADATA} tm
            ON f.{TRACK_ID} = tm.{TRACK_ID}
        WHERE 1=1
        "
        );

        // Apply search filter (if query not empty)
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
                let path: String = row.get(1)?;

                match TrackId::from_hex(&track_id_hex) {
                    Ok(track_id) => Ok(Some((track_id, path))),
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
                let path: String = row.get(1)?;

                match TrackId::from_hex(&track_id_hex) {
                    Ok(track_id) => Ok(Some((track_id, path))),
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

        let mut map: HashMap<_, HashSet<_>> = HashMap::new();

        for (key, value) in results {
            map.entry(key).or_default().insert(value);
        }

        Ok(map)
    }

    /// removes all files inside specified directory from the database
    /// useful when some files got moved or deleted
    pub fn forget_path(&mut self, path: &Path) -> Result<ForgetReport, StorageError> {
        let tx = self.db.transaction()?;

        let prefix = path_to_string(path);

        let dir_prefix = format!("{}{}%", prefix, DB_PATH_SEPARATOR);
        // --------------------------------------------------
        // Collect affected track ids BEFORE deletion
        // --------------------------------------------------

        let mut stmt = tx.prepare(&format!(
            "SELECT DISTINCT {TRACK_ID} FROM {FILES}
         WHERE {PATH} = ?1 OR {PATH} LIKE ?2"
        ))?;

        let affected_track_ids = stmt
            .query_map(params![prefix, dir_prefix], |row| row.get::<_, String>(0))?
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
            params![prefix, dir_prefix],
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

/// converts a path to string with forward slashes.
///
/// Must be used on both windows and linux to keep path representation consistent
/// within the database
pub fn path_to_string(p: &Path) -> String {
    p.to_string_lossy().replace('\\', DB_PATH_SEPARATOR)
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
        fs::{self, File},
        path::{Path, PathBuf},
        sync::Mutex,
        time::SystemTime,
    };

    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use crate::{
        config::{LibrarySource, Location},
        domain::hash::TrackId,
        storage::{
            self,
            error::StorageError,
            fs::{FsSnapshot, ObservedFile},
            operations::{DBSnapshot, Diff, MetadataUpdate, Storage, TrackChange, path_to_string},
            schema::{self, *},
        },
    };

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

    fn insert_track_files<S: AsRef<str>>(
        conn: &Connection,
        tracks: impl IntoIterator<Item = (TrackId, S)>,
    ) {
        for (track, path) in tracks {
            insert_file(&conn, &track.to_string(), path.as_ref());
        }
    }

    #[test]
    fn test_scan_db() -> anyhow::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;

        schema::init(&conn)?;

        insert_tracks(&mut conn, [mock_trackid(1)]);
        insert_track_files(&mut conn, [(mock_trackid(1), "song.mp3")]);

        conn.execute(
            &format!("INSERT INTO {UPDATES} ({UPDATED_AT}) VALUES (?1)"),
            params![200],
        )?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let snapshot = storage.scan_db()?;

        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].path, PathBuf::from("song.mp3"));
        assert_eq!(snapshot.updated_at, 200);

        Ok(())
    }

    #[test]
    fn test_diff_simple() -> anyhow::Result<()> {
        let fs_snapshot = FsSnapshot {
            files: vec![
                ObservedFile::new(mock_trackid(1), "a.mp3".into()), // same as DB
                ObservedFile::new(mock_trackid(3), "c.mp3".into()), // new
            ],
            observed_at: SystemTime::now(),
        };

        let db_snapshot = DBSnapshot {
            updated_at: 100,
            files: vec![
                ObservedFile::new(mock_trackid(1), "a.mp3".into()), // same as FS
                ObservedFile::new(mock_trackid(2), "b.mp3".into()), // deleted in FS
            ],
        };

        let diff_result = Storage::diff(&fs_snapshot, &db_snapshot);

        // Expect: track 2 deleted, track 3 new
        assert_eq!(diff_result.len(), 2);

        assert_eq!(
            diff_result
                .get(&mock_trackid(2))
                .unwrap()
                .deleted_locations(),
            HashSet::from([PathBuf::from("b.mp3")])
        );

        assert_eq!(
            diff_result.get(&mock_trackid(3)).unwrap().new_locations(),
            HashSet::from([PathBuf::from("c.mp3")])
        );

        Ok(())
    }

    #[test]
    fn test_diff_complex() -> anyhow::Result<()> {
        // DB snapshot (database state)
        let db_snapshot = DBSnapshot {
            updated_at: 100,
            files: vec![
                ObservedFile::new(mock_trackid(1), "a.mp3".into()), // will be moved
                ObservedFile::new(mock_trackid(2), "b1.mp3".into()), // will stay
                ObservedFile::new(mock_trackid(2), "b2.mp3".into()), // will be removed
                ObservedFile::new(mock_trackid(3), "c1.mp3".into()), // will be copied
            ],
        };

        // FS snapshot (filesystem reality)
        let fs_snapshot = FsSnapshot {
            files: vec![
                ObservedFile::new(mock_trackid(1), "a_new.mp3".into()), // moved
                ObservedFile::new(mock_trackid(2), "b1.mp3".into()),    // stayed
                ObservedFile::new(mock_trackid(3), "c1.mp3".into()),    // stayed
                ObservedFile::new(mock_trackid(3), "c2.mp3".into()),    // copied
                ObservedFile::new(mock_trackid(4), "d.mp3".into()),     // brand new
            ],
            observed_at: SystemTime::now(),
        };

        let diff_result = Storage::diff(&fs_snapshot, &db_snapshot);

        // Track 1 moved → old path deleted, new path new
        assert_eq!(
            diff_result.get(&mock_trackid(1)).unwrap().new_locations(),
            HashSet::from([PathBuf::from("a_new.mp3")])
        );
        assert_eq!(
            diff_result
                .get(&mock_trackid(1))
                .unwrap()
                .deleted_locations(),
            HashSet::from([PathBuf::from("a.mp3")])
        );

        // Track 2 → b2.mp3 deleted
        assert_eq!(
            diff_result
                .get(&mock_trackid(2))
                .unwrap()
                .deleted_locations(),
            HashSet::from([PathBuf::from("b2.mp3")])
        );

        // Track 3 → copy c2.mp3 added
        assert_eq!(
            diff_result.get(&mock_trackid(3)).unwrap().new_locations(),
            HashSet::from([PathBuf::from("c2.mp3")])
        );

        // Track 4 → new file
        assert_eq!(
            diff_result.get(&mock_trackid(4)).unwrap().new_locations(),
            HashSet::from([PathBuf::from("d.mp3")])
        );

        Ok(())
    }

    #[test]
    fn test_update_db_with_new_files_using_scan_db() -> anyhow::Result<()> {
        // Setup in-memory DB and schema
        let mut conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        insert_tracks(&mut conn, [mock_trackid(1), mock_trackid(2)]);
        insert_track_files(
            &mut conn,
            [(mock_trackid(1), "a.mp3"), (mock_trackid(2), "b1.mp3")],
        );

        // Prepare a diff
        let mut diff_result: Diff = HashMap::new();

        // Track 1 moved → new path "a_new.mp3"
        diff_result.insert(
            mock_trackid(1),
            TrackChange {
                fs_locations: HashSet::from([PathBuf::from("a_new.mp3"), PathBuf::from("a.mp3")]),
                db_locations: HashSet::from([PathBuf::from("a.mp3")]),
            },
        );

        // Track 2 new copy "b2.mp3"
        diff_result.insert(
            mock_trackid(2),
            TrackChange {
                fs_locations: HashSet::from([PathBuf::from("b1.mp3"), PathBuf::from("b2.mp3")]),
                db_locations: HashSet::from([PathBuf::from("b1.mp3")]),
            },
        );

        // Track 3 completely new
        diff_result.insert(
            mock_trackid(3),
            TrackChange {
                db_locations: HashSet::new(),
                fs_locations: HashSet::from([PathBuf::from("c.mp3")]),
            },
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        // Call the function
        storage._update_db_with_new_files(&diff_result)?;

        // Reuse scan_db to read DB snapshot
        let db_snapshot = storage.scan_db()?;

        // Track 1 has both old and new path
        let t1_files: Vec<_> = db_snapshot
            .files
            .iter()
            .filter(|f| f.track_id == mock_trackid(1))
            .map(|f| f.path.clone())
            .collect();
        assert!(t1_files.contains(&PathBuf::from("a.mp3")));
        assert!(t1_files.contains(&PathBuf::from("a_new.mp3")));

        // Track 2 has both copies
        let t2_files: Vec<_> = db_snapshot
            .files
            .iter()
            .filter(|f| f.track_id == mock_trackid(2))
            .map(|f| f.path.clone())
            .collect();
        assert!(t2_files.contains(&PathBuf::from("b1.mp3")));
        assert!(t2_files.contains(&PathBuf::from("b2.mp3")));

        // Track 3 has the new file
        let t3_files: Vec<_> = db_snapshot
            .files
            .iter()
            .filter(|f| f.track_id == mock_trackid(3))
            .map(|f| f.path.clone())
            .collect();
        assert_eq!(t3_files, vec![PathBuf::from("c.mp3")]);

        Ok(())
    }

    #[test]
    fn test_insert_tracks_duplicate_path() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());
        let err = storage
            .insert_tracks([
                (mock_trackid(1), PathBuf::from("a.mp3")),
                (mock_trackid(2), PathBuf::from("a.mp3")),
            ])
            .unwrap_err();
        assert!(matches!(err, StorageError::DuplicatePath { .. }));
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
        insert_track_files(&mut conn, [(track_id, &path_to_string(&file_path))]);

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let (track, path) = storage.find_track_file(track_id)?;

        assert_eq!(track, track_id);
        assert_eq!(path, file_path);

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
        insert_track_files(&mut conn, [(track_id, path_to_string(&bad_path))]);

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
        insert_track_files(
            &mut conn,
            [
                (track_id, path_to_string(&bad)),
                (track_id, path_to_string(&good)),
            ],
        );

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let (_, path) = storage.find_track_file(track_id)?;

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

    #[test]
    fn test_list_tracks_fully_available() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let mut storage = setup_storage(dir.path())?;
        let path = dir.path().join("song.mp3");
        fs::write(&path, b"x")?;

        let track_id = TrackId::from_file(&path)?;

        insert_tracks(&mut storage.db, [track_id]);
        insert_track_files(&mut storage.db, [(track_id, path_to_string(&path))]);

        let tracks = storage.list_tracks()?;
        assert_eq!(tracks.len(), 1);

        let entry = &tracks[0];
        assert_eq!(entry.track_id, track_id);
        assert_eq!(entry.available_files, vec![path.clone()]);
        assert!(entry.unavailable_files.is_empty());

        Ok(())
    }

    #[test]
    fn test_list_tracks_partially_available() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let mut storage = setup_storage(dir.path())?;

        let available_path = dir.path().join("song1.mp3");
        fs::write(&available_path, b"x")?;
        let unavailable_path = dir.path().join("song2.mp3"); // not created

        let track_id = TrackId::from_file(&available_path)?;

        // Insert both available and unavailable paths into DB
        insert_tracks(&mut storage.db, [track_id]);
        insert_track_files(
            &mut storage.db,
            [
                (track_id, path_to_string(&available_path)),
                (track_id, path_to_string(&unavailable_path)),
            ],
        );

        let tracks = storage.list_tracks()?;
        assert_eq!(tracks.len(), 1);

        let entry = &tracks[0];
        assert_eq!(entry.track_id, track_id);
        assert_eq!(entry.available_files, vec![available_path.clone()]);
        assert_eq!(entry.unavailable_files, vec![unavailable_path.clone()]);

        Ok(())
    }

    #[test]
    fn test_list_tracks_fully_unavailable() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let mut storage = setup_storage(dir.path())?;

        let path = dir.path().join("song.mp3"); // not created on disk

        let track_id = TrackId::from_bytes(&[0, 1, 2]);

        insert_tracks(&mut storage.db, [track_id]);
        insert_track_files(&mut storage.db, [(track_id, path_to_string(&path))]);

        let tracks = storage.list_tracks()?;
        assert_eq!(tracks.len(), 1);

        let entry = &tracks[0];
        assert_eq!(entry.track_id, track_id);
        assert!(entry.available_files.is_empty());
        assert_eq!(entry.unavailable_files, vec![path.clone()]);

        Ok(())
    }

    fn assert_files<I>(results: &HashMap<TrackId, HashSet<String>>, expected: I)
    where
        I: IntoIterator<Item = (TrackId, Vec<&'static str>)>,
    {
        for (id, files) in expected {
            let expected_set: HashSet<String> = files.into_iter().map(|s| s.to_string()).collect();
            let actual_set = &results[&id];
            assert_eq!(
                actual_set, &expected_set,
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
            (mock_trackid(1), "Some Artist - Track Name.mp3"),
            (mock_trackid(2), "AnotherArtist_Track Name.flac"),
            (mock_trackid(3), "completely-different-track.mp3"),
        ];

        insert_tracks(
            &mut conn,
            [mock_trackid(1), mock_trackid(2), mock_trackid(3)],
        );
        insert_track_files(&mut conn, data);

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
        insert_track_files(
            &mut conn,
            vec![
                (mock_trackid(1), "foo.mp3"),
                (mock_trackid(2), "bar.mp3"),
                (mock_trackid(3), "baz.mp3"),
            ],
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

    fn insert_file(conn: &Connection, track_id: &str, path: &str) {
        conn.execute(
            &format!("INSERT INTO {FILES} (track_id, path) VALUES (?1, ?2)"),
            params![track_id, path],
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
            (mock_trackid(1), "/music/track_a1.mp3"),
            (mock_trackid(1), "/music/subdir/track_a2.mp3"),
            (mock_trackid(2), "/music/track_b.mp3"),
            (mock_trackid(3), "/hello/track_c.mp3"), // outside deleted path
            (mock_trackid(1), "/hello/track_a3.mp3"), // outside deleted path
        ];

        insert_tracks(&storage.db, tracks);
        insert_track_files(&storage.db, track_files);

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
            (mock_trackid(1), "C:/music/track_a1.mp3"),
            (mock_trackid(1), "C:/music/subdir/track_a2.mp3"),
        ];

        insert_tracks(&storage.db, tracks);
        insert_track_files(&storage.db, track_files);

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
            domain::track::{ArtworkRef, TrackMetadata},
            storage::operations::MetadataUpdate,
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
        use std::collections::HashSet;

        use tempfile::tempdir;

        use crate::{
            domain::hash::TrackId,
            storage::operations::{
                path_to_string,
                tests::{insert_track_files, insert_tracks, mock_trackid, setup_storage},
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
            insert_track_files(&mut storage.db, [(track_id, path_to_string(&path))]);

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

            let track_id = TrackId::from_file(&path)?;

            // DB is empty

            let diff = storage.check_new()?;

            assert_eq!(diff.len(), 1);
            assert!(diff.contains_key(&track_id));
            assert_eq!(diff[&track_id], HashSet::from([path.clone()]));

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
            insert_track_files(&mut storage.db, [(track_id, path_to_string(&path1))]);

            let diff = storage.check_new()?;

            assert_eq!(diff.len(), 1);
            assert_eq!(diff[&track_id], HashSet::from([path2.clone()]));

            Ok(())
        }

        #[test]
        fn test_check_new_ignores_missing_fs_tracks() -> anyhow::Result<()> {
            let dir = tempdir()?;
            let mut storage = setup_storage(dir.path())?;

            let path = dir.path().join("song.mp3");
            let track_id = mock_trackid(123); // file not created

            insert_tracks(&mut storage.db, [track_id]);
            insert_track_files(&mut storage.db, [(track_id, path_to_string(&path))]);

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
            insert_track_files(&mut storage.db, [(track_id, path_to_string(&path))]);

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
            insert_track_files(&mut storage.db, [(track_id, path_to_string(&path))]);

            let diff = storage.check_missing()?;

            assert_eq!(diff.len(), 1);
            assert!(diff.contains_key(&track_id));
            assert_eq!(diff[&track_id], HashSet::from([path.clone()]));

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
            insert_track_files(
                &mut storage.db,
                [
                    (track_id, path_to_string(&available)),
                    (track_id, path_to_string(&missing)),
                ],
            );

            let diff = storage.check_missing()?;

            assert!(diff.is_empty());

            Ok(())
        }
    }
}
