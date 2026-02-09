use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::SystemTime,
};

use crate::{
    config::{self, LibrarySource},
    domain::{
        hash::TrackId,
        track::{Track, TrackMetadata},
    },
    storage::{
        self,
        db::{self, SecondsSinceUnix, system_time_to_i64},
        error::StorageError,
        fs::{FsSnapshot, ObservedFile},
        schema::{columns, tables},
    },
};

use anyhow::{Context, anyhow};
use columns::*;
use rusqlite::params;
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
    pub metadata: TrackMetadata,
    pub available_files: Vec<PathBuf>,
    pub unavailable_files: Vec<PathBuf>,
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

impl Storage {
    /// when called, opens a data base connection
    pub fn new(
        db_config: config::Database,
        lib_config: LibrarySource,
    ) -> Result<Self, StorageError> {
        let db: rusqlite::Connection = db::open(&db_config)?;
        Ok(Self::from_existing_conn(db, lib_config))
    }

    pub fn from_existing_conn(db: rusqlite::Connection, lib_config: LibrarySource) -> Self {
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

    /// Updates the database by adding new files from the diff.
    fn _update_db_with_new_files(
        &mut self,
        update_time: SystemTime,
        diff_result: &Diff,
    ) -> Result<Vec<ObservedFile>, StorageError> {
        let time_secs = system_time_to_i64(update_time).map_err(StorageError::Internal)?;
        let tx = self.db.transaction()?;

        let new = diff_result
            .iter()
            .flat_map(|(id, changes)| {
                changes
                    .new_locations()
                    .into_iter()
                    .map(|path| ObservedFile::new(id.clone(), path))
            })
            .collect::<Vec<_>>();

        for file in &new {
            tx.execute(
                &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
                params![file.track_id.to_hex(), file.path.to_string_lossy()],
            )?;
        }

        tx.execute(
            &format!("INSERT INTO {UPDATES} ({UPDATED_AT}) VALUES (?1)"),
            params![time_secs],
        )?;

        tx.commit()?;
        Ok(new)
    }

    pub fn update_db_with_new_files(&mut self) -> Result<Vec<ObservedFile>, StorageError> {
        let (fs, _, diff_result) = self.status()?;
        let time = fs.observed_at;
        self._update_db_with_new_files(time, &diff_result)
    }

    /// aka git status
    ///
    /// reads files in the file system,
    /// reads file records in the database,
    /// returns both, and difference between the database and the file system
    pub fn status(&mut self) -> Result<(FsSnapshot, DBSnapshot, Diff), StorageError> {
        println!("Scanning the music on file system...");
        let fs = FsSnapshot::scan(&self.source)?;
        let db = self.scan_db()?;
        let diff = Self::diff(&fs, &db);
        Ok((fs, db, diff))
    }

    /// retrieves location of the track, checking that it is present in the file system
    ///
    /// If multiple locations point to the same track, chooses one of them.
    pub fn get_track(&mut self, track_id: TrackId) -> Result<(Track, PathBuf), StorageError> {
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
            Ok((
                Track {
                    id: track_id,
                    metadata: Default::default(),
                },
                path,
            ))
        } else {
            Err(StorageError::InvalidTrackFile { track: track_id })
        }
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

    /// List unique tracks, optionally including unavailable ones
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
                metadata: TrackMetadata::default(), // todo: fill from DB if metadata stored
                available_files: available_paths,
                unavailable_files: unavailable_paths,
            });
        }

        Ok(result)
    }

    /// searches for a file where path matches the query
    /// todo: extend it with track metadata once metadata is stored in database
    pub fn find_files(&mut self, query: &str) -> Result<Vec<(TrackId, String)>, StorageError> {
        let tx = self.db.transaction()?;

        let mut stmt = tx.prepare(&format!("SELECT {TRACK_ID}, {PATH} FROM {FILES}"))?;

        // Normalize the query string: lowercase, remove spaces and some punctuation
        let norm_query = query
            .to_lowercase()
            .replace(' ', "")
            .replace('-', "")
            .replace('_', "")
            .replace('.', "");

        let results = stmt
            .query_map([], |row| {
                let track_id_hex: String = row.get(0)?;
                let path: String = row.get(1)?;

                // Normalize path the same way
                let norm_path = path
                    .to_lowercase()
                    .replace(' ', "")
                    .replace('-', "")
                    .replace('_', "")
                    .replace('.', "");

                // If normalized path contains normalized query, keep it
                if norm_path.contains(&norm_query) {
                    let track_id = TrackId::from_hex(&track_id_hex);
                    if let Ok(track_id) = track_id {
                        Ok(Some((track_id, path)))
                    } else {
                        log::warn!("Database table {FILES} contain invalid trackid {track_id_hex}");
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            })?
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        drop(stmt);

        tx.commit()?;
        Ok(results)
    }

    /// removes all files inside specified directory from the database
    /// useful when some files got moved or deleted
    pub fn forget_path(&mut self, path: &Path) -> Result<ForgetReport, StorageError> {
        let time_secs = system_time_to_i64(SystemTime::now()).map_err(StorageError::Internal)?;

        let tx = self.db.transaction()?;

        let prefix = path.to_string_lossy();

        // --------------------------------------------------
        // Collect affected track ids BEFORE deletion
        // --------------------------------------------------

        let mut stmt = tx.prepare(&format!(
            "SELECT DISTINCT {TRACK_ID} FROM {FILES}
         WHERE {PATH} = ?1 OR {PATH} LIKE ?2"
        ))?;

        let affected_track_ids = stmt
            .query_map(params![prefix, format!("{}/%", prefix)], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;

        drop(stmt);

        let affected_tracks = affected_track_ids.len();

        // --------------------------------------------------
        // Count entries to delete
        // --------------------------------------------------

        let removed_files: usize = tx
            .query_row::<isize, _, _>(
                &format!(
                    "SELECT COUNT(*) FROM {FILES}
             WHERE {PATH} = ?1 OR {PATH} LIKE ?2"
                ),
                params![prefix, format!("{}/%", prefix)],
                |row| row.get(0),
            )?
            .try_into()
            .map_err(|e| {
                StorageError::Internal(anyhow!(
                    "Strange conversion error to usize after select count: {e}"
                ))
            })?;

        // --------------------------------------------------
        // Delete entries
        // --------------------------------------------------

        tx.execute(
            &format!(
                "DELETE FROM {FILES}
             WHERE {PATH} = ?1 OR {PATH} LIKE ?2"
            ),
            params![prefix, format!("{}/%", prefix)],
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

        tx.execute(
            &format!("INSERT INTO {UPDATES} ({UPDATED_AT}) VALUES (?1)"),
            params![time_secs],
        )?;

        tx.commit()?;

        Ok(ForgetReport {
            removed_tracks,
            affected_tracks,
            removed_files,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs,
        path::{Path, PathBuf},
        time::SystemTime,
    };

    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use crate::{
        config::{LibrarySource, Location},
        domain::hash::TrackId,
        storage::{
            error::StorageError,
            fs::{FsSnapshot, ObservedFile},
            operations::{DBSnapshot, Diff, Storage, TrackChange},
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

    #[test]
    fn test_scan_db() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;

        schema::init(&conn)?;

        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![mock_trackid_str(1), "song.mp3"],
        )?;

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
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        // Pre-insert some files
        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![mock_trackid_str(1), "a.mp3"],
        )?;

        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![mock_trackid_str(2), "b1.mp3"],
        )?;

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
        storage._update_db_with_new_files(SystemTime::now(), &diff_result)?;

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
    fn test_get_track_success() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let dir = tempdir()?;
        let file_path = dir.path().join("song.mp3");

        // Create valid music file
        fs::write(&file_path, b"x")?;

        let track_id = mock_trackid(1);

        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), file_path.to_string_lossy()],
        )?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let (track, path) = storage.get_track(track_id)?;

        assert_eq!(track.id, track_id);
        assert_eq!(path, file_path);

        Ok(())
    }

    #[test]
    fn test_get_track_invalid_paths() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let dir = tempdir()?;
        let bad_path = dir.path().join("song.txt"); // invalid extension

        fs::write(&bad_path, b"x")?;

        let track_id = mock_trackid(3);

        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), bad_path.to_string_lossy()],
        )?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let err = storage.get_track(track_id).unwrap_err();

        assert!(matches!(err, StorageError::InvalidTrackFile { .. }));

        Ok(())
    }

    #[test]
    fn test_get_track_multiple_paths_picks_valid() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let dir = tempdir()?;

        let bad = dir.path().join("bad.txt");
        let good = dir.path().join("good.mp3");

        fs::write(&bad, b"x")?;
        fs::write(&good, b"x")?;

        let track_id = mock_trackid(5);

        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), bad.to_string_lossy()],
        )?;

        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), good.to_string_lossy()],
        )?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let (_, path) = storage.get_track(track_id)?;

        assert_eq!(path, good);

        Ok(())
    }

    #[test]
    fn test_get_track_not_in_db() -> anyhow::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        schema::init(&conn)?;

        let mut storage = Storage::from_existing_conn(conn, Default::default());

        let track_id = mock_trackid(42);

        let err = storage.get_track(track_id).unwrap_err();

        assert!(matches!(err, StorageError::TrackNotFound(..)));

        Ok(())
    }

    #[test]
    fn test_list_tracks_fully_available() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let mut storage = setup_storage(dir.path())?;
        let path = dir.path().join("song.mp3");
        fs::write(&path, b"x")?;

        let track_id = TrackId::from_file(&path)?;

        storage.db.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), path.to_string_lossy()],
        )?;

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
        storage.db.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), available_path.to_string_lossy()],
        )?;
        storage.db.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), unavailable_path.to_string_lossy()],
        )?;

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

        storage.db.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), path.to_string_lossy()],
        )?;

        let tracks = storage.list_tracks()?;
        assert_eq!(tracks.len(), 1);

        let entry = &tracks[0];
        assert_eq!(entry.track_id, track_id);
        assert!(entry.available_files.is_empty());
        assert_eq!(entry.unavailable_files, vec![path.clone()]);

        Ok(())
    }

    #[test]
    fn test_find_files() {
        let conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();

        // Insert some test rows
        let data = vec![
            (mock_trackid_str(1), "Some Artist - Track Name.mp3"),
            (mock_trackid_str(2), "AnotherArtist_TrackName.flac"),
            (mock_trackid_str(3), "completely-different-track.mp3"),
        ];

        for (id, path) in &data {
            conn.execute(
                "INSERT INTO files (track_id, path) VALUES (?1, ?2)",
                [&id[..], &path[..]],
            )
            .unwrap();
        }

        let mut storage = Storage::from_existing_conn(conn, LibrarySource::default());

        // Search for a liberal match
        let results = storage.find_files("trackname").unwrap();

        // Should match first two entries
        assert_eq!(
            results,
            vec![
                (mock_trackid(1), "Some Artist - Track Name.mp3".to_string()),
                (mock_trackid(2), "AnotherArtist_TrackName.flac".to_string())
            ]
        );

        // Search with different casing and spaces
        let results2 = storage.find_files("another artist track").unwrap();
        assert_eq!(
            results2,
            vec![(mock_trackid(2), "AnotherArtist_TrackName.flac".to_string())]
        );

        // Search for non-existent track
        let results3 = storage.find_files("nonexistent").unwrap();
        assert!(results3.is_empty());
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

        // Mock files
        let file_a = "/music/track_a1.mp3";
        let file_a2 = "/music/subdir/track_a2.mp3";
        let file_b = "/music/track_b.mp3";
        let file_c = "/hello/track_c.mp3"; // outside deleted path
        let file_a3 = "/hello/track_a3.mp3"; // outside deleted path

        let track_a = "trackid_a";
        let track_b = "trackid_b";
        let track_c = "trackid_c";

        insert_file(&storage.db, track_a, file_a);
        insert_file(&storage.db, track_a, file_a2);
        insert_file(&storage.db, track_a, file_a3);
        insert_file(&storage.db, track_b, file_b);
        insert_file(&storage.db, track_c, file_c);

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
}
