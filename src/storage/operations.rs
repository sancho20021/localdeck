use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::SystemTime,
};

use crate::{
    config::{self, LibrarySource},
    domain::{hash::TrackId, track::Track},
    storage::{
        self,
        db::{self, SecondsSinceUnix, system_time_to_i64},
        fs::{FsSnapshot, ObservedFile},
        schema::{columns, tables},
    },
};

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
    db: rusqlite::Connection,
    source: LibrarySource,
}

impl Storage {
    /// when called, opens a data base connection
    pub fn new(db_config: config::Database, lib_config: LibrarySource) -> anyhow::Result<Self> {
        let db: rusqlite::Connection = db::open(&db_config)?;
        Ok(Self::from_existing_conn(db, lib_config))
    }

    pub fn from_existing_conn(db: rusqlite::Connection, lib_config: LibrarySource) -> Self {
        Self {
            db,
            source: lib_config,
        }
    }

    pub fn scan_db(&mut self) -> anyhow::Result<DBSnapshot> {
        let tx = self.db.transaction()?;

        let (files, updated_at) = {
            let mut stmt = tx.prepare(&format!("SELECT {TRACK_ID}, {PATH} FROM {FILES}"))?;
            let files = stmt
                .query_map([], |row| {
                    let track_id_hex: String = row.get(0)?;
                    let path: String = row.get(1)?;

                    Ok((TrackId::from_hex(&track_id_hex), path.into()))
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
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(DBSnapshot { updated_at, files })
    }

    /// Updates the database by adding new files from the diff.
    fn _update_db_with_new_files(
        &mut self,
        update_time: SystemTime,
        diff_result: &Diff,
    ) -> anyhow::Result<Vec<ObservedFile>> {
        let time_secs = system_time_to_i64(update_time)?;
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

    pub fn update_db_with_new_files(&mut self) -> anyhow::Result<Vec<ObservedFile>> {
        let (fs, _, diff_result) = self.status().unwrap();
        let time = fs.observed_at;
        self._update_db_with_new_files(time, &diff_result)
    }

    /// aka git status
    ///
    /// reads files in the file system,
    /// reads file records in the database,
    /// returns both, and difference between the database and the file system
    pub fn status(&mut self) -> anyhow::Result<(FsSnapshot, DBSnapshot, Diff)> {
        let fs = FsSnapshot::scan(&self.source)?;
        let db = self.scan_db()?;
        let diff = Self::diff(&fs, &db);
        Ok((fs, db, diff))
    }

    /// retrieves location of the track, checking that it is present in the file system
    ///
    /// If multiple locations point to the same track, chooses one of them.
    pub fn get_track(&mut self, track_id: TrackId) -> anyhow::Result<(Track, PathBuf)> {
        let mut stmt = self
            .db
            .prepare("SELECT path FROM files WHERE track_id = ?1")?;

        let paths = stmt
            .query_map(params![track_id.to_string()], |row| {
                Ok(PathBuf::from(row.get::<_, String>(0)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        if paths.is_empty() {
            return Err(anyhow::anyhow!("track {} not found in database", track_id));
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
            Err(anyhow::anyhow!(
                "track {} exists in database, but no valid music files were found on disk",
                track_id
            ))
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
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs,
        path::PathBuf,
        time::SystemTime,
    };

    use rusqlite::params;
    use tempfile::tempdir;

    use crate::{
        domain::hash::TrackId,
        storage::{
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

        assert!(
            err.to_string()
                .contains("no valid music files were found on disk")
        );

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

        assert!(
            err.to_string().contains("track") && err.to_string().contains("not found in database")
        );

        Ok(())
    }
}
