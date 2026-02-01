use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::SystemTime,
};

use crate::{
    config,
    domain::hash::TrackId,
    storage::{
        db::{SecondsSinceUnix, system_time_to_i64},
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

pub fn scan_db(db: &mut rusqlite::Connection) -> anyhow::Result<DBSnapshot> {
    let tx = db.transaction()?;

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

pub fn diff(fs: &FsSnapshot, ds: &DBSnapshot) -> Diff {
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

/// aka git status
///
/// reads files in the file system,
/// reads file records in the database,
/// returns both, and difference between the database and the file system
pub fn status(
    config: &config::LibrarySource,
    db: &mut rusqlite::Connection,
) -> anyhow::Result<(FsSnapshot, DBSnapshot, Diff)> {
    let fs = FsSnapshot::scan(config)?;
    let db = scan_db(db)?;
    let diff = diff(&fs, &db);
    Ok((fs, db, diff))
}

/// Updates the database by adding new files from the diff.
pub fn update_db_with_new_files(
    update_time: SystemTime,
    diff_result: &Diff,
    db: &mut rusqlite::Connection,
) -> anyhow::Result<()> {
    let time_secs = system_time_to_i64(update_time)?;
    let tx = db.transaction()?;

    let new = diff_result.iter().flat_map(|(id, changes)| {
        changes
            .new_locations()
            .into_iter()
            .map(|path| (id.clone(), path))
    });

    for (track_id, path) in new {
        tx.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![track_id.to_hex(), path.to_string_lossy()],
        )?;
    }

    tx.execute(
        &format!("INSERT INTO {UPDATES} ({UPDATED_AT}) VALUES (?1)"),
        params![time_secs],
    )?;

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        path::PathBuf,
        time::SystemTime,
    };

    use rusqlite::params;

    use crate::{
        domain::hash::TrackId,
        storage::{
            fs::{FsSnapshot, ObservedFile},
            operations::{DBSnapshot, Diff, TrackChange, diff, scan_db, update_db_with_new_files},
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
        let mut conn = rusqlite::Connection::open_in_memory()?;

        schema::init(&conn)?;

        conn.execute(
            &format!("INSERT INTO {FILES} ({TRACK_ID}, {PATH}) VALUES (?1, ?2)"),
            params![mock_trackid_str(1), "song.mp3"],
        )?;

        conn.execute(
            &format!("INSERT INTO {UPDATES} ({UPDATED_AT}) VALUES (?1)"),
            params![200],
        )?;

        let snapshot = scan_db(&mut conn)?;

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

        let diff_result = diff(&fs_snapshot, &db_snapshot);

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

        let diff_result = diff(&fs_snapshot, &db_snapshot);

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

        // Call the function
        update_db_with_new_files(SystemTime::now(), &diff_result, &mut conn)?;

        // Reuse scan_db to read DB snapshot
        let db_snapshot = scan_db(&mut conn)?;

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
}
