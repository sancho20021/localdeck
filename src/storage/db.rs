use rusqlite::Connection;

#[derive(Debug)]
pub struct Db {
    conn: rusqlite::Connection,
}

impl Db {
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Ok(Self {
            conn: Connection::open_in_memory()?,
        })
    }
}
