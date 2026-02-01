use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::config;
use crate::storage::db::i64_seconds_to_local_time;
use crate::storage::operations::FileChange;
use crate::storage::{db, operations};

#[derive(Parser)]
#[command(name = "localdec")]
#[command(author = "Sasha Pak")]
#[command(version = "0.1")]
#[command(about = "Local music library manager")]
pub struct Cli {
    /// Path to the config TOML file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Status,
    Update,
}

/// Entrypoint for CLI
pub fn run() {
    let cli = Cli::parse();

    let cfg = config::Config::load(cli.config.to_str().unwrap()).unwrap();

    let mut conn = db::open(&cfg.database).unwrap();

    match &cli.command {
        Commands::Status {} => {
            let (fs_snapshot, db_snapshot, diff_result) =
                operations::status(&cfg.library_source, &mut conn).unwrap();

            println!("Filesystem contains {} files", fs_snapshot.files.len());
            println!(
                "Database was updated {} and contains {} files",
                i64_seconds_to_local_time(db_snapshot.updated_at).unwrap(),
                db_snapshot.files.len()
            );
            println!("Certain files' locations do not match database:");

            for (track_id, changes) in &diff_result {
                for change in changes {
                    match change {
                        FileChange::New(path) => {
                            println!("  [NEW]   {} -> {:?}", track_id.to_hex(), path);
                        }
                        FileChange::Deleted(path) => {
                            println!("  [DELETED] {} -> {:?}", track_id.to_hex(), path);
                        }
                    }
                }
            }
        }

        Commands::Update {} => {
            let (_, _, diff_result) = operations::status(&cfg.library_source, &mut conn).unwrap();
            operations::update_db_with_new_files(&diff_result, &mut conn).unwrap();
            println!("Database updated with new files.");
        }
    }
}
