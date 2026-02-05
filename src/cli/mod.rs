use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::config;
use crate::storage::db::i64_seconds_to_local_time;
use crate::storage::operations::Storage;
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

    match &cli.command {
        Commands::Status {} => {
            let mut storage = Storage::new(cfg.database, cfg.library_source).unwrap();
            let (fs_snapshot, db_snapshot, diff_result) = storage.status().unwrap();

            println!("Filesystem contains {} files", fs_snapshot.files.len());
            println!(
                "Database was updated {} and contains {} files",
                i64_seconds_to_local_time(db_snapshot.updated_at).unwrap(),
                db_snapshot.files.len()
            );
            if !diff_result.is_empty() {
                println!(
                    "Certain files' locations do not match database. Run \"update\" to update the database:"
                );

                for (track_id, changes) in &diff_result {
                    if changes.is_new() {
                        println!("  [NEW]  {}, found at:", track_id);
                        for location in changes.new_locations() {
                            println!("    - {}", location.to_string_lossy());
                        }
                    } else if changes.is_deleted() {
                        println!("  [DELETED]  {}, previously located at:", track_id);
                        for location in changes.deleted_locations() {
                            println!("    - {}", location.to_string_lossy());
                        }
                    } else {
                        println!("  [MOVED / COPIED]  {}", track_id);
                        println!("  removed locations:");
                        for location in changes.deleted_locations() {
                            println!("    - {}", location.to_string_lossy());
                        }
                        println!("  new locations:");
                        for location in changes.new_locations() {
                            println!("    - {}", location.to_string_lossy());
                        }
                    }
                }
            }
        }

        Commands::Update {} => {
            let mut storage = Storage::new(cfg.database, cfg.library_source).unwrap();
            let files = storage.update_db_with_new_files().unwrap();
            println!("Database updated, new files ({}):", files.len());
            for file in &files {
                println!("    - {} at {}", file.track_id, file.path.to_string_lossy());
            }
        }
    }
}
