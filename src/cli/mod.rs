use clap::{Parser, Subcommand};
use log::info;
use std::path::PathBuf;

use crate::domain::hash::TrackId;
use crate::domain::track::{ArtworkRef, TrackMetadata};
use crate::storage::db::i64_seconds_to_local_time;
use crate::storage::operations::{MetadataUpdate, Storage};
use crate::{config, public_endpoint};

#[derive(Parser)]
#[command(name = "localdec")]
#[command(author = "Sasha Pak")]
#[command(version = "0.1")]
#[command(about = "Local music library manager")]
pub struct Cli {
    /// Path to the config TOML file
    #[arg(short, long, default_value = "../config.toml")]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Show library status
    Status,
    /// Update library
    Update,
    /// Run http server hosting library
    Serve,
    /// List tracks on the computer
    List {
        /// Include unavailable tracks (tracks in database but files missing)
        #[arg(short, long)]
        show_unavailable: bool,
    },
    /// Find a track
    Find {
        /// Artist, Track Name, or part of the filename to search for
        track: String,
    },
    /// Remove specified path from the database.
    ///
    /// Useful to stop tracking moved or deleted files
    Forget {
        /// Directory or file to remove from database
        path: PathBuf,
    },
    /// Generate url for a track to be printed on qr code or nfc chip
    /// Currently does not include youtube link
    Url { track_id: String },

    /// Add, Get, or update metadata for a track
    Metadata {
        /// Track ID
        track_id: String,

        /// Track title
        #[arg(long)]
        title: Option<String>,

        /// Artist name
        #[arg(long)]
        artist: Option<String>,

        /// Release year
        #[arg(long)]
        year: Option<u32>,

        /// Label / publisher
        #[arg(long)]
        label: Option<String>,

        /// Artwork URL
        #[arg(long)]
        artwork: Option<String>,

        /// Allow overwriting existing metadata
        #[arg(long)]
        overwrite: bool,
    },
}

impl Commands {
    fn to_metadata_update(
        title: Option<String>,
        artist: Option<String>,
        year: Option<u32>,
        label: Option<String>,
        artwork: Option<String>,
    ) -> MetadataUpdate {
        MetadataUpdate {
            title,
            artist,
            year,
            label,
            artwork: artwork.map(ArtworkRef),
        }
    }
}

/// Entrypoint for CLI
pub fn run() {
    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .init();
    info!("Initialized logging to stdout");

    let cli = Cli::parse();

    let cfg = config::Config::load(cli.config.to_str().unwrap()).unwrap();

    match cli.command {
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

        Commands::Serve {} => {
            println!("Starting HTTP server...");

            let storage = Storage::new(cfg.database, cfg.library_source)
                .expect("Failed to initialize storage");

            let http_server = crate::http::server::HttpServer::new(storage, cfg.http);

            println!(
                "HTTP server running at http://{}:{}",
                http_server.config.bind_addr, http_server.config.port
            );
            http_server.run();
        }

        Commands::List { show_unavailable } => {
            let mut storage = Storage::new(cfg.database, cfg.library_source)
                .expect("Failed to initialize storage");

            let tracks = storage.list_tracks().unwrap();

            for track in tracks {
                println!("Track: {}", track.track_id.to_hex());

                if !track.available_files.is_empty() {
                    println!("  Available files:");
                    for path in &track.available_files {
                        println!("    - {}", path.to_string_lossy());
                    }
                } else {
                    println!("  No available files found :(");
                }

                if show_unavailable && !track.unavailable_files.is_empty() {
                    println!("  Unavailable files:");
                    for path in &track.unavailable_files {
                        println!("    - {}", path.to_string_lossy());
                    }
                }
            }
        }
        Commands::Find { track: name } => {
            let mut storage = Storage::new(cfg.database, cfg.library_source)
                .expect("Failed to initialize storage");
            let tracks = storage.find_files(&name).unwrap();
            if !tracks.is_empty() {
                for (trackid, path) in tracks {
                    println!("    - {trackid} at {path}");
                }
            } else {
                println!("No tracks found :(");
            }
        }
        Commands::Forget { path } => {
            let mut storage = Storage::new(cfg.database, cfg.library_source)
                .expect("Failed to initialize storage");
            let report = storage.forget_path(&path).unwrap();
            if report.affected_tracks == 0 {
                println!("No tracks located under {} found", path.to_string_lossy());
            } else {
                println!(
                    "Forget operation completed:\n  Removed files: {}\n  Affected tracks: {}\n  Removed tracks: {}",
                    report.removed_files, report.affected_tracks, report.removed_tracks
                );
            }
        }
        Commands::Url { track_id } => {
            let track_id = TrackId::from_hex(track_id).unwrap();
            let url = public_endpoint::get_play_url(&cfg.public_endpoint, track_id, None);
            println!("{url}");
        }

        Commands::Metadata {
            track_id,
            title,
            artist,
            year,
            label,
            artwork,
            overwrite,
        } => {
            let mut storage = Storage::new(cfg.database, cfg.library_source)
                .expect("Failed to initialize storage");

            let track_id = TrackId::from_hex(&track_id).unwrap();

            if title.is_none()
                && artist.is_none()
                && year.is_none()
                && label.is_none()
                && artwork.is_none()
            {
                let meta = storage.get_track_metadata(track_id).unwrap();
                if let Some(meta) = meta {
                    println!("{}", pretty_metadata(meta));
                } else {
                    println!("No metadata for this track found :(");
                }
            } else {
                let update = Commands::to_metadata_update(title, artist, year, label, artwork);

                storage
                    .update_track_metadata(track_id, update, overwrite)
                    .unwrap();
                println!("Metadata updated for {}", track_id);
            }
        }
    }
}

pub fn pretty_metadata(m: TrackMetadata) -> String {
    let mut lines = Vec::new();

    lines.push(format!("Title : {}", m.title));
    lines.push(format!("Artist: {}", m.artist));

    if let Some(year) = m.year {
        lines.push(format!("Year  : {}", year));
    }

    if let Some(label) = m.label {
        lines.push(format!("Label : {}", label));
    }

    if let Some(artwork) = m.artwork {
        lines.push(format!("Artwork: {}", artwork.0));
    }

    lines.join("\n")
}
