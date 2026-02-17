use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use log::info;
use std::env;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;

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
    /// If not provided, reads it from LOCALDECK_CONFIG env var
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Check library status
    Check {
        #[command(subcommand)]
        action: Option<CheckAction>,
    },
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

    /// get or edit metadata
    Meta {
        #[command(subcommand)]
        action: MetaAction,
    },
}

#[derive(Subcommand)]
pub enum CheckAction {
    /// Check for new music files not present in database
    New,
    /// Check for tracks without any available files.
    ///
    /// Ignores tracks that have at least one available file.
    Missing,
}

#[derive(Subcommand)]
enum MetaAction {
    /// Get track metadata
    Get {
        /// Track Id to fetch
        track_id: String,
        /// Use json format
        #[arg(long)]
        json: bool,
    },
    /// Add or update metadata
    Add {
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
pub fn run() -> anyhow::Result<()> {
    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .init();
    info!("Initialized logging to stdout");

    let cli = Cli::parse();

    let cfg_path = if let Some(path) = cli.config {
        path
    } else {
        let path = env::var("LOCALDECK_CONFIG")
            .context("Failed to get path to config. Provide it via flag or environment variable")?;
        PathBuf::from(path)
    };
    let cfg = config::Config::load(&cfg_path)?;

    match cli.command {
        Commands::Check { action } => {
            let mut storage = Storage::new(cfg.database, cfg.library_source)?;
            if let Some(action) = action {
                match action {
                    CheckAction::New => {
                        let new = storage.check_new()?;
                        if !new.is_empty() {
                            for (track, locs) in new {
                                println!("  [NEW]  {}, found at:", track);
                                for location in locs {
                                    println!("    - {}", location.to_string_lossy());
                                }
                            }
                        } else {
                            println!("No new files discovered :)");
                        }
                    }
                    CheckAction::Missing => {
                        let missing = storage.check_missing()?;
                        if !missing.is_empty() {
                            println!("The following tracks do not have available files:");
                            for (track, old_locs) in missing {
                                println!("{track}");
                                if !old_locs.is_empty() {
                                    println!("Unavailable locations:");
                                    for loc in old_locs {
                                        println!("   - {}", loc.to_string_lossy());
                                    }
                                }
                            }
                        } else {
                            println!("No missing files!");
                        }
                    }
                }
            } else {
                let ds = storage.scan_db()?;
                println!(
                    "Data base was updated {}",
                    i64_seconds_to_local_time(ds.updated_at)?
                );
            }
        }

        Commands::Update {} => {
            let mut storage = Storage::new(cfg.database, cfg.library_source)?;
            let files = storage.update_db_with_new_files()?;
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

            let tracks = storage.list_tracks()?;

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
            let tracks = storage.find_files(&name)?;
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
            let report = storage.forget_path(&path)?;
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
            let track_id = TrackId::from_hex(track_id)?;
            let url = public_endpoint::get_play_url(&cfg.public_endpoint, track_id, None);
            println!("{url}");
        }

        Commands::Meta { action } => {
            let mut storage = Storage::new(cfg.database, cfg.library_source)
                .expect("Failed to initialize storage");
            match action {
                MetaAction::Get { track_id, json } => {
                    let track_id = TrackId::from_hex(&track_id)?;
                    let meta = storage.get_track_metadata(track_id)?;
                    if let Some(meta) = meta {
                        let str = if json {
                            serde_json::to_string(&meta)
                                .expect("failed to serialize metadata to json")
                        } else {
                            pretty_metadata(meta)
                        };
                        println!("{str}");
                    } else {
                        bail!("No metadata for this track found :(");
                    }
                }
                MetaAction::Add {
                    track_id,
                    title,
                    artist,
                    year,
                    label,
                    artwork,
                    overwrite,
                } => {
                    let track_id = TrackId::from_hex(&track_id)?;
                    let update = Commands::to_metadata_update(title, artist, year, label, artwork);

                    storage.update_track_metadata(track_id, update, overwrite)?;
                    println!("Metadata updated for {}", track_id);
                }
            }
        }
    }
    Ok(())
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
