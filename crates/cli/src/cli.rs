use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use log::info;
use std::env;
use std::path::PathBuf;

use crate::music_player::Output;
use crate::{card_player, config};
use localdeck_storage::operations::{MetadataUpdate, Storage};
use localdeck_storage::track::{ArtworkRef, TrackId, TrackMetadata};

#[derive(Parser)]
#[command(name = "localdeck")]
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
    /// Automatically update library by scanning configured directories
    Update,
    /// Link a specific music file to an existing track ID
    /// (Useful for adding high-quality, fixed, or alternative versions)
    Add {
        /// The existing track ID to append the file to
        track_id: TrackId,
        /// Path to the physical music file
        path: PathBuf,
    },
    /// Merge a duplicate or lower-quality track into a master track
    Merge {
        /// The slave track ID that will be completely deleted
        slave_id: TrackId,

        /// The master track ID that absorbs all files and mappings
        #[arg(long, short = 'i', name = "MASTER_ID")]
        into: TrackId,

        /// Skip checking or raising an error if the slave track contains metadata
        #[arg(long)]
        ignore_slave_meta: bool,
    },
    /// Run http server hosting library
    Serve,
    /// Find a track
    Find {
        /// Artist, Track Name, Track Id or part of the filename to search for
        track: String,
        /// Find tracks only without metadata
        #[arg(long)]
        no_meta: bool,
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
    Url { track_id: TrackId },

    /// get or edit metadata
    Meta {
        #[command(subcommand)]
        action: MetaAction,
    },

    /// Clean dangling tracks (no files + no metadata)
    Clean,

    /// Start QR music player (needs qr scanner connected via USB)
    Scan {
        /// Device name to play audio from
        #[arg(short, long)]
        device: Option<String>,
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
    /// Check for tracks without any files recorded in database
    Stale,
}

#[derive(Subcommand)]
pub enum MetaAction {
    /// Get track metadata
    Get {
        /// Track Id to fetch
        track_id: TrackId,
        /// Use json format
        #[arg(long)]
        json: bool,
    },
    /// Add or update metadata
    Add {
        /// Track ID
        track_id: TrackId,

        /// Track title
        #[arg(short, long)]
        title: Option<String>,

        /// Artist name
        #[arg(short, long)]
        artist: Option<String>,

        /// Release year
        #[arg(short, long)]
        year: Option<u32>,

        /// Label / publisher
        #[arg(short, long)]
        label: Option<String>,

        /// Artwork URL
        #[arg(long)]
        artwork: Option<String>,

        /// Allow overwriting existing metadata
        #[arg(long)]
        overwrite: bool,
    },
    /// retrieve all metadata
    All,
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
            .context("Failed to get path to config. Provide it via flag or environment variable LOCALDECK_CONFIG")?;
        PathBuf::from(path)
    };
    let cfg = config::Config::load(&cfg_path)?;

    match cli.command {
        Commands::Check { action } => {
            let mut storage = Storage::new(cfg.storage)?;
            if let Some(action) = action {
                match action {
                    CheckAction::New => {
                        let new = storage.check_new()?;
                        if !new.is_empty() {
                            for file in new {
                                println!("{}\n   size: {:.2} MB\n", file.loc, file.size_mb());
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
                                    for file in old_locs {
                                        println!(
                                            "  - {}\n      size: {:.2} MB",
                                            file.loc,
                                            file.size_mb()
                                        );
                                    }
                                }
                            }
                        } else {
                            println!("No missing files!");
                        }
                    }
                    CheckAction::Stale => {
                        let stale = storage.check_stale()?;

                        let has_metadata_only = !stale.metadata_only.is_empty();
                        let has_dangling = !stale.dangling.is_empty();

                        if has_metadata_only || has_dangling {
                            if has_metadata_only {
                                println!("Tracks with metadata but no associated files:");

                                for track in stale.metadata_only {
                                    println!("  - {track}");
                                }

                                println!();
                            }

                            if has_dangling {
                                println!("Dangling tracks (no files and no metadata):");

                                for track in stale.dangling {
                                    println!("  - {track}");
                                }

                                println!();

                                println!("You can remove dangling tracks with:");
                                println!("localdeck clean");
                            }
                        } else {
                            println!("No stale tracks!");
                        }
                    }
                }
            } else {
                let time = storage.updated_at()?;
                println!("Data base was updated {}", time);
            }
        }

        Commands::Update {} => {
            let mut storage = Storage::new(cfg.storage)?;
            let files = storage.update_db_with_new_files()?;
            println!("Database updated, new files ({}):", files.len());
            for (track, files) in &files {
                println!("  * track {track}:");
                for file in files {
                    println!("    - {}", file.file.loc);
                }
            }
        }

        Commands::Serve {} => {
            println!("Starting HTTP server...");

            let storage = Storage::new(cfg.storage).expect("Failed to initialize storage");

            let http_server = localdeck_http::server::HttpServer::new(storage, cfg.http);

            println!(
                "HTTP server running at http://{}:{}",
                http_server.config.bind_addr, http_server.config.port
            );
            http_server.run();
        }

        Commands::Find {
            track: name,
            no_meta,
        } => {
            let mut storage = Storage::new(cfg.storage).expect("Failed to initialize storage");
            let tracks = storage.find_files(&name, no_meta)?;
            if !tracks.is_empty() {
                for (trackid, paths) in tracks {
                    println!("{trackid} at:");
                    for path in paths {
                        println!("    - {path}");
                    }
                }
            } else {
                println!("No tracks found :(");
            }
        }
        Commands::Forget { path } => {
            let mut storage = Storage::new(cfg.storage).expect("Failed to initialize storage");
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
            let mut storage = Storage::new(cfg.storage).expect("Failed to initialize storage");
            let _ = storage.get_track_metadata(track_id).unwrap();
            println!("{track_id}");
        }

        Commands::Meta { action } => {
            let mut storage = Storage::new(cfg.storage).expect("Failed to initialize storage");
            match action {
                MetaAction::Get { track_id, json } => {
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
                    let update = Commands::to_metadata_update(title, artist, year, label, artwork);

                    storage.update_track_metadata(track_id, update, overwrite)?;
                    println!("Metadata updated for {}", track_id);
                }
                MetaAction::All => {
                    let meta = storage.scan_metadata()?;
                    println!("Database contains metadata for {} tracks", meta.len());
                    for track in meta {
                        println!("- {}", track.id);
                        println!("{}\n", pretty_metadata(track.metadata));
                    }
                }
            }
        }
        Commands::Clean => {
            let mut storage = Storage::new(cfg.storage).expect("Failed to initialize storage");
            let report = storage.clean_dangling()?;

            if report.removed_tracks > 0 {
                println!("Removed {} dangling track(s)", report.removed_tracks);
            } else {
                println!("Nothing to clean :)");
            }
        }
        Commands::Scan { device } => {
            let mut storage = Storage::new(cfg.storage)?;
            let output = match device {
                Some(d) => Output::Device(d),
                None => Output::Default,
            };
            card_player::run_card_player(&mut storage, output).unwrap();
        }
        Commands::Add { track_id, path } => {
            let mut storage = Storage::new(cfg.storage)?;
            storage.add_file_to_track(track_id, &path)?;
            println!("Linked {} to track {}", path.to_string_lossy(), track_id);
        }
        Commands::Merge {
            slave_id,
            into,
            ignore_slave_meta,
        } => {
            let mut storage = Storage::new(cfg.storage)?;
            storage.merge_tracks(into, slave_id, ignore_slave_meta)?;
            println!("Track {} successfully merged into {}", slave_id, into);
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
