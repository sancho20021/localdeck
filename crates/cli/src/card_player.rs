use anyhow::bail;
use url::Url;

use crate::{
    music_player::{AudioPlayerError, MusicPlayer, Output, start_music_player},
    qr_scanner::{QrScanner, start_qr_scanner},
};
use localdeck_storage::{TrackId, operations::Storage};

const STOP_LOCALDECK: &'static str = "FINISH";
const STOP_MUSIC: &'static str = "STOP_MUSIC";

fn shutdown(player: MusicPlayer, scanner: QrScanner) {
    println!("Turning off the card player");
    scanner.shutdown();
    player.shutdown();
}

/// Starts:
/// - QR scanner thread
/// - audio player thread
///
/// Then continuously:
/// QR scan -> extract track id -> resolve path -> play
pub fn run_card_player(storage: &mut Storage, output: Output) -> anyhow::Result<()> {
    let (qr_events, scanner) = start_qr_scanner();

    let (audio_errors, player) = match start_music_player(output) {
        Ok(s) => s,
        Err(e) => {
            bail!("Failed to start music player: {e}");
        }
    };

    // ---------------------------------------------------
    // Main Event Loop (QR Scanning & Audio Error Monitoring)
    // ---------------------------------------------------
    log::info!("Starting main qr and audio errors event controller loop...");
    loop {
        crossbeam::select! {
                    // ---------------------------------------------
                    // Channel A: Monitoring Audio Failures
                    // ---------------------------------------------
                    recv(audio_errors) -> msg => {
                        match msg {
                            Ok(e) => {
                                eprintln!("audio player error: {e}");
                                if !matches!(e, AudioPlayerError::FileOpen(_)) {
                                    scanner.shutdown();
                                    player.shutdown();
                                    bail!("Player can't play audio, terminating");
                                }
                            }
                            Err(_) => {
                                // The audio error channel disconnected (player thread probably panicked or closed)
                                log::warn!("Audio error reporting channel disconnected.");
                                break;
                            }
                        }
                    }

                    // ---------------------------------------------
                    // Channel B: Main QR Processing Pipeline
                    // ---------------------------------------------
                    recv(qr_events) -> msg => {
                        let event = match msg {
                            Ok(evt) => evt,
                            Err(_) => {
                                // QR channel closed/dropped, break out of loop cleanly
                                log::info!("QR event channel closed.");
                                break;
                            }
                        };

                        match event {
                            // QR successfully scanned
                            Ok(raw) => {
                                let raw = raw.trim();
                                log::info!("scanned qr: {raw}");

                                if raw == STOP_LOCALDECK {
                                    shutdown(player, scanner);
                                    return Ok(());
                                }
                                if raw == STOP_MUSIC {
                                    player.stop();
                                    println!("Music stopped");
                                }

                                let track_id = match extract_trackid(&raw) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        eprintln!("invalid qr payload: {e}");
                                        continue;
                                    }
                                };

                                let (path, metadata) = match storage.find_track_file_with_meta(track_id) {
                                    Ok((path, _, metadata)) => (path, metadata),
                                    Err(e) => {
                                        eprintln!("could not resolve track {}: {}", track_id, e);
                                        continue;
                                    }
                                };

                                if let Some(meta) = &metadata {
                                    let year = meta
                                        .year
                                        .map(|y| y.to_string())
                                        .unwrap_or_else(|| "?".into());

                                    let label = meta.label.as_deref().unwrap_or("?");

                                    println!(
                                        "playing: {} - {} [{} • {}]",
                                        meta.artist, meta.title, year, label
                                    );
                                } else {
                                    println!("playing unknown track: {:?}", &path);
                                }

                                player.play(&path);
                            }

                            // Scanner failed mid-operation
                            Err(e) => {
                                eprintln!("qr scanner error: {e}");
                                shutdown(player, scanner);
                                bail!("qr scanner failed");
                            }
                        }
                    }
                }
    }

    println!("card player stopped");
    Ok(())
}

/// Extracts track id from QR/card text.
///
/// Accepts:
/// - raw hash:
///     abc123
///
/// - full URL:
///     https://example.com/play?h=abc123
fn extract_trackid(text: &str) -> Result<TrackId, String> {
    let text = text.trim();

    // -----------------------------------------
    // Full URL:
    // https://example.com/play?h=abc123
    // -----------------------------------------
    if let Ok(url) = Url::parse(text) {
        if let Some(hash) = url
            .query_pairs()
            .find(|(k, _)| k == "h")
            .map(|(_, v)| v.to_string())
        {
            if let Ok(hash) = TrackId::from_hex(hash) {
                return Ok(hash);
            }
        }
    }

    // -----------------------------------------
    // raw hash:
    // abc123
    // -----------------------------------------
    TrackId::from_hex(text)
}
