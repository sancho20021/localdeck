use url::Url;

use crate::{music_player::start_music_player, qr_scanner::start_qr_scanner};
use localdeck_storage::{TrackId, operations::Storage};

/// Starts:
/// - QR scanner thread
/// - audio player thread
///
/// Then continuously:
/// QR scan -> extract track id -> resolve path -> play
pub fn run_card_player(storage: &mut Storage) {
    let (qr_events, scanner) = start_qr_scanner();

    let (audio_errors, player) = start_music_player();

    // ---------------------------------------------------
    // Audio errors monitor thread
    // ---------------------------------------------------
    std::thread::spawn(move || {
        for e in audio_errors {
            log::error!("audio player error: {e}");
        }
    });

    // ---------------------------------------------------
    // Main QR processing loop
    // ---------------------------------------------------
    for event in qr_events {
        match event {
            // ---------------------------------------------
            // QR successfully scanned
            // ---------------------------------------------
            Ok(raw) => {
                log::info!("scanned qr: {raw}");

                let track_id = match extract_trackid(&raw) {
                    Ok(id) => id,

                    Err(e) => {
                        log::error!("invalid qr payload: {e}");
                        continue;
                    }
                };

                let (path, metadata) = {
                    match storage.find_track_file_with_meta(track_id) {
                        Ok((path, _, metadata)) => (path, metadata),

                        Err(e) => {
                            log::error!("could not resolve track {}: {}", track_id, e);

                            continue;
                        }
                    }
                };

                if let Some(meta) = &metadata {
                    let year = meta
                        .year
                        .map(|y| y.to_string())
                        .unwrap_or_else(|| "?".into());

                    let label = meta.label.as_deref().unwrap_or("?");

                    log::info!(
                        "playing: {} - {} [{} • {}]",
                        meta.artist,
                        meta.title,
                        year,
                        label
                    );
                } else {
                    log::info!("playing unknown track: {:?}", &path);
                }

                player.play(&path);
            }

            // ---------------------------------------------
            // Scanner failed
            // ---------------------------------------------
            Err(e) => {
                log::error!("qr scanner error: {e}");

                scanner.shutdown();

                player.shutdown();

                return;
            }
        }
    }

    log::info!("card player stopped");
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
