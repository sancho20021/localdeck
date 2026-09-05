use std::io::{BufRead, BufReader};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam::channel::{Receiver, Sender, unbounded};
use thiserror::Error;
use url::Url;

/// Public so a caller can name the device it failed to reach.
pub const LINUX_PORT_NAME: &'static str =
    "/dev/serial/by-id/usb-TMS_Virtual_ComPort_in_FS_Mode_1234567890abcd-if00";
const BAUD_RATE: u32 = 9600;

/// The card id a scanned payload refers to.
///
/// Two forms are in circulation, because cards printed at different times encode
/// differently: a bare id, and a URL carrying it as the `h` query parameter.
/// Anything else is taken at face value and left to the library to reject.
pub fn extract_cardid(payload: &str) -> String {
    let payload = payload.trim();

    if let Ok(url) = Url::parse(payload) {
        if let Some(id) = url
            .query_pairs()
            .find(|(key, _)| key == "h")
            .map(|(_, value)| value.to_string())
        {
            return id;
        }
    }

    payload.to_string()
}

/// Whether the gun is plugged in.
///
/// Presence only: the port can still fail to open, when another process holds it
/// or the permissions are wrong.
pub fn is_connected() -> bool {
    std::path::Path::new(LINUX_PORT_NAME).exists()
}

#[derive(Debug, Error)]
#[error("failed to open serial port: {0}")]
pub struct PortOpenError(String);

/// The last thing a scanner ever sends: it stopped reading and its thread has
/// exited, so no further scans arrive on that channel.
///
/// `kind` is kept so a caller can tell an unplugged device from a misbehaving one
/// without matching on the message.
#[derive(Debug, Error)]
#[error("qr scanner stopped: {message}")]
pub struct ScannerStopped {
    pub kind: std::io::ErrorKind,
    pub message: String,
}

/// QR scanner handle that listens to scanned strings, and shutdowns on demand
pub struct QrScanner {
    shutdown: Sender<()>,
    handle: JoinHandle<()>,
}

impl QrScanner {
    /// Tries to shut down the scanning thread.
    ///
    /// Note that the thread may already be dead (for example due to failed read), in which case shutdown does nothing
    pub fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.handle.join();
    }
}

/// The port is opened here rather than on the scanning thread, so a caller that
/// cannot run without a scanner learns that before it commits to anything else.
pub fn start_qr_scanner()
-> Result<(Receiver<Result<String, ScannerStopped>>, QrScanner), PortOpenError> {
    let port = serialport::new(LINUX_PORT_NAME, BAUD_RATE)
        .timeout(Duration::from_millis(1000))
        .open()
        .map_err(|e| PortOpenError(e.to_string()))?;
    log::info!("qr scanner: initialized serial port");

    let (tx, rx) = unbounded::<Result<String, ScannerStopped>>();
    let (shutdown_tx, shutdown_rx) = unbounded::<()>();

    let handle = thread::spawn(move || {
        let mut reader = BufReader::new(port);

        loop {
            if shutdown_rx.try_recv().is_ok() {
                break;
            }

            let mut line = String::new();

            match reader.read_line(&mut line) {
                Ok(n) if n > 0 => {
                    let msg = line.trim().to_string();

                    if let Err(e) = tx.send(Ok(msg)) {
                        log::error!("qr scanner thread: failed to send read string: {e}");
                        break;
                    }
                }
                Ok(_) => {
                    log::debug!("qr scanner thread: no data");
                    continue;
                }
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    log::debug!("qr scanner thread: timeout");
                    continue;
                }
                Err(e) => {
                    let failure = ScannerStopped {
                        kind: e.kind(),
                        message: e.to_string(),
                    };
                    if let Err(e) = tx.send(Err(failure)) {
                        log::error!("qr scanner thread: failed to send error: {e}");
                    }
                    break;
                }
            }
        }
    });

    Ok((
        rx,
        QrScanner {
            shutdown: shutdown_tx,
            handle,
        },
    ))
}

pub fn print_qrs() {
    let (events, scanner) = match start_qr_scanner() {
        Ok(started) => started,
        Err(e) => {
            log::error!("{e}");
            return;
        }
    };

    for event in events {
        match event {
            Ok(qr) => println!("Got QR: {qr}"),

            Err(e) => {
                log::error!("{e}");
                scanner.shutdown();
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::extract_cardid;

    /// What the cards in circulation actually carry.
    #[test]
    fn a_bare_id_is_the_card_id() {
        assert_eq!(extract_cardid("003778873f9d0ca7"), "003778873f9d0ca7");
        assert_eq!(extract_cardid("1701"), "1701");
    }

    #[test]
    fn a_url_gives_up_its_h_parameter() {
        assert_eq!(
            extract_cardid("http://main-deck:8080/play?h=003778873f9d0ca7"),
            "003778873f9d0ca7"
        );
        assert_eq!(
            extract_cardid("https://example.com/play?h=1701&y=abc"),
            "1701"
        );
    }

    #[test]
    fn a_percent_encoded_value_is_decoded() {
        assert_eq!(extract_cardid("https://example.com/play?h=a%2Fb"), "a/b");
    }

    /// A URL without the parameter is not a card id hiding somewhere else, so it
    /// goes through whole and the library gets to reject it.
    #[test]
    fn a_url_without_h_is_left_alone() {
        let payload = "https://example.com/play?x=1701";
        assert_eq!(extract_cardid(payload), payload);
    }

    /// The gun's line ending survives as far as here.
    #[test]
    fn surrounding_whitespace_is_stripped() {
        assert_eq!(extract_cardid("  1701 \r\n"), "1701");
        assert_eq!(
            extract_cardid("  https://example.com/play?h=1701  "),
            "1701"
        );
    }
}
