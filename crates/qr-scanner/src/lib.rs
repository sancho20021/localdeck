use std::io::{BufRead, BufReader, ErrorKind};
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

/// What the gun sends over its life: any number of scans, then at most one
/// `Stopped`.
#[derive(Debug)]
pub enum ScanEvent {
    Scan(String),
    /// A card was in front of the gun and its payload could not be made out.
    /// Carries why. The gun is still reading, and the next card arrives normally.
    Unreadable(String),
    Stopped(ScannerStopped),
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
pub fn start_qr_scanner() -> Result<(Receiver<ScanEvent>, QrScanner), PortOpenError> {
    let port = serialport::new(LINUX_PORT_NAME, BAUD_RATE)
        .timeout(Duration::from_millis(1000))
        .open()
        .map_err(|e| PortOpenError(e.to_string()))?;
    log::info!("qr scanner: initialized serial port");

    let (tx, rx) = unbounded::<ScanEvent>();
    let (shutdown_tx, shutdown_rx) = unbounded::<()>();

    let handle = thread::spawn(move || drain(&mut BufReader::new(port), &tx, &shutdown_rx));

    Ok((
        rx,
        QrScanner {
            shutdown: shutdown_tx,
            handle,
        },
    ))
}

/// Reads scans out of the gun until it stops answering or `shutdown` fires.
///
/// A read that fails without costing a scan is retried here and the caller never
/// hears about it. Everything else it sends on.
fn drain(reader: &mut impl BufRead, tx: &Sender<ScanEvent>, shutdown: &Receiver<()>) {
    loop {
        if shutdown.try_recv().is_ok() {
            break;
        }

        let mut line = String::new();

        let event = match reader.read_line(&mut line) {
            Ok(n) if n > 0 => ScanEvent::Scan(line.trim().to_string()),
            Ok(_) => {
                log::debug!("qr scanner thread: no data");
                continue;
            }
            Err(e) if e.kind() == ErrorKind::TimedOut => {
                log::debug!("qr scanner thread: timeout");
                continue;
            }
            // No bytes are lost to a signal, so the retry is invisible.
            Err(e) if e.kind() == ErrorKind::Interrupted => {
                log::debug!("qr scanner thread: interrupted");
                continue;
            }
            // `read_line` consumes the line up to its newline before reporting it
            // as `InvalidData`, so the port is left on a line boundary and the
            // next read starts on a fresh payload.
            Err(e) if e.kind() == ErrorKind::InvalidData => {
                log::warn!("qr scanner thread: unreadable scan: {e}");
                ScanEvent::Unreadable(e.to_string())
            }
            Err(e) => ScanEvent::Stopped(ScannerStopped {
                kind: e.kind(),
                message: e.to_string(),
            }),
        };

        let last = matches!(event, ScanEvent::Stopped(_));

        if let Err(e) = tx.send(event) {
            log::error!("qr scanner thread: nobody is listening: {e}");
            break;
        }

        if last {
            break;
        }
    }
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
            ScanEvent::Scan(qr) => println!("Got QR: {qr}"),
            ScanEvent::Unreadable(why) => eprintln!("Unreadable scan: {why}"),
            ScanEvent::Stopped(e) => {
                log::error!("{e}");
                scanner.shutdown();
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read};

    use super::*;

    /// A gun that hands over `bytes` and is then unplugged, which serialport
    /// reports as `BrokenPipe`.
    struct Gun(Cursor<Vec<u8>>);

    impl Read for Gun {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match self.0.read(buf)? {
                0 => Err(io::Error::from(ErrorKind::BrokenPipe)),
                n => Ok(n),
            }
        }
    }

    /// Everything one gun sends over its life.
    fn read_until_hangup(bytes: &[u8]) -> Vec<ScanEvent> {
        let (tx, events) = unbounded();
        let (_shutdown, never) = unbounded();

        drain(
            &mut BufReader::new(Gun(Cursor::new(bytes.to_vec()))),
            &tx,
            &never,
        );

        drop(tx);
        events.try_iter().collect()
    }

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

    /// A line the gun garbles costs that scan and nothing else: it is reported,
    /// the card after it still arrives, and only the hangup ends the stream.
    #[test]
    fn a_line_that_is_not_utf8_costs_only_that_scan() {
        let events = read_until_hangup(b"1701\n\xff\xfe\n42\n");

        assert!(
            matches!(
                events.as_slice(),
                [
                    ScanEvent::Scan(first),
                    ScanEvent::Unreadable(_),
                    ScanEvent::Scan(second),
                    ScanEvent::Stopped(ScannerStopped {
                        kind: ErrorKind::BrokenPipe,
                        ..
                    }),
                ] if first.as_str() == "1701" && second.as_str() == "42"
            ),
            "{events:?}"
        );
    }
}
