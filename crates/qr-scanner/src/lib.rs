use std::io::{BufRead, BufReader};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam::channel::{Receiver, SendError, Sender, unbounded};
use thiserror::Error;

/// Public so a caller can name the device it failed to reach.
pub const LINUX_PORT_NAME: &'static str =
    "/dev/serial/by-id/usb-TMS_Virtual_ComPort_in_FS_Mode_1234567890abcd-if00";
const BAUD_RATE: u32 = 9600;

/// Whether the gun is plugged in.
///
/// Presence only: the port can still fail to open, when another process holds it
/// or the permissions are wrong.
pub fn is_connected() -> bool {
    std::path::Path::new(LINUX_PORT_NAME).exists()
}

#[derive(Debug, Error)]
pub enum QrScannerError {
    #[error("failed to open serial port: {0}")]
    PortOpen(String),
    /// `kind` is kept so a caller can tell an unplugged device from a
    /// misbehaving one without matching on the message.
    #[error("failed to read from serial device: {message}")]
    ReadError {
        kind: std::io::ErrorKind,
        message: String,
    },
    // #[error("invalid UTF-8 from device")]
    // Utf8Error,

    // #[error("failed to send message through channel")]
    // ChannelSend,

    // #[error(transparent)]
    // Io(#[from] io::Error),

    // #[error("serial port not found or disconnected")]
    // Disconnected,
    #[error("failed to send message through channel: {0}")]
    SendError(String),
}

impl<T> From<SendError<T>> for QrScannerError {
    fn from(value: SendError<T>) -> Self {
        Self::SendError(value.to_string())
    }
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
-> Result<(Receiver<Result<String, QrScannerError>>, QrScanner), QrScannerError> {
    let port = serialport::new(LINUX_PORT_NAME, BAUD_RATE)
        .timeout(Duration::from_millis(1000))
        .open()
        .map_err(|e| QrScannerError::PortOpen(e.to_string()))?;
    log::info!("qr scanner: initialized serial port");

    let (tx, rx) = unbounded::<Result<String, QrScannerError>>();
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
                    let failure = QrScannerError::ReadError {
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
