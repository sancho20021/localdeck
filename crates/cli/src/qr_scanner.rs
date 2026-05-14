use std::io::{BufRead, BufReader};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam::channel::{Receiver, SendError, Sender, unbounded};
use thiserror::Error;

const LINUX_PORT_NAME: &'static str =
    "/dev/serial/by-id/usb-TMS_Virtual_ComPort_in_FS_Mode_1234567890abcd-if00";
const BAUD_RATE: u32 = 9600;

#[derive(Debug, Error)]
pub enum QrScannerError {
    #[error("failed to open serial port: {0}")]
    PortOpen(String),
    #[error("failed to read from serial device: {0}")]
    ReadError(String),
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

pub fn start_qr_scanner() -> (Receiver<Result<String, QrScannerError>>, QrScanner) {
    let (tx, rx) = unbounded::<Result<String, QrScannerError>>();
    let (shutdown_tx, shutdown_rx) = unbounded::<()>();

    let handle = thread::spawn(move || {
        let port = match serialport::new(LINUX_PORT_NAME, BAUD_RATE)
            .timeout(Duration::from_millis(300))
            .open()
        {
            Ok(p) => p,
            Err(e) => {
                if let Err(e) = tx.send(Err(QrScannerError::PortOpen(e.to_string()))) {
                    log::error!("qr scanner thread: failed to send message: {e}");
                }
                return;
            }
        };
        log::info!("qr scanner: initialized serial port");

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
                    if let Err(e) = tx.send(Err(QrScannerError::ReadError(e.to_string()))) {
                        log::error!("qr scanner thread: failed to send error: {e}");
                    }
                    break;
                }
            }
        }
    });

    (
        rx,
        QrScanner {
            shutdown: shutdown_tx,
            handle,
        },
    )
}

pub fn print_qrs() {
    let (events, scanner) = start_qr_scanner();

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
