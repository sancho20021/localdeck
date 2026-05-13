use crossbeam::channel::{Receiver, Sender, unbounded};
use rodio::{Decoder, DeviceSinkBuilder, Player};
use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AudioPlayerError {
    #[error("failed to open default audio device: {0}")]
    DeviceInit(String),

    #[error("failed to open audio file: {0}")]
    FileOpen(String),

    #[error("failed to decode audio: {0}")]
    Decode(String),
}

enum PlayerCommand {
    Play(PathBuf),
    Stop,
    Shutdown,
}

/// Music player. Plays music files
pub struct MusicPlayer {
    tx: Sender<PlayerCommand>,
    handle: JoinHandle<()>,
}

impl MusicPlayer {
    /// Plays the provided music file
    pub fn play(&self, path: &Path) {
        let _ = self.tx.send(PlayerCommand::Play(path.to_path_buf()));
    }

    /// Stops playing current song
    pub fn stop(&self) {
        let _ = self.tx.send(PlayerCommand::Stop);
    }

    /// Stops music player thread
    pub fn shutdown(self) {
        let _ = self.tx.send(PlayerCommand::Shutdown);
        let _ = self.handle.join();
    }
}

pub fn start_music_player() -> (Receiver<AudioPlayerError>, MusicPlayer) {
    let (tx, rx): (Sender<PlayerCommand>, Receiver<PlayerCommand>) = unbounded();

    let (errors_tx, errors_rx) = unbounded::<AudioPlayerError>();

    let handle = thread::spawn(move || {
        let device = match DeviceSinkBuilder::open_default_sink() {
            Ok(d) => d,

            Err(e) => {
                let _ = errors_tx.send(AudioPlayerError::DeviceInit(e.to_string()));
                return;
            }
        };

        let mut current_player: Option<Player> = None;

        for cmd in rx {
            match cmd {
                PlayerCommand::Play(path) => {
                    current_player.take();

                    let file = match File::open(&path) {
                        Ok(f) => f,

                        Err(e) => {
                            let _ = errors_tx.send(AudioPlayerError::FileOpen(e.to_string()));
                            continue;
                        }
                    };

                    let source = match Decoder::try_from(file) {
                        Ok(s) => s,

                        Err(e) => {
                            let _ = errors_tx.send(AudioPlayerError::Decode(e.to_string()));
                            continue;
                        }
                    };

                    let player = Player::connect_new(&device.mixer());

                    player.append(source);

                    current_player = Some(player);
                }

                PlayerCommand::Stop => {
                    current_player.take();
                }

                PlayerCommand::Shutdown => {
                    current_player.take();

                    log::info!("audio player shutting down");

                    break;
                }
            }
        }
    });

    (errors_rx, MusicPlayer { tx, handle })
}
