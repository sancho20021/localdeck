use crossbeam::channel::{Receiver, Sender, unbounded};
use rodio::{
    Decoder, DeviceSinkBuilder, DeviceSinkError, DeviceTrait, MixerDeviceSink,
    cpal::traits::HostTrait,
};
use std::{
    fs::File,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};
use thiserror::Error;

/// Output to play audio from.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Output {
    /// default output (speakers / headphones)
    Default,
    /// device with given keyword present in its name
    Device(String),
}

#[derive(Debug, Error)]
pub enum AudioPlayerError {
    #[error(transparent)]
    DevicesError(#[from] DeviceSinkError),

    #[error("No available output devices with name {0} found")]
    NoDevices(String),

    #[error("No default output device found")]
    NoDefaultDevice,

    #[error("Audio device is not suitable for playback: {0}. Probably can't play any sound")]
    UnsupportedDevice(#[from] rodio::CpalError),

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

pub fn start_music_player(
    output: Output,
) -> Result<(Receiver<AudioPlayerError>, MusicPlayer), AudioPlayerError> {
    let (tx, rx): (Sender<PlayerCommand>, Receiver<PlayerCommand>) = unbounded();

    let (errors_tx, errors_rx) = unbounded::<AudioPlayerError>();

    let device = find_audio_device(output)?;
    let handle = thread::spawn(move || {
        let mut current_sink: Option<MixerDeviceSink> = None;

        for cmd in rx {
            match cmd {
                PlayerCommand::Play(path) => {
                    // Dropping the old sink instantly halts any previous track
                    current_sink.take();

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

                    let sink = (|| -> Result<_, _> {
                        DeviceSinkBuilder::from_device(device.clone())?.open_stream()
                    })();

                    match sink {
                        Ok(sink) => {
                            sink.mixer().add(source);
                            current_sink = Some(sink);
                        }
                        Err(e) => {
                            let _ = errors_tx.send(e.into());
                            return;
                        }
                    }
                }

                PlayerCommand::Stop => {
                    current_sink.take();
                }

                PlayerCommand::Shutdown => {
                    current_sink.take();
                    log::info!("audio player shutting down");
                    break;
                }
            }
        }
    });

    Ok((errors_rx, MusicPlayer { tx, handle }))
}

/// Finds a CPAL audio device containing the given target name case-insensitively,
/// ensuring the device explicitly supports output streams.
///
/// Returns an error if the hardware cannot be queried, or if no matching device is found.
/// If multiple devices match, it logs a warning and returns arbitrary one.
pub fn find_audio_device(output: Output) -> Result<rodio::cpal::Device, AudioPlayerError> {
    // Fetch the default platform audio host
    let host = rodio::cpal::default_host();
    let mut matched_devices = Vec::new();

    let target_name = match output {
        Output::Default => {
            return host
                .default_output_device()
                .ok_or(AudioPlayerError::NoDefaultDevice);
        }
        Output::Device(name) => name,
    };

    // Enumerate ALL devices available to the host
    let devices = host.devices()?;

    let normalized_target = target_name.to_lowercase();

    for device in devices.filter(|d| {
        if let Ok(mut configs) = d.supported_output_configs() {
            configs.next().is_some()
        } else {
            false
        }
    }) {
        let name = match device.description() {
            Ok(name) => name,
            Err(e) => {
                eprintln!("Failed to get device name: {e}");
                continue;
            }
        };

        log::debug!("Available output device: {name}");

        if name.to_string().to_lowercase().contains(&normalized_target) {
            matched_devices.push((name, device));
        }
    }

    let matched_device = if let Some(device) = matched_devices.pop() {
        device
    } else {
        return Err(AudioPlayerError::NoDevices(target_name.to_string()));
    };
    if matched_devices.is_empty() {
        log::info!(
            "Successfully matched unique audio output device: '{}'",
            matched_device.0
        );
    } else {
        log::warn!(
            "Other output devices also match target '{}': {:?}. Choosing match '{}'",
            target_name,
            matched_devices
                .iter()
                .map(|f| f.0.clone())
                .collect::<Vec<_>>(),
            matched_device.0
        );
    }

    Ok(matched_device.1)
}
