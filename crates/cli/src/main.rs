use crate::cli::run;

mod card_player;
pub mod cli;
mod config;
mod music_player;
mod qr_scanner;

fn main() {
    run().unwrap();
}
