use crate::cli::run;

pub mod cli;
mod config;
pub mod domain;
pub mod server;
pub mod storage;

fn main() {
    run();
}
