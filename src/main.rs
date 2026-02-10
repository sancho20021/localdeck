use crate::cli::run;

pub mod cli;
mod config;
pub mod domain;
pub mod http;
pub mod storage;
mod public_endpoint;

fn main() {
    run();
}
