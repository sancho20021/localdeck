use crate::cli::run;

pub mod cli;
mod config;
mod location;
pub mod domain;
pub mod http;
pub mod storage;
mod public_endpoint;

fn main() {
    run().unwrap();
}
