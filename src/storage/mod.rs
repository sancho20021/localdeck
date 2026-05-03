use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use anyhow::Context;

use crate::{config::Location, storage::usb::find_mount_by_label};

pub mod db;
pub mod error;
mod fs;
pub mod operations;
pub(crate) mod schema;
pub mod usb;
