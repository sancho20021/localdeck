use std::path::PathBuf;

use anyhow::bail;

pub fn find_mount_by_label(label: &str) -> anyhow::Result<PathBuf> {
    let mounts = std::fs::read_to_string("/proc/self/mounts")?;

    for line in mounts.lines() {
        let parts: Vec<_> = line.split_whitespace().collect();
        if parts.len() >= 2 && parts[1].contains(label) {
            return Ok(PathBuf::from(parts[1]));
        }
    }

    bail!("device '{label}' not mounted");
}
