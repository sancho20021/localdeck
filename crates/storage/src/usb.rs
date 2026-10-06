use std::path::PathBuf;

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::location::Location;

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("no drive with label '{label}' is connected")]
    UsbNotFound { label: String },

    #[error("drive with label '{label}' ({}) is connected but not mounted", device.display())]
    UsbNotMounted { label: String, device: PathBuf },

    #[error("failed to query system mounts")]
    SystemQueryFail(#[from] std::io::Error),

    #[cfg(target_os = "windows")]
    #[error("usb resolve failed, windows-specific error: {0}")]
    WindowsError(String),
}

#[derive(Debug)]
struct UsbResolver {
    /// maps USB_LABEL -> path where it is mounted
    label_mounts: HashMap<String, PathBuf>,
    last_refresh: Instant,
    ttl: Duration,
}

impl UsbResolver {
    fn new(ttl: Duration) -> Self {
        Self {
            label_mounts: HashMap::new(),
            last_refresh: Instant::now() - ttl,
            ttl,
        }
    }

    /// Cached function to resolve location of USB with given label
    fn resolve_label(&mut self, label: &str) -> Result<PathBuf, ResolveError> {
        if self.last_refresh.elapsed() > self.ttl {
            self.reset();
        }

        if let Some(mount) = self.label_mounts.get(label) {
            return Ok(mount.clone());
        }

        let mount = find_mount_by_label(label)?;
        self.label_mounts.insert(label.to_string(), mount.clone());
        Ok(mount)
    }

    fn reset(&mut self) {
        self.label_mounts.clear();
        self.last_refresh = Instant::now();
    }
}

#[derive(Debug)]
/// Struct to resolve paths of locations
pub struct LocationResolver {
    usb_resolver: UsbResolver,
}

impl LocationResolver {
    pub fn new(ttl: Duration) -> Self {
        LocationResolver {
            usb_resolver: UsbResolver::new(ttl),
        }
    }

    #[cfg(test)]
    pub fn test_resolver(locs: impl IntoIterator<Item = (String, PathBuf)>) -> Self {
        LocationResolver {
            usb_resolver: UsbResolver {
                label_mounts: locs.into_iter().collect(),
                last_refresh: Instant::now(),
                ttl: Duration::from_secs(999),
            },
        }
    }

    /// Cached function to resolve path of given location
    /// Cache is getting cleared every `ttl` (see Self::new) so it can work if drive mounts get changed (for example, usb drive re-inserted)
    pub fn resolve(&mut self, loc: &Location) -> Result<PathBuf, ResolveError> {
        match loc {
            Location::File { path } => Ok(path.clone()),
            Location::Usb { label, path } => {
                let mount = self.usb_resolver.resolve_label(label)?;
                Ok(mount.join(path))
            }
        }
    }
}

impl Default for LocationResolver {
    fn default() -> Self {
        Self::new(Duration::from_secs(1))
    }
}

#[cfg(not(target_os = "windows"))]
pub fn find_mount_by_label(label: &str) -> Result<PathBuf, ResolveError> {
    let mounts = std::fs::read_to_string("/proc/self/mounts")?;

    if let Some(device) = device_for_label(label) {
        for line in mounts.lines() {
            let mut parts = line.split_whitespace();
            let (Some(src), Some(mount)) = (parts.next(), parts.next()) else {
                continue;
            };
            let src = std::fs::canonicalize(unescape_mounts(src)).unwrap_or_default();
            if src == device {
                return Ok(PathBuf::from(unescape_mounts(mount)));
            }
        }
        return Err(ResolveError::UsbNotMounted {
            label: label.to_string(),
            device,
        });
    }

    for line in mounts.lines() {
        let Some(mount) = line.split_whitespace().nth(1) else {
            continue;
        };
        let mount = PathBuf::from(unescape_mounts(mount));
        if mount.file_name().is_some_and(|name| name == label) {
            return Ok(mount);
        }
    }

    Err(ResolveError::UsbNotFound {
        label: label.to_string(),
    })
}

/// Device node a filesystem label points at, or `None` when udev has no such label.
#[cfg(not(target_os = "windows"))]
fn device_for_label(label: &str) -> Option<PathBuf> {
    std::fs::canonicalize(PathBuf::from("/dev/disk/by-label").join(udev_encode(label))).ok()
}

/// udev replaces every byte outside `[0-9A-Za-z#+-.:=@_]` with `\x<hex>` when it names
/// a link in `/dev/disk/by-label`.
#[cfg(not(target_os = "windows"))]
fn udev_encode(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for b in label.bytes() {
        if b.is_ascii_alphanumeric() || b"#+-.:=@_".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("\\x{b:02x}"));
        }
    }
    out
}

/// `/proc/self/mounts` writes space, tab, newline and backslash as three-digit octal escapes.
#[cfg(not(target_os = "windows"))]
fn unescape_mounts(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut rest = field;
    while let Some(i) = rest.find('\\') {
        out.push_str(&rest[..i]);
        let octal = rest.get(i + 1..i + 4).filter(|s| s.len() == 3);
        match octal.and_then(|s| u8::from_str_radix(s, 8).ok()) {
            Some(byte) => {
                out.push(byte as char);
                rest = &rest[i + 4..];
            }
            None => {
                out.push('\\');
                rest = &rest[i + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(target_os = "windows")]
pub fn find_mount_by_label(label: &str) -> Result<PathBuf, ResolveError> {
    for_windows::find_mount_by_label(label)
}

#[cfg(target_os = "windows")]
mod for_windows {
    use std::{
        ffi::OsString,
        os::windows::ffi::{OsStrExt, OsStringExt},
        path::PathBuf,
    };

    use windows::{
        Win32::{
            Foundation::MAX_PATH,
            Storage::FileSystem::{GetLogicalDriveStringsW, GetVolumeInformationW},
        },
        core::PCWSTR,
    };

    use crate::usb::ResolveError;

    pub(super) fn find_mount_by_label(label: &str) -> Result<PathBuf, ResolveError> {
        for drive in get_all_drives_with_labels()? {
            if &drive.label == label {
                return Ok(drive.path);
            }
        }
        Err(ResolveError::UsbNotFound {
            label: label.to_string(),
        })
    }

    #[derive(Debug)]
    pub struct DriveInfo {
        pub path: PathBuf,
        pub label: String,
    }

    /// windows-specific function to get drive paths and labels
    fn get_all_drives_with_labels() -> Result<Vec<DriveInfo>, ResolveError> {
        let mut buffer: [u16; 256] = [0; 256];
        let len = unsafe { GetLogicalDriveStringsW(Some(&mut buffer)) };
        if len == 0 {
            return Err(ResolveError::WindowsError(
                "Failed to get logical drives".to_string(),
            ));
        }

        let mut drives = Vec::new();
        let mut start = 0;

        while start < len as usize {
            let end = buffer[start..].iter().position(|&c| c == 0).unwrap() + start;

            let drive_path = OsString::from_wide(&buffer[start..end])
                .to_string_lossy()
                .to_string();
            start = end + 1;

            // Get volume label
            let mut vol_name: [u16; MAX_PATH as usize + 1] = [0; MAX_PATH as usize + 1];
            let drive_w: Vec<u16> = OsString::from(&drive_path)
                .encode_wide()
                .chain(Some(0))
                .collect();

            let success = unsafe {
                GetVolumeInformationW(
                    PCWSTR(drive_w.as_ptr()),
                    Some(&mut vol_name),
                    None,
                    None,
                    None,
                    None,
                )
            };

            if success.is_ok() {
                let label = OsString::from_wide(&vol_name)
                    .to_string_lossy()
                    .trim_end_matches('\0')
                    .to_string();
                drives.push(DriveInfo {
                    path: PathBuf::from(drive_path),
                    label,
                });
            }
        }

        Ok(drives)
    }

    #[cfg(test)]
    mod tests {
        use crate::usb::for_windows::get_all_drives_with_labels;

        #[test]
        fn test_get_all_drives_windows() {
            let x = get_all_drives_with_labels().unwrap();
            for x in x {
                println!("{x:?}");
            }
        }
    }
}

#[cfg(all(test, not(target_os = "windows")))]
mod unix_tests {
    use super::*;

    #[test]
    fn unescapes_octal_fields() {
        assert_eq!(unescape_mounts("/media/u/MY\\040DISK"), "/media/u/MY DISK");
        assert_eq!(unescape_mounts("/mnt/music"), "/mnt/music");
        assert_eq!(unescape_mounts("/mnt/a\\134b"), "/mnt/a\\b");
    }

    #[test]
    fn encodes_label_like_udev() {
        assert_eq!(udev_encode("MUSIC_DRIVE"), "MUSIC_DRIVE");
        assert_eq!(udev_encode("MY DISK"), "MY\\x20DISK");
    }

    #[test]
    #[ignore = "depends on the labels of the machine running the test"]
    fn finds_a_real_mount() {
        let label = std::env::var("LOCALDECK_TEST_LABEL").unwrap();
        println!("{label} -> {:?}", find_mount_by_label(&label));
    }
}
