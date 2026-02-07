use std::path::PathBuf;

#[cfg(not(target_os = "windows"))]
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

#[cfg(target_os = "windows")]
pub fn find_mount_by_label(label: &str) -> anyhow::Result<PathBuf> {
    for_windows::find_mount_by_label(label)
}

#[cfg(target_os = "windows")]
mod for_windows {
    use std::{
        ffi::OsString,
        os::windows::ffi::{OsStrExt, OsStringExt},
        path::PathBuf,
    };

    use anyhow::bail;
    use windows::{
        Win32::{
            Foundation::MAX_PATH,
            Storage::FileSystem::{GetLogicalDriveStringsW, GetVolumeInformationW},
        },
        core::PCWSTR,
    };

    pub(super) fn find_mount_by_label(label: &str) -> anyhow::Result<PathBuf> {
        for drive in get_all_drives_with_labels()? {
            if &drive.label == label {
                return Ok(drive.path);
            }
        }
        bail!("device '{label}' not mounted");
    }

    #[derive(Debug)]
    pub struct DriveInfo {
        pub path: PathBuf,
        pub label: String,
    }

    /// windows-specific function to get drive paths and labels
    fn get_all_drives_with_labels() -> Result<Vec<DriveInfo>, anyhow::Error> {
        let mut buffer: [u16; 256] = [0; 256];
        let len = unsafe { GetLogicalDriveStringsW(Some(&mut buffer)) };
        if len == 0 {
            bail!("Failed to get logical drives");
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
        use crate::storage::usb::for_windows::get_all_drives_with_labels;

        #[test]
        fn test_get_all_drives_windows() {
            let x = get_all_drives_with_labels().unwrap();
            for x in x {
                println!("{x:?}");
            }
        }
    }
}
