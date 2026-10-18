//! Race-aware bounded reads for files confined to an allowed root.

use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

/// Open and read a regular file within one allowed root while enforcing the
/// byte limit on the opened handle. The metadata comparison prevents a leaf
/// replacement between validation and open from being accepted, and callers
/// may require a single link so a hard link cannot become a read channel.
pub fn read_bounded_regular_file(
    path: &Path,
    allowed_root: &Path,
    max_bytes: u64,
    require_single_link: bool,
) -> Result<Option<Vec<u8>>, String> {
    let leaf_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    if !leaf_metadata.file_type().is_file() {
        return Err("not a regular file".to_owned());
    }

    let canonical_root = fs::canonicalize(allowed_root).map_err(|error| error.to_string())?;
    let canonical_path = fs::canonicalize(path).map_err(|error| error.to_string())?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err("path escapes its allowed root".to_owned());
    }
    let path_metadata = fs::symlink_metadata(&canonical_path).map_err(|error| error.to_string())?;
    if !path_metadata.file_type().is_file() {
        return Err("not a regular file".to_owned());
    }
    let file = File::open(&canonical_path).map_err(|error| error.to_string())?;
    let opened_metadata = file.metadata().map_err(|error| error.to_string())?;
    if !same_open_file(&path_metadata, &opened_metadata) {
        return Err("file changed while it was being opened".to_owned());
    }
    #[cfg(unix)]
    if require_single_link {
        use std::os::unix::fs::MetadataExt;
        if opened_metadata.nlink() != 1 {
            return Err("file must not be a hard link".to_owned());
        }
    }
    if opened_metadata.len() > max_bytes {
        return Err(format!("exceeds the {max_bytes} byte limit"));
    }

    let mut bytes = Vec::with_capacity(usize::try_from(opened_metadata.len()).unwrap_or(0));
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > max_bytes {
        return Err(format!("exceeds the {max_bytes} byte limit"));
    }
    Ok(Some(bytes))
}

#[cfg(unix)]
fn same_open_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_open_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}
