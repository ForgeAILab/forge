use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use crate::{error::ConfigError, ForgeConfig};

const MIN_JWT_SECRET_BYTES: usize = 32;
const MAX_JWT_SECRET_BYTES: usize = 64 * 1024;
const TEMP_FILE_ATTEMPTS: usize = 8;

impl ForgeConfig {
    /// Resolve the JWT signing secret used for auth tokens.
    ///
    /// Precedence:
    /// 1. `server.jwt_secret` from config (file, env, or CLI override)
    /// 2. Existing file at [`ForgeConfig::jwt_secret_path`]
    /// 3. Generate a cryptographically random secret, persist it, and use it
    pub fn resolve_jwt_secret(&self) -> Result<Vec<u8>, ConfigError> {
        if let Some(secret) = &self.server.jwt_secret {
            if secret.is_empty() {
                return Err(ConfigError::InvalidConfig {
                    message: "server.jwt_secret cannot be empty".to_owned(),
                });
            }
            return Ok(secret.as_bytes().to_vec());
        }

        let path = self.jwt_secret_path();
        if let Some(bytes) = read_jwt_secret_file(&path)? {
            return Ok(bytes);
        }

        let secret = generate_jwt_secret();
        match persist_jwt_secret(&path, &secret) {
            Ok(()) => Ok(secret),
            Err(ConfigError::Write { source, .. })
                if source.kind() == io::ErrorKind::AlreadyExists =>
            {
                // Another process may have won the publication race.  Read
                // its complete, validated secret instead of returning a
                // value that was never installed at the canonical path.
                read_jwt_secret_file(&path)?.ok_or_else(|| {
                    jwt_secret_invalid_error("JWT secret disappeared during publication")
                })
            }
            Err(error) => Err(error),
        }
    }
}

fn validate_jwt_secret_bytes(bytes: &[u8]) -> Result<(), ConfigError> {
    if bytes.len() < MIN_JWT_SECRET_BYTES {
        return Err(jwt_secret_invalid_error(
            "JWT secret file is shorter than the minimum length",
        ));
    }
    if bytes.len() > MAX_JWT_SECRET_BYTES {
        return Err(jwt_secret_invalid_error("JWT secret file is too large"));
    }
    Ok(())
}

fn generate_jwt_secret() -> Vec<u8> {
    let mut secret = vec![0_u8; MIN_JWT_SECRET_BYTES];
    rand::fill(&mut secret[..]);
    secret
}

fn persist_jwt_secret(path: &Path, secret: &[u8]) -> Result<(), ConfigError> {
    validate_jwt_secret_bytes(secret)?;

    if let Some(parent) = path.parent() {
        reject_symlink_components(parent)?;
        fs::create_dir_all(parent).map_err(|source| ConfigError::Write {
            path: parent.to_path_buf(),
            source: sanitized_io_error(source.kind()),
        })?;
        reject_symlink_components(parent)?;
    }

    let parent = path
        .parent()
        .ok_or_else(|| jwt_secret_write_error(path, io::ErrorKind::InvalidInput))?;
    let temporary_path = write_private_temp_file(path, secret)?;

    let publication = (|| {
        // Hard-link creation is atomic and never replaces an existing
        // destination.  This gives concurrent first runs a single winner and
        // prevents a partially-written or attacker-selected destination from
        // being truncated.
        fs::hard_link(&temporary_path, path)
            .map_err(|source| jwt_secret_write_error(path, source.kind()))?;
        sync_directory(parent).map_err(|source| jwt_secret_write_error(path, source.kind()))?;
        fs::remove_file(&temporary_path)
            .map_err(|source| jwt_secret_write_error(path, source.kind()))?;
        sync_directory(parent).map_err(|source| jwt_secret_write_error(path, source.kind()))
    })();

    if publication.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    publication
}

fn read_jwt_secret_file(path: &Path) -> Result<Option<Vec<u8>>, ConfigError> {
    let parent = path
        .parent()
        .ok_or_else(|| jwt_secret_read_error(path, io::ErrorKind::InvalidInput))?;
    reject_symlink_components(parent)?;

    let before = match inspect_jwt_secret_leaf(path)? {
        Some(metadata) => metadata,
        None => return Ok(None),
    };

    // Keep the read tied to this descriptor.  Path metadata is checked both
    // before and after opening to detect ordinary replacement races; the
    // descriptor remains authoritative for the bounded byte read itself.
    let mut file = File::open(path).map_err(|source| jwt_secret_read_error(path, source.kind()))?;
    let opened = file
        .metadata()
        .map_err(|source| jwt_secret_read_error(path, source.kind()))?;
    validate_opened_jwt_secret(&before, &opened)?;

    reject_symlink_components(parent)?;
    let after_open = inspect_jwt_secret_leaf(path)?
        .ok_or_else(|| jwt_secret_invalid_error("JWT secret changed while it was being opened"))?;
    if !same_file(&after_open, &opened) {
        return Err(jwt_secret_invalid_error(
            "JWT secret changed while it was being opened",
        ));
    }

    enforce_private_permissions(&file, path)?;

    let mut bytes = Vec::new();
    (&mut file)
        .take((MAX_JWT_SECRET_BYTES as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| jwt_secret_read_error(path, source.kind()))?;
    validate_jwt_secret_bytes(&bytes)?;

    reject_symlink_components(parent)?;
    let after_read = inspect_jwt_secret_leaf(path)?
        .ok_or_else(|| jwt_secret_invalid_error("JWT secret changed while it was being read"))?;
    let final_metadata = file
        .metadata()
        .map_err(|source| jwt_secret_read_error(path, source.kind()))?;
    if !same_file(&after_read, &final_metadata) {
        return Err(jwt_secret_invalid_error(
            "JWT secret changed while it was being read",
        ));
    }

    Ok(Some(bytes))
}

fn inspect_jwt_secret_leaf(path: &Path) -> Result<Option<fs::Metadata>, ConfigError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(jwt_secret_invalid_error(
            "JWT secret file must not be a symlink",
        )),
        Ok(metadata) if !metadata.is_file() => Err(jwt_secret_invalid_error(
            "JWT secret file must be a regular file",
        )),
        Ok(metadata) => Ok(Some(metadata)),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(jwt_secret_read_error(path, source.kind())),
    }
}

fn validate_opened_jwt_secret(
    before: &fs::Metadata,
    opened: &fs::Metadata,
) -> Result<(), ConfigError> {
    if !opened.is_file() {
        return Err(jwt_secret_invalid_error(
            "JWT secret file must be a regular file",
        ));
    }
    if !same_file(before, opened) {
        return Err(jwt_secret_invalid_error(
            "JWT secret changed while it was being opened",
        ));
    }
    if opened.len() > (MAX_JWT_SECRET_BYTES as u64).saturating_add(1) {
        return Err(jwt_secret_invalid_error("JWT secret file is too large"));
    }
    Ok(())
}

fn write_private_temp_file(destination: &Path, contents: &[u8]) -> Result<PathBuf, ConfigError> {
    for attempt in 0..TEMP_FILE_ATTEMPTS {
        let temporary_path = temporary_path(destination, attempt);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let mut file = match options.open(&temporary_path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(jwt_secret_write_error(destination, source.kind()));
            }
        };

        let result = (|| {
            enforce_private_permissions(&file, destination)?;
            file.write_all(contents)
                .map_err(|source| jwt_secret_write_error(destination, source.kind()))?;
            file.sync_all()
                .map_err(|source| jwt_secret_write_error(destination, source.kind()))?;
            enforce_private_permissions(&file, destination)?;
            file.sync_all()
                .map_err(|source| jwt_secret_write_error(destination, source.kind()))
        })();
        drop(file);

        match result {
            Ok(()) => return Ok(temporary_path),
            Err(error) => {
                let _ = fs::remove_file(&temporary_path);
                return Err(error);
            }
        }
    }

    Err(jwt_secret_write_error(
        destination,
        io::ErrorKind::AlreadyExists,
    ))
}

fn temporary_path(destination: &Path, attempt: usize) -> PathBuf {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("jwt_secret.bin");
    destination.with_file_name(format!(
        ".{file_name}.tmp-{}-{attempt}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ))
}

fn reject_symlink_components(path: &Path) -> Result<(), ConfigError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata)
                if metadata.file_type().is_symlink() && !is_trusted_platform_alias(&current) =>
            {
                return Err(jwt_secret_invalid_error(
                    "JWT secret parent directory must not contain symlinks",
                ));
            }
            Ok(metadata) if !metadata.is_dir() && !is_trusted_platform_alias(&current) => {
                return Err(jwt_secret_invalid_error(
                    "JWT secret parent path must contain only directories",
                ));
            }
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => return Err(jwt_secret_read_error(path, source.kind())),
        }
    }
    Ok(())
}

fn is_trusted_platform_alias(path: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        matches!(path.to_str(), Some("/var" | "/tmp" | "/etc"))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        false
    }
}

fn enforce_private_permissions(file: &File, path: &Path) -> Result<(), ConfigError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = file
            .metadata()
            .map_err(|source| jwt_secret_read_error(path, source.kind()))?;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|source| jwt_secret_write_error(path, source.kind()))?;
        }
        let metadata = file
            .metadata()
            .map_err(|source| jwt_secret_read_error(path, source.kind()))?;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(jwt_secret_invalid_error(
                "JWT secret file permissions must be private",
            ));
        }
    }
    Ok(())
}

fn same_file(path_metadata: &fs::Metadata, file_metadata: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        path_metadata.dev() == file_metadata.dev() && path_metadata.ino() == file_metadata.ino()
    }
    #[cfg(not(unix))]
    {
        // Stable std does not expose a portable Windows file identity.  The
        // descriptor still anchors the read; this fallback detects common
        // replacement races while retaining cross-platform compilation.
        path_metadata.is_file()
            && file_metadata.is_file()
            && path_metadata.len() == file_metadata.len()
            && path_metadata.permissions().readonly() == file_metadata.permissions().readonly()
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path).and_then(|file| file.sync_all())?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn sanitized_io_error(kind: io::ErrorKind) -> io::Error {
    io::Error::new(kind, "JWT secret file operation failed")
}

fn jwt_secret_read_error(path: &Path, kind: io::ErrorKind) -> ConfigError {
    ConfigError::Read {
        path: path.to_path_buf(),
        source: sanitized_io_error(kind),
    }
}

fn jwt_secret_write_error(path: &Path, kind: io::ErrorKind) -> ConfigError {
    ConfigError::Write {
        path: path.to_path_buf(),
        source: sanitized_io_error(kind),
    }
}

fn jwt_secret_invalid_error(message: &str) -> ConfigError {
    ConfigError::InvalidConfig {
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn config_for(data_dir: &Path) -> ForgeConfig {
        let mut config = ForgeConfig::default();
        config.forge.data_dir = data_dir.to_path_buf();
        config
    }

    #[cfg(unix)]
    #[test]
    fn rejects_jwt_secret_symlink_without_reading_target() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().expect("temporary directory");
        let target = directory.path().join("outside-secret");
        let secret_path = directory.path().join("jwt_secret.bin");
        let original = vec![7_u8; MIN_JWT_SECRET_BYTES];
        fs::write(&target, &original).expect("target writes");
        symlink(&target, &secret_path).expect("symlink creates");

        let error = config_for(directory.path())
            .resolve_jwt_secret()
            .expect_err("secret symlink must be rejected");
        assert!(matches!(error, ConfigError::InvalidConfig { .. }));
        assert_eq!(fs::read(&target).expect("target reads"), original);
    }

    #[test]
    fn rejects_nonregular_jwt_secret_leaf() {
        let directory = tempdir().expect("temporary directory");
        fs::create_dir(directory.path().join("jwt_secret.bin")).expect("directory creates");

        let error = config_for(directory.path())
            .resolve_jwt_secret()
            .expect_err("secret directory must be rejected");
        assert!(matches!(error, ConfigError::InvalidConfig { .. }));
    }

    #[test]
    fn rejects_oversized_jwt_secret_before_unbounded_read() {
        let directory = tempdir().expect("temporary directory");
        fs::write(
            directory.path().join("jwt_secret.bin"),
            vec![0_u8; MAX_JWT_SECRET_BYTES + 1],
        )
        .expect("oversized secret writes");

        let error = config_for(directory.path())
            .resolve_jwt_secret()
            .expect_err("oversized secret must be rejected");
        assert!(matches!(error, ConfigError::InvalidConfig { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn tightens_existing_jwt_secret_to_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("jwt_secret.bin");
        fs::write(&path, vec![9_u8; MIN_JWT_SECRET_BYTES]).expect("secret writes");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("permissive permissions set");

        config_for(directory.path())
            .resolve_jwt_secret()
            .expect("valid secret resolves");
        assert_eq!(
            fs::metadata(path)
                .expect("secret metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
