use crate::daemon_service::{
    DaemonRegisterInput, DaemonReportInput, DaemonService, DetectedCliInput, RuntimeReportInput,
};
use crate::{Result, ServiceError};
use db::SqliteDb;
use events::EventBus;
use executors::{AdapterRegistry, AvailabilityStatus, ExecutorKind};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::process::Stdio;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use tokio::process::Command;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(60);
const VERSION_TIMEOUT: Duration = Duration::from_secs(2);
const CREDENTIALS_FILE: &str = "daemon_credentials.json";
const MAX_CREDENTIALS_BYTES: usize = 16 * 1024;
const TEMP_FILE_ATTEMPTS: usize = 8;

#[derive(Clone)]
pub struct EmbeddedDaemon {
    service: DaemonService,
    adapter_registry: Arc<AdapterRegistry>,
    forge_home: PathBuf,
    workspace_root: PathBuf,
    report_interval: Duration,
    stop_requested: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DaemonCredentials {
    daemon_id: String,
    token: String,
}

impl EmbeddedDaemon {
    pub async fn new(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        adapter_registry: Arc<AdapterRegistry>,
        forge_home: PathBuf,
        workspace_root: PathBuf,
    ) -> Result<Self> {
        Ok(Self::with_report_interval(
            db,
            event_bus,
            adapter_registry,
            forge_home,
            workspace_root,
            DEFAULT_REPORT_INTERVAL,
        ))
    }

    pub fn with_report_interval(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        adapter_registry: Arc<AdapterRegistry>,
        forge_home: PathBuf,
        workspace_root: PathBuf,
        report_interval: Duration,
    ) -> Self {
        Self {
            service: DaemonService::new(db, event_bus),
            adapter_registry,
            forge_home,
            workspace_root,
            report_interval,
            stop_requested: Arc::new(AtomicBool::new(false)),
            stop_notify: Arc::new(Notify::new()),
        }
    }

    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            while !self.stop_requested.load(Ordering::SeqCst) {
                if let Err(error) = self.scan_and_report().await {
                    tracing::warn!(%error, "embedded daemon report failed");
                }

                tokio::select! {
                    () = sleep(self.report_interval) => {}
                    () = self.stop_notify.notified() => {}
                }
            }
        })
    }

    pub fn stop(&self) {
        self.stop_requested.store(true, Ordering::SeqCst);
        self.stop_notify.notify_waiters();
    }

    async fn scan_and_report(&self) -> Result<()> {
        let credentials = self.register_or_load_credentials().await?;
        let detected_clis = self.detect_clis().await;
        let runtimes = vec![RuntimeReportInput {
            kind: "local".to_owned(),
            workspace_root: self.workspace_root().to_string_lossy().into_owned(),
            status: Some("ready".to_owned()),
        }];

        self.service
            .ingest_report(
                &credentials.daemon_id,
                DaemonReportInput {
                    detected_clis,
                    runtimes,
                    labels: None,
                    active_execution_ids: Some(Vec::new()),
                },
            )
            .await?;

        Ok(())
    }

    async fn register_or_load_credentials(&self) -> Result<DaemonCredentials> {
        if let Some(credentials) = self.read_credentials()? {
            if self
                .service
                .authenticate(&credentials.daemon_id, &credentials.token)
                .await
                .is_ok()
            {
                return Ok(credentials);
            }
        }

        let registration = self.service.register(self.registration_input()).await?;
        let credentials = DaemonCredentials {
            daemon_id: registration.daemon_id,
            token: registration.plaintext_token,
        };
        self.write_credentials(&credentials)?;
        Ok(credentials)
    }

    fn read_credentials(&self) -> Result<Option<DaemonCredentials>> {
        let path = self.credentials_path();
        let Some(contents) = read_private_file(&path, MAX_CREDENTIALS_BYTES)? else {
            return Ok(None);
        };
        let credentials = serde_json::from_slice(&contents).map_err(|_| {
            ServiceError::invalid_operation("embedded daemon credentials file is invalid")
        })?;
        Ok(Some(credentials))
    }

    fn write_credentials(&self, credentials: &DaemonCredentials) -> Result<()> {
        ensure_directory(&self.forge_home)?;
        let path = self.credentials_path();
        // Reject a pre-existing directory or symlink before staging a new
        // credential file.  The final rename never follows a destination
        // symlink, but observing one here is still a fail-closed condition.
        let _ = inspect_sensitive_leaf(&path)?;
        let contents = serde_json::to_vec_pretty(credentials).map_err(|_| {
            ServiceError::invalid_operation("embedded daemon credentials could not be encoded")
        })?;
        if contents.len() > MAX_CREDENTIALS_BYTES {
            return Err(ServiceError::invalid_operation(
                "embedded daemon credentials file is too large",
            ));
        }

        let temporary_path = write_private_temp_file(&self.forge_home, &path, &contents)?;
        let result = replace_file_atomically(&temporary_path, &path, &self.forge_home);
        if result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    }

    fn registration_input(&self) -> DaemonRegisterInput {
        let hostname = local_hostname();
        DaemonRegisterInput {
            machine_id: embedded_machine_id(),
            hostname,
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            agent_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            labels: json!({ "mode": "embedded" }),
            runtimes: vec![RuntimeReportInput {
                kind: "local".to_owned(),
                workspace_root: self.workspace_root().to_string_lossy().into_owned(),
                status: Some("ready".to_owned()),
            }],
            owner_id: None,
            visibility: Some("global".to_owned()),
        }
    }

    async fn detect_clis(&self) -> Vec<DetectedCliInput> {
        let mut detected = Vec::new();
        for kind in self.adapter_registry.kinds() {
            let Some(adapter) = self.adapter_registry.get(&kind) else {
                continue;
            };
            let availability = adapter.check_availability();
            let (path, version) = cli_path_and_version(&kind).await;
            detected.push(DetectedCliInput {
                kind: kind.to_string(),
                availability: availability_status(&availability.status).to_owned(),
                config_path: availability.config_path,
                version,
                path,
            });
        }
        detected
    }

    fn credentials_path(&self) -> PathBuf {
        self.forge_home.join(CREDENTIALS_FILE)
    }

    fn workspace_root(&self) -> PathBuf {
        self.workspace_root.clone()
    }
}

pub fn embedded_machine_id() -> String {
    format!(
        "embedded:{}:{}:{}",
        local_hostname(),
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}

pub fn is_embedded_daemon_machine(machine_id: &str) -> bool {
    machine_id == embedded_machine_id()
}

async fn cli_path_and_version(kind: &ExecutorKind) -> (Option<String>, Option<String>) {
    match kind {
        // Forge-hosted native profiles do not have a CLI binary or daemon
        // detection record. They are dispatched by the Task executor router.
        ExecutorKind::Embedded => (None, None),
        ExecutorKind::Shell => (Some("/bin/sh".to_owned()), None),
        ExecutorKind::Codex => binary_path_and_version("codex").await,
        ExecutorKind::ClaudeCode => binary_path_and_version("claude").await,
        ExecutorKind::Cursor => binary_path_and_version("cursor-agent").await,
        ExecutorKind::Opencode => binary_path_and_version("opencode").await,
        ExecutorKind::Gemini => binary_path_and_version("gemini").await,
        ExecutorKind::Smith => binary_path_and_version("smith").await,
        ExecutorKind::Null => (None, None),
    }
}

async fn binary_path_and_version(binary: &str) -> (Option<String>, Option<String>) {
    let path = which::which(binary)
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    let version = if path.is_some() {
        binary_version(binary).await
    } else {
        None
    };
    (path, version)
}

async fn binary_version(binary: &str) -> Option<String> {
    let mut command = Command::new(binary);
    command
        .arg("--version")
        // Discovery runs alongside the terminal UI.  A CLI probe must never
        // inherit the controlling PTY and change its termios settings.
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let child = command.spawn().ok()?;
    let output = timeout(VERSION_TIMEOUT, child.wait_with_output())
        .await
        .ok()?
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
}

fn availability_status(status: &AvailabilityStatus) -> &'static str {
    match status {
        AvailabilityStatus::Authenticated => "authenticated",
        AvailabilityStatus::Installed => "installed",
        AvailabilityStatus::NotFound => "not_found",
    }
}

fn local_hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(command_hostname)
}

fn command_hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "localhost".to_owned())
}

fn read_private_file(path: &Path, max_bytes: usize) -> Result<Option<Vec<u8>>> {
    reject_symlink_components(path.parent().unwrap_or_else(|| Path::new(".")))?;

    let before = match inspect_sensitive_leaf(path)? {
        Some(metadata) => metadata,
        None => return Ok(None),
    };

    // Keep the bounded read tied to the descriptor that was opened.  The
    // path is checked again around the open/read to detect ordinary
    // replacement races; no credential bytes are ever included in errors.
    let mut file = File::open(path).map_err(|_| credential_file_error())?;
    let opened = file.metadata().map_err(|_| credential_file_error())?;
    validate_opened_file(&before, &opened, max_bytes)?;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    reject_symlink_components(parent)?;
    let after_open = inspect_sensitive_leaf(path)?.ok_or_else(credential_file_error)?;
    if !same_file(&after_open, &opened) {
        return Err(credential_file_error());
    }

    enforce_private_permissions(&file)?;

    let mut bytes = Vec::new();
    (&mut file)
        .take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| credential_file_error())?;
    if bytes.len() > max_bytes {
        return Err(ServiceError::invalid_operation(
            "embedded daemon credentials file is too large",
        ));
    }

    reject_symlink_components(parent)?;
    let after_read = inspect_sensitive_leaf(path)?.ok_or_else(credential_file_error)?;
    let final_metadata = file.metadata().map_err(|_| credential_file_error())?;
    if !same_file(&after_read, &final_metadata) {
        return Err(credential_file_error());
    }

    Ok(Some(bytes))
}

fn inspect_sensitive_leaf(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(ServiceError::invalid_operation(
            "embedded daemon credentials file must not be a symlink",
        )),
        Ok(metadata) if !metadata.is_file() => Err(ServiceError::invalid_operation(
            "embedded daemon credentials file must be a regular file",
        )),
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(credential_file_error()),
    }
}

fn validate_opened_file(
    before: &fs::Metadata,
    opened: &fs::Metadata,
    max_bytes: usize,
) -> Result<()> {
    if !opened.is_file() || !same_file(before, opened) {
        return Err(credential_file_error());
    }
    if opened.len() > (max_bytes as u64).saturating_add(1) {
        return Err(ServiceError::invalid_operation(
            "embedded daemon credentials file is too large",
        ));
    }
    Ok(())
}

fn ensure_directory(path: &Path) -> Result<()> {
    reject_symlink_components(path)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(ServiceError::invalid_operation(
            "embedded daemon credential directory must not be a symlink",
        )),
        Ok(metadata) if !metadata.is_dir() => Err(ServiceError::invalid_operation(
            "embedded daemon credential path must be a directory",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|_| credential_file_error())?;
            reject_symlink_components(path)?;
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
                _ => Err(ServiceError::invalid_operation(
                    "embedded daemon credential directory is invalid",
                )),
            }
        }
        Err(_) => Err(credential_file_error()),
    }
}

fn write_private_temp_file(parent: &Path, destination: &Path, contents: &[u8]) -> Result<PathBuf> {
    for attempt in 0..TEMP_FILE_ATTEMPTS {
        let temporary_path = temporary_path(destination, attempt);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let mut file = match options.open(&temporary_path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(credential_file_error()),
        };

        let result = (|| {
            enforce_private_permissions(&file)?;
            file.write_all(contents)
                .map_err(|_| credential_file_error())?;
            file.sync_all().map_err(|_| credential_file_error())?;
            enforce_private_permissions(&file)?;
            file.sync_all().map_err(|_| credential_file_error())
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

    let _ = parent;
    Err(credential_file_error())
}

fn replace_file_atomically(temp: &Path, destination: &Path, parent: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::rename(temp, destination).map_err(|_| credential_file_error())?;
        sync_directory(parent)?;
    }

    #[cfg(not(unix))]
    {
        match fs::rename(temp, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // Portable std has no replace-existing rename on Windows.
                // Removing the already-validated leaf before the rename keeps
                // symlinks from being followed, but the crash window is an
                // unavoidable residual until a platform API is introduced.
                match inspect_sensitive_leaf(destination)? {
                    Some(_) => fs::remove_file(destination).map_err(|_| credential_file_error())?,
                    None => {}
                }
                fs::rename(temp, destination).map_err(|_| credential_file_error())?;
            }
            Err(_) => return Err(credential_file_error()),
        }
        sync_directory(parent)?;
    }

    let _ = inspect_sensitive_leaf(destination)?.ok_or_else(credential_file_error)?;
    Ok(())
}

fn temporary_path(destination: &Path, attempt: usize) -> PathBuf {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(CREDENTIALS_FILE);
    destination.with_file_name(format!(
        ".{file_name}.tmp-{}-{attempt}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ))
}

fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata)
                if metadata.file_type().is_symlink() && !is_trusted_platform_alias(&current) =>
            {
                return Err(ServiceError::invalid_operation(
                    "embedded daemon credential path must not contain symlinks",
                ));
            }
            Ok(metadata) if !metadata.is_dir() && !is_trusted_platform_alias(&current) => {
                return Err(ServiceError::invalid_operation(
                    "embedded daemon credential parent path is not a directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(credential_file_error()),
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

fn enforce_private_permissions(file: &File) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = file.metadata().map_err(|_| credential_file_error())?;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| credential_file_error())?;
        }
        let metadata = file.metadata().map_err(|_| credential_file_error())?;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(ServiceError::invalid_operation(
                "embedded daemon credentials file permissions are not private",
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
        // descriptor still anchors the read; this fallback catches common
        // replacement races while retaining cross-platform compilation.
        path_metadata.is_file()
            && file_metadata.is_file()
            && path_metadata.len() == file_metadata.len()
            && path_metadata.permissions().readonly() == file_metadata.permissions().readonly()
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|_| credential_file_error())?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn credential_file_error() -> ServiceError {
    // Never propagate an OS/parser message from this path: credential tokens
    // must not appear in logs, terminal diagnostics, or public errors.
    ServiceError::invalid_operation("embedded daemon credentials file operation failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations};
    use std::path::Path;

    async fn test_db() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        Arc::new(SqliteDb::new(pool))
    }

    async fn test_daemon(forge_home: &Path) -> EmbeddedDaemon {
        EmbeddedDaemon::new(
            test_db().await,
            Arc::new(EventBus::new(16)),
            Arc::new(AdapterRegistry::new()),
            forge_home.to_path_buf(),
            forge_home.join("workspaces"),
        )
        .await
        .expect("embedded daemon creates")
    }

    #[tokio::test]
    async fn test_first_run_registers() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        let daemon = test_daemon(tempdir.path()).await;

        let credentials = daemon
            .register_or_load_credentials()
            .await
            .expect("registers");

        assert!(!credentials.daemon_id.is_empty());
        assert!(tempdir.path().join(CREDENTIALS_FILE).exists());
    }

    #[tokio::test]
    async fn test_second_run_loads() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        let daemon = test_daemon(tempdir.path()).await;

        let first = daemon
            .register_or_load_credentials()
            .await
            .expect("first register succeeds");
        let second = daemon
            .register_or_load_credentials()
            .await
            .expect("second load succeeds");

        assert_eq!(first.daemon_id, second.daemon_id);
    }

    #[tokio::test]
    async fn test_auth_failure_reregisters() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        let first_daemon = test_daemon(tempdir.path()).await;
        let first = first_daemon
            .register_or_load_credentials()
            .await
            .expect("first register succeeds");

        let second_daemon = test_daemon(tempdir.path()).await;
        let second = second_daemon
            .register_or_load_credentials()
            .await
            .expect("reregister succeeds");

        assert_ne!(first.daemon_id, second.daemon_id);
        assert!(tempdir.path().join(CREDENTIALS_FILE).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn credentials_symlink_is_rejected_without_reading_target() {
        use std::os::unix::fs::symlink;

        let outer = tempfile::tempdir().expect("temporary directory");
        let forge_home = outer.path().join("forge-home");
        fs::create_dir(&forge_home).expect("forge home creates");
        let target = outer.path().join("outside-credentials");
        let original = serde_json::to_vec(&DaemonCredentials {
            daemon_id: "daemon".to_owned(),
            token: "secret-token".to_owned(),
        })
        .expect("credentials encode");
        fs::write(&target, &original).expect("target writes");
        symlink(&target, forge_home.join(CREDENTIALS_FILE)).expect("symlink creates");

        let daemon = test_daemon(&forge_home).await;
        let error = daemon
            .read_credentials()
            .expect_err("credential symlink must be rejected");
        assert!(matches!(error, ServiceError::InvalidOperation { .. }));
        assert_eq!(fs::read(target).expect("target reads"), original);
    }

    #[test]
    fn credentials_nonregular_leaf_is_rejected() {
        let outer = tempfile::tempdir().expect("temporary directory");
        let forge_home = outer.path().join("forge-home");
        fs::create_dir(&forge_home).expect("forge home creates");
        fs::create_dir(forge_home.join(CREDENTIALS_FILE)).expect("credential directory creates");

        let error = inspect_sensitive_leaf(&forge_home.join(CREDENTIALS_FILE))
            .expect_err("credential directory must be rejected");
        assert!(matches!(error, ServiceError::InvalidOperation { .. }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn credentials_are_published_with_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let outer = tempfile::tempdir().expect("temporary directory");
        let daemon = test_daemon(outer.path()).await;
        daemon
            .write_credentials(&DaemonCredentials {
                daemon_id: "daemon".to_owned(),
                token: "secret-token".to_owned(),
            })
            .expect("credentials write");

        let mode = fs::metadata(outer.path().join(CREDENTIALS_FILE))
            .expect("credential metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[tokio::test]
    async fn oversized_credentials_are_rejected_before_publication() {
        let outer = tempfile::tempdir().expect("temporary directory");
        let daemon = test_daemon(outer.path()).await;
        let error = daemon
            .write_credentials(&DaemonCredentials {
                daemon_id: "daemon".to_owned(),
                token: "x".repeat(MAX_CREDENTIALS_BYTES),
            })
            .expect_err("oversized credentials must be rejected");

        assert!(matches!(error, ServiceError::InvalidOperation { .. }));
        assert!(!outer.path().join(CREDENTIALS_FILE).exists());
    }
}
