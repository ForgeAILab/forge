//! Publish scripts without exposing a writable executable inode to other forks.
#[cfg(not(unix))]
use std::io::Write;
use std::{fs, io, path::Path, time::Duration};
#[cfg(unix)]
use std::{io::Write, process::Stdio, time::Instant};

/// The Unix writer process (`scripts/executable_fixture.py` runs the same
/// program). Kept in the crate: release images build from `crates/` alone.
/// `set -C` makes the redirection create the file exclusively, so an entry
/// planted at the staged path (a link included) fails the write instead of
/// being written through.
#[cfg(unix)]
const WRITER: &str = "umask 077\nset -C\nexec /bin/cat > \"$1\"\n";
/// A writer still running then is stopped, for example one whose staged path
/// became a FIFO that nobody reads.
const WRITER_TIMEOUT: Duration = Duration::from_secs(10);

/// Atomically publish an executable, without following an existing symlink.
///
/// On Unix the writer is a separate process: the multithreaded parent never
/// opens the staged inode for writing. Otherwise an unrelated concurrent fork
/// can inherit that writable descriptor until exec, and Linux rejects execution
/// with ETXTBSY even after the parent closes it and renames the file. Waiting for
/// the writer to exit closes every writable descriptor before publication. This
/// also protects scripts executed indirectly (for example Git hooks).
///
/// The staging directory sits beside `path`, which may be writable by
/// untrusted processes (a compiler-cache store is writable by its runs), so
/// neither the writer nor the parent follows a link or another entry planted
/// in it: such an entry makes the publication fail.
pub fn write_executable(path: &Path, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let staging = tempfile::Builder::new()
        .prefix(".forge-executable-")
        .tempdir_in(parent)?;
    let staged = staging.path().join("script");
    write_staged(&staged, contents.as_ref(), WRITER_TIMEOUT)?;
    publish_staged(&staged, path)
}

/// Create `staged` exclusively with `contents`, from a separate process.
#[cfg(unix)]
fn write_staged(staged: &Path, contents: &[u8], timeout: Duration) -> io::Result<()> {
    let mut writer = std::process::Command::new("/bin/sh")
        .args(["-c", WRITER, "forge-write-executable"])
        .arg(staged)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdin = writer.stdin.take().expect("piped stdin");
    std::thread::scope(|scope| {
        // Fed beside the deadline: stopping a stuck writer ends a blocked feed.
        let feed = scope.spawn(move || stdin.write_all(contents));
        let deadline = Instant::now() + timeout;
        let status = loop {
            match writer.try_wait() {
                Ok(Some(status)) => break Ok(status),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(None) => {
                    let _ = writer.kill();
                    let _ = writer.wait();
                    break Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "executable writer did not finish",
                    ));
                }
                Err(error) => {
                    let _ = writer.kill();
                    let _ = writer.wait();
                    break Err(error);
                }
            }
        };
        let fed = feed.join().expect("the writer feed does not panic");
        if !status?.success() {
            return Err(io::Error::other("executable writer failed"));
        }
        fed
    })
}

#[cfg(not(unix))]
fn write_staged(staged: &Path, contents: &[u8], _timeout: Duration) -> io::Result<()> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(staged)?
        .write_all(contents)
}

/// fsync, mark executable and rename the staged file over `path`, through a
/// read-only descriptor of the staged inode itself: never through a link,
/// never waiting on a FIFO, never on an inode also linked elsewhere.
#[cfg(unix)]
fn publish_staged(staged: &Path, path: &Path) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(staged)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the staged executable was replaced",
        ));
    }
    file.sync_all()?;
    file.set_permissions(fs::Permissions::from_mode(0o755))?;
    drop(file);
    fs::rename(staged, path)
}

#[cfg(not(unix))]
fn publish_staged(staged: &Path, path: &Path) -> io::Result<()> {
    // Windows requires a writable handle for FlushFileBuffers.
    fs::OpenOptions::new()
        .write(true)
        .open(staged)?
        .sync_all()?;
    fs::rename(staged, path)
}

fn retry_delay(error: &io::Error, attempt: u32) -> Option<Duration> {
    (error.kind() == io::ErrorKind::ExecutableFileBusy && attempt < 5)
        .then(|| Duration::from_millis(10 << attempt))
}

fn retry_spawn<T>(mut spawn: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    for attempt in 0.. {
        match spawn() {
            Err(error) => match retry_delay(&error, attempt) {
                Some(delay) => std::thread::sleep(delay),
                None => return Err(error),
            },
            result => return result,
        }
    }
    unreachable!()
}

async fn retry_spawn_async<T>(mut spawn: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    for attempt in 0.. {
        match spawn() {
            Err(error) => match retry_delay(&error, attempt) {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return Err(error),
            },
            result => return result,
        }
    }
    unreachable!()
}

/// Retry only ETXTBSY spawn errors, at most five times (310 ms total delay).
/// A successful spawn, nonzero exit, or any other error is never retried.
pub fn spawn_std(command: &mut std::process::Command) -> io::Result<std::process::Child> {
    retry_spawn(|| command.spawn())
}

/// Async equivalent of [`spawn_std`]; waiting does not block the runtime.
pub async fn spawn(command: &mut tokio::process::Command) -> io::Result<tokio::process::Child> {
    retry_spawn_async(|| command.spawn()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_spawn_retry_is_bounded_and_only_retries_text_file_busy() {
        let mut calls = 0;
        assert_eq!(
            retry_spawn(|| {
                calls += 1;
                if calls < 3 {
                    Err(io::ErrorKind::ExecutableFileBusy.into())
                } else {
                    Ok(42)
                }
            })
            .unwrap(),
            42
        );
        assert_eq!(calls, 3);
        let mut calls = 0;
        let error = retry_spawn::<()>(|| {
            calls += 1;
            Err(io::ErrorKind::ExecutableFileBusy.into())
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ExecutableFileBusy);
        assert_eq!(calls, 6);
        let mut calls = 0;
        let error = retry_spawn::<()>(|| {
            calls += 1;
            Err(io::ErrorKind::PermissionDenied.into())
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn executable_async_spawn_retry_recovers_and_preserves_errors() {
        let mut calls = 0;
        retry_spawn_async(|| {
            calls += 1;
            if calls < 3 {
                Err(io::ErrorKind::ExecutableFileBusy.into())
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(calls, 3);
        let mut calls = 0;
        assert_eq!(
            retry_spawn_async::<()>(|| {
                calls += 1;
                Err(io::ErrorKind::ExecutableFileBusy.into())
            })
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::ExecutableFileBusy
        );
        assert_eq!(calls, 6);
        let mut calls = 0;
        assert_eq!(
            retry_spawn_async::<()>(|| {
                calls += 1;
                Err(io::ErrorKind::NotFound.into())
            })
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(calls, 1);
    }

    #[cfg(unix)]
    #[test]
    fn executable_publication_replaces_inode_without_following_links() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("script");
        let outside = temp.path().join("outside");
        fs::write(&outside, "untouched").unwrap();
        symlink(&outside, &path).unwrap();
        write_executable(&path, "#!/bin/sh\nprintf first\n").unwrap();
        assert_eq!(fs::read_to_string(&outside).unwrap(), "untouched");
        let old = fs::File::open(&path).unwrap();
        write_executable(&path, "#!/bin/sh\nprintf second\n").unwrap();
        assert_eq!(
            std::io::read_to_string(old).unwrap(),
            "#!/bin/sh\nprintf first\n"
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o755
        );
        let output = std::process::Command::new(&path).output().unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"second");
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn executable_writer_never_writes_through_a_planted_entry() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let victim = temp.path().join("victim");
        fs::write(&victim, "untouched").unwrap();
        let linked = temp.path().join("linked");
        symlink(&victim, &linked).unwrap();
        let dangling = temp.path().join("dangling");
        symlink(temp.path().join("absent"), &dangling).unwrap();
        let existing = temp.path().join("existing");
        fs::write(&existing, "kept").unwrap();
        for staged in [&linked, &dangling, &existing] {
            assert!(
                write_staged(staged, b"#!/bin/sh\n", WRITER_TIMEOUT).is_err(),
                "{}",
                staged.display()
            );
        }
        assert_eq!(fs::read_to_string(&victim).unwrap(), "untouched");
        assert!(!temp.path().join("absent").exists());
        assert_eq!(fs::read_to_string(&existing).unwrap(), "kept");
    }

    #[cfg(unix)]
    #[test]
    fn executable_writer_stuck_on_a_fifo_is_stopped_at_its_deadline() {
        let temp = tempfile::tempdir().unwrap();
        let fifo = temp.path().join("fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let staged = temp.path().join("staged");
        std::os::unix::fs::symlink(&fifo, &staged).unwrap();
        let started = std::time::Instant::now();
        // More than a pipe buffer, so the feed itself blocks until the stop.
        let error =
            write_staged(&staged, &vec![b'x'; 1 << 20], Duration::from_millis(200)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[cfg(unix)]
    #[test]
    fn executable_publication_refuses_a_replaced_staged_entry() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let temp = tempfile::tempdir().unwrap();
        let victim = temp.path().join("victim");
        fs::write(&victim, "secret").unwrap();
        fs::set_permissions(&victim, fs::Permissions::from_mode(0o600)).unwrap();
        let linked = temp.path().join("linked");
        symlink(&victim, &linked).unwrap();
        let hard = temp.path().join("hard");
        fs::hard_link(&victim, &hard).unwrap();
        let fifo = temp.path().join("fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let path = temp.path().join("published");
        for staged in [&linked, &hard, &fifo] {
            assert!(
                publish_staged(staged, &path).is_err(),
                "{}",
                staged.display()
            );
            assert!(fs::symlink_metadata(staged).is_ok(), "{}", staged.display());
        }
        assert!(fs::symlink_metadata(&path).is_err());
        assert_eq!(
            fs::metadata(&victim).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(fs::read_to_string(&victim).unwrap(), "secret");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn executable_spawn_retry_waits_for_a_writable_descriptor_to_close() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("script");
        write_executable(&path, "#!/bin/sh\nexit 0\n").unwrap();
        let writer = fs::OpenOptions::new().write(true).open(&path).unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            drop(writer);
        });
        let mut child = spawn_std(&mut std::process::Command::new(&path)).unwrap();
        assert!(child.wait().unwrap().success());
        release.join().unwrap();
    }
}
