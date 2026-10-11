//! Removing and moving entries of a directory a Task can write, without
//! ever following a link.
//!
//! A Task with write access to a directory can replace any directory in it
//! by a link between the moment Forge looks and the moment Forge deletes. A
//! removal by path then goes through the link and deletes somewhere else.
//! Everything here walks down from one directory by file descriptor
//! (`openat` with `O_NOFOLLOW | O_DIRECTORY`, `unlinkat`, `renameat`): a
//! component that became a link is not opened, and an entry is removed from
//! the directory that was opened, wherever its name points now.

use std::{
    ffi::{CStr, CString, OsStr},
    io,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::{Component, Path},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

/// Entries removed between two looks at the deadline. A tree smaller than
/// this always goes in one call, so every pass makes progress.
const STRIDE: usize = 256;

/// Directory levels held open at once. A removal holds one descriptor per
/// open level (and one more while a level is listed), so this bounds what a
/// tree of any depth costs the process: a directory deeper than this is
/// first moved up to the top of the tree being removed and emptied from
/// there. (One level more can be open for a moment when an entry listed
/// as a file became a directory.)
const MAX_DEPTH: usize = 32;

/// Prefix of the name a too-deep directory is given at the top of its tree.
const FLATTENED_PREFIX: &str = ".forge-rm-";

fn c_name(name: &OsStr) -> io::Result<CString> {
    CString::new(name.as_bytes()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
}

/// Run a system call until it is not interrupted by a signal.
fn retry(mut call: impl FnMut() -> libc::c_int) -> io::Result<libc::c_int> {
    loop {
        let result = call();
        if result >= 0 {
            return Ok(result);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

const DIR_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;

/// Open the directory at `path`. The last component is never followed when
/// it is a link; the components before it are the caller's own.
fn open_dir(path: &Path) -> io::Result<OwnedFd> {
    let path = c_name(path.as_os_str())?;
    // SAFETY: `path` is a valid NUL-terminated string that outlives the call.
    let fd = retry(|| unsafe { libc::open(path.as_ptr(), DIR_FLAGS) })?;
    // SAFETY: `fd` was just returned by `open` and is owned by nobody else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Open the directory `name` inside `parent`; fails when `name` is a link
/// or anything else that is not a directory.
fn open_dir_at(parent: BorrowedFd<'_>, name: &CStr) -> io::Result<OwnedFd> {
    // SAFETY: `parent` is an open descriptor and `name` a valid
    // NUL-terminated string that outlives the call.
    let fd = retry(|| unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), DIR_FLAGS) })?;
    // SAFETY: `fd` was just returned by `openat` and is owned by nobody else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn unlink_at(parent: BorrowedFd<'_>, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    // SAFETY: `parent` is an open descriptor and `name` a valid
    // NUL-terminated string that outlives the call.
    retry(|| unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), flags) }).map(drop)
}

fn rename_at(
    from: BorrowedFd<'_>,
    name: &CStr,
    to: libc::c_int,
    target: &CStr,
) -> io::Result<()> {
    // SAFETY: `from` is an open descriptor, `to` an open descriptor or
    // `AT_FDCWD`, both names valid NUL-terminated strings that outlive the
    // call.
    retry(|| unsafe { libc::renameat(from.as_raw_fd(), name.as_ptr(), to, target.as_ptr()) })
        .map(drop)
}

/// What the open descriptor `fd` is.
fn stat_fd(fd: BorrowedFd<'_>) -> io::Result<libc::stat> {
    // SAFETY: `libc::stat` is plain data for which all-zero is a valid
    // value; `fd` is an open descriptor and the call only fills `stat` in.
    unsafe {
        let mut stat: libc::stat = std::mem::zeroed();
        retry(|| libc::fstat(fd.as_raw_fd(), &mut stat))?;
        Ok(stat)
    }
}

/// What the entry `name` of `parent` is. A link is reported as a link.
fn stat_at(parent: BorrowedFd<'_>, name: &CStr) -> io::Result<libc::stat> {
    // SAFETY: `libc::stat` is plain data for which all-zero is a valid
    // value; `parent` is an open descriptor, `name` a valid NUL-terminated
    // string, and the call only fills `stat` in.
    unsafe {
        let mut stat: libc::stat = std::mem::zeroed();
        retry(|| {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        })?;
        Ok(stat)
    }
}

fn is_dir(stat: &libc::stat) -> bool {
    stat.st_mode & libc::S_IFMT == libc::S_IFDIR
}

/// The mode that adds owner read, write and search to what `stat` has.
fn owner_writable(stat: &libc::stat) -> libc::mode_t {
    (stat.st_mode | 0o700) & 0o7777
}

/// What a directory listing says an entry is, when it says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Directory,
    Other,
    Unknown,
}

/// The names in the open directory `dir`, without `.` and `..`, each with
/// what the listing says it is.
fn names(dir: &OwnedFd) -> io::Result<Vec<(CString, Kind)>> {
    /// Closes the stream, and with it the descriptor it was given.
    struct Stream(*mut libc::DIR);
    impl Drop for Stream {
        fn drop(&mut self) {
            // SAFETY: the pointer came from `fdopendir` and is closed once.
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let copy = dir.try_clone()?;
    // SAFETY: `copy` is an open directory descriptor. `fdopendir` takes it
    // over only when it succeeds, so it is released to the stream after the
    // null check and dropped (closed) by `copy` otherwise.
    let stream = unsafe {
        let stream = libc::fdopendir(copy.as_raw_fd());
        if stream.is_null() {
            return Err(io::Error::last_os_error());
        }
        let _ = copy.into_raw_fd();
        Stream(stream)
    };
    let mut names = Vec::new();
    // SAFETY: `stream` is an open directory stream used by this thread
    // only. The entry `readdir` returns is valid until the next call on the
    // stream; its NUL-terminated name is copied out before that.
    unsafe {
        // The copy shares its position with `dir`.
        libc::rewinddir(stream.0);
        loop {
            let entry = libc::readdir(stream.0);
            if entry.is_null() {
                break;
            }
            let name = CStr::from_ptr((*entry).d_name.as_ptr());
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            let kind = match (*entry).d_type {
                libc::DT_DIR => Kind::Directory,
                libc::DT_UNKNOWN => Kind::Unknown,
                _ => Kind::Other,
            };
            names.push((name.to_owned(), kind));
        }
    }
    Ok(names)
}

/// Why a directory inside a tree is left where it is.
fn other_device() -> io::Error {
    io::Error::new(
        io::ErrorKind::CrossesDevices,
        "a directory on another filesystem is never entered",
    )
}

struct Frame {
    dir: OwnedFd,
    /// The name of `dir` in the frame below (in the tree's parent for the
    /// first frame).
    name: CString,
    pending: Vec<(CString, Kind)>,
}

impl Frame {
    /// Open the directory `name` of `parent` and list it. Only a real
    /// directory on the filesystem `device` is opened: never a link, never
    /// what is mounted inside the tree.
    fn open(parent: BorrowedFd<'_>, name: CString, device: libc::dev_t) -> io::Result<Self> {
        let dir = match open_dir_at(parent, &name) {
            Ok(dir) => dir,
            // A directory the owner cannot read: a run left it that way.
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                grant_owner_access_at(parent, &name, device);
                open_dir_at(parent, &name)?
            }
            Err(error) => return Err(error),
        };
        let stat = stat_fd(dir.as_fd())?;
        if stat.st_dev != device {
            return Err(other_device());
        }
        if stat.st_mode & 0o700 != 0o700 {
            // So its entries can be listed and removed. Through the
            // descriptor, never a path.
            // SAFETY: `dir` is an open descriptor.
            let _ = retry(|| unsafe { libc::fchmod(dir.as_raw_fd(), owner_writable(&stat)) });
        }
        let pending = names(&dir)?;
        Ok(Self { dir, name, pending })
    }
}

/// Give the owner access to the directory `name` in `parent`, which could
/// not be opened for reading. Never through a link, never a directory on
/// another filesystem than `device`.
fn grant_owner_access_at(parent: BorrowedFd<'_>, name: &CStr, device: libc::dev_t) {
    let Ok(stat) = stat_at(parent, name) else {
        return;
    };
    if is_dir(&stat) && stat.st_dev == device {
        let _ = chmod_dir_at(parent, name, owner_writable(&stat));
    }
}

/// Set the mode of the directory `name` in `parent`. Fails, and changes
/// nothing, when `name` is a link or not a directory.
///
/// Linux only: `fchmodat` cannot refuse a link there, so the entry itself
/// is opened without following it (`O_PATH` needs no permission on it;
/// with `O_DIRECTORY` a link or a file is refused) and the mode is changed
/// through that descriptor's `/proc/self/fd` name.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn chmod_dir_at(parent: BorrowedFd<'_>, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    const FLAGS: libc::c_int =
        libc::O_PATH | libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC;
    // SAFETY: `parent` is an open descriptor and `name` a valid
    // NUL-terminated string that outlives the call.
    let fd = retry(|| unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), FLAGS) })?;
    // SAFETY: `fd` was just returned by `openat` and is owned by nobody else.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let link = CString::new(format!("/proc/self/fd/{}", fd.as_raw_fd()))
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `link` is a valid NUL-terminated string that outlives the call.
    retry(|| unsafe { libc::chmod(link.as_ptr(), mode) }).map(drop)
}

/// Set the mode of the directory `name` in `parent`. Fails, and changes
/// nothing a link points at, when `name` is a link.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn chmod_dir_at(parent: BorrowedFd<'_>, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: `parent` is an open descriptor and `name` a valid
    // NUL-terminated string that outlives the call.
    retry(|| unsafe {
        libc::fchmodat(
            parent.as_raw_fd(),
            name.as_ptr(),
            mode,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })
    .map(drop)
}

/// Whether removing `name` as a file failed because it is a directory
/// (`EISDIR` on Linux, `EPERM` elsewhere).
fn is_a_directory(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::EISDIR | libc::EPERM))
}

/// Whether opening `name` as a directory failed because it is something
/// else (`ELOOP` for a link).
fn is_not_a_directory(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::ENOTDIR | libc::ELOOP))
}

/// What happened to one entry.
enum Entry {
    /// Gone (or was never there).
    Removed,
    /// A directory, opened and listed: to be emptied first.
    Directory(Frame),
    /// Could not be removed; it stays and the walk goes on past it.
    Left(io::Error),
}

/// Remove the entry `name` of `parent` when it is not a directory, open it
/// when it is one. What the listing said (`kind`) only picks what is tried
/// first: a file is never opened and a link never followed, whatever the
/// entry has become since.
fn remove_or_open(
    parent: BorrowedFd<'_>,
    name: &CStr,
    kind: Kind,
    device: libc::dev_t,
) -> Entry {
    let mut directory = match kind {
        Kind::Directory => true,
        Kind::Other => false,
        Kind::Unknown => match stat_at(parent, name) {
            Ok(stat) => is_dir(&stat),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Entry::Removed,
            Err(error) => return Entry::Left(error),
        },
    };
    let mut last = None;
    // Twice: the entry may have changed between the listing and here.
    for _ in 0..2 {
        let result = if directory {
            match Frame::open(parent, name.to_owned(), device) {
                Ok(frame) => return Entry::Directory(frame),
                Err(error) => Err(error),
            }
        } else {
            unlink_at(parent, name, 0)
        };
        match result {
            Ok(()) => return Entry::Removed,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Entry::Removed,
            Err(error) if directory && is_not_a_directory(&error) => {
                directory = false;
                last = Some(error);
            }
            Err(error) if !directory && is_a_directory(&error) => {
                directory = true;
                last = Some(error);
            }
            Err(error) => return Entry::Left(error),
        }
    }
    Entry::Left(last.unwrap_or_else(|| io::Error::from(io::ErrorKind::Other)))
}

/// What one walk did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Walked {
    /// `false` when the deadline passed first.
    finished: bool,
    /// The most directory levels held open at once.
    peak_depth: usize,
    /// Directories moved to the top of the tree because they were too deep.
    flattened: usize,
    /// Entries that could not be removed.
    left: usize,
}

impl Walked {
    /// Whether the walk finished, after noting what it had to leave.
    fn report(self, path: &Path) -> bool {
        if self.left > 0 || self.flattened > 0 {
            tracing::debug!(
                path = %path.display(),
                left = self.left,
                flattened = self.flattened,
                peak_depth = self.peak_depth,
                "tree removal left entries or met directories too deep to hold open"
            );
        }
        self.finished
    }
}

/// Remove the entry `name` of the open directory `parent` with everything
/// in it. See [`remove_tree`].
fn remove_entry_at(
    parent: BorrowedFd<'_>,
    name: &CStr,
    deadline: Option<Instant>,
    device: libc::dev_t,
) -> io::Result<Walked> {
    static FLATTENED: AtomicU64 = AtomicU64::new(0);
    let mut walked = Walked {
        finished: true,
        peak_depth: 0,
        flattened: 0,
        left: 0,
    };
    let mut stack = match remove_or_open(parent, name, Kind::Unknown, device) {
        Entry::Removed => return Ok(walked),
        Entry::Left(error) => return Err(error),
        Entry::Directory(frame) => vec![frame],
    };
    let mut removed = 0_usize;
    loop {
        let depth = stack.len();
        walked.peak_depth = walked.peak_depth.max(depth);
        let Some(frame) = stack.last_mut() else { break };
        let Some((child, kind)) = frame.pending.pop() else {
            let Some(done) = stack.pop() else { break };
            let above = stack.last().map_or(parent, |frame| frame.dir.as_fd());
            // Fails when an entry inside could not be removed; it then stays.
            if unlink_at(above, &done.name, libc::AT_REMOVEDIR).is_err() {
                walked.left += 1;
            }
            removed += 1;
            if !stack.is_empty() && out_of_time(removed, deadline) {
                walked.finished = false;
                break;
            }
            continue;
        };
        let dir = stack[depth - 1].dir.as_fd();
        let too_deep = depth >= MAX_DEPTH
            && match kind {
                Kind::Directory => true,
                Kind::Other => false,
                Kind::Unknown => stat_at(dir, &child).is_ok_and(|stat| is_dir(&stat)),
            };
        if too_deep {
            // No descriptor for another level: the directory moves (by
            // name, never followed) to the top of the tree and is emptied
            // from there, one level deep.
            let moved = CString::new(format!(
                "{FLATTENED_PREFIX}{}-{}",
                std::process::id(),
                FLATTENED.fetch_add(1, Ordering::Relaxed)
            ))
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            // Moving a directory to another parent needs write access to it.
            grant_owner_access_at(dir, &child, device);
            let top = stack[0].dir.as_fd();
            // A name already there is somebody else's: never replaced.
            let free = stat_at(top, &moved).is_err_and(|e| e.kind() == io::ErrorKind::NotFound);
            if free && rename_at(dir, &child, top.as_raw_fd(), &moved).is_ok() {
                walked.flattened += 1;
                stack[0].pending.insert(0, (moved, Kind::Unknown));
            } else {
                // Stays where it is; the walk goes on past it.
                walked.left += 1;
            }
            continue;
        }
        match remove_or_open(dir, &child, kind, device) {
            Entry::Directory(frame) => stack.push(frame),
            outcome => {
                if matches!(outcome, Entry::Left(_)) {
                    walked.left += 1;
                }
                removed += 1;
                if out_of_time(removed, deadline) {
                    walked.finished = false;
                    break;
                }
            }
        }
    }
    Ok(walked)
}

fn out_of_time(removed: usize, deadline: Option<Instant>) -> bool {
    removed.is_multiple_of(STRIDE) && deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

/// Remove the entry at `path` with everything in it, including directories
/// a run left read-only. Nothing is ever followed: a link, at `path` or
/// anywhere below it, is removed as the entry it is, also when it replaced
/// a directory while this ran. The directory that holds the entry is opened
/// once (it must be a real directory) and the entry removed relative to it;
/// a directory on another filesystem than that one is never entered.
///
/// `Ok(false)` when `deadline` passed first: what is left is still a tree
/// at `path` and the next call carries on. `Ok(true)` otherwise, whether or
/// not every entry could be removed (the caller looks whether `path` is
/// still there).
pub(crate) fn remove_tree(path: &Path, deadline: Option<Instant>) -> io::Result<bool> {
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let parent = open_dir(parent)?;
    let device = stat_fd(parent.as_fd())?.st_dev;
    remove_entry_at(parent.as_fd(), &c_name(name)?, deadline, device)
        .map(|walked| walked.report(path))
}

/// [`remove_tree`] for the entry `relative` under the directory `base`,
/// when directories between the two can be written by a Task: every one of
/// them is opened by descriptor and must be a real directory. Fails, and
/// removes nothing, when one is a link or `relative` is not made of plain
/// names.
pub(crate) fn remove_tree_under(
    base: &Path,
    relative: &Path,
    deadline: Option<Instant>,
) -> io::Result<bool> {
    let (parents, name) = split(relative)?;
    let base = open_dir(base)?;
    let device = stat_fd(base.as_fd())?.st_dev;
    let parent = descend(base, parents)?;
    remove_entry_at(parent.as_fd(), &name, deadline, device).map(|walked| walked.report(relative))
}

/// Open the directory reached from the open directory `dir` through
/// `components`, each of which must be a plain name of a real directory.
fn descend<'a>(
    mut dir: OwnedFd,
    components: impl IntoIterator<Item = Component<'a>>,
) -> io::Result<OwnedFd> {
    for component in components {
        let Component::Normal(name) = component else {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        };
        dir = open_dir_at(dir.as_fd(), &c_name(name)?)?;
    }
    Ok(dir)
}

/// `relative` as the plain names of the directories on the way and the
/// name of the entry. Fails for an absolute path and for `.` or `..`
/// anywhere in it.
fn split(relative: &Path) -> io::Result<(Vec<Component<'_>>, CString)> {
    let mut components: Vec<Component<'_>> = relative.components().collect();
    let Some(Component::Normal(name)) = components.pop() else {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    };
    if components
        .iter()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    Ok((components, c_name(name)?))
}

/// Delete the regular file `relative` under the directory `base`. Fails,
/// and deletes nothing, when `base` or any directory on the way is a link
/// (or is no longer a directory) or the entry is not a regular file.
pub(crate) fn remove_file_under(base: &Path, relative: &Path) -> io::Result<()> {
    let (parents, name) = split(relative)?;
    let dir = descend(open_dir(base)?, parents)?;
    if stat_at(dir.as_fd(), &name)?.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    unlink_at(dir.as_fd(), &name, 0)
}

/// Rename the entry `relative` under the directory `base` to `target`
/// (whose own directory is the caller's). Fails, and moves nothing, when
/// `base` or any directory on the way to the entry is a link.
pub(crate) fn rename_under(base: &Path, relative: &Path, target: &Path) -> io::Result<()> {
    let (parents, name) = split(relative)?;
    let dir = descend(open_dir(base)?, parents)?;
    let target = c_name(target.as_os_str())?;
    rename_at(dir.as_fd(), &name, libc::AT_FDCWD, &target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::symlink, os::unix::fs::PermissionsExt, time::Duration};

    fn outside(base: &Path) -> std::path::PathBuf {
        let outside = base.join("outside");
        fs::create_dir_all(outside.join("sub")).unwrap();
        fs::write(outside.join("sub").join("precious"), "keep").unwrap();
        fs::write(outside.join("precious"), "keep").unwrap();
        outside
    }

    /// A tree with links in it, to a file and to a directory outside, and
    /// with directories a run left read-only, is removed whole; what the
    /// links point at is untouched.
    #[test]
    fn remove_tree_removes_links_as_links_and_read_only_directories() {
        let dir = tempfile::tempdir().unwrap();
        let outside = outside(dir.path());
        let tree = dir.path().join("tree");
        fs::create_dir_all(tree.join("a").join("b")).unwrap();
        fs::write(tree.join("a").join("b").join("file"), "x").unwrap();
        symlink(&outside, tree.join("a").join("to-dir")).unwrap();
        symlink(outside.join("precious"), tree.join("to-file")).unwrap();
        fs::create_dir_all(tree.join("locked").join("inner")).unwrap();
        fs::write(tree.join("locked").join("inner").join("file"), "x").unwrap();
        for locked in [tree.join("locked").join("inner"), tree.join("locked")] {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        }

        assert!(remove_tree(&tree, None).unwrap());
        assert!(fs::symlink_metadata(&tree).is_err());
        assert_eq!(
            fs::read_to_string(outside.join("precious")).unwrap(),
            "keep"
        );
        assert_eq!(
            fs::read_to_string(outside.join("sub").join("precious")).unwrap(),
            "keep"
        );

        // A link where the tree was: removed as a link.
        symlink(&outside, &tree).unwrap();
        assert!(remove_tree(&tree, None).unwrap());
        assert!(fs::symlink_metadata(&tree).is_err() && outside.join("precious").exists());
        // Nothing there: nothing to do.
        assert!(remove_tree(&tree, None).unwrap());
    }

    /// The swap itself: a directory that was seen as a directory is a link
    /// by the time the walk opens it. It is not opened, so nothing behind
    /// it is listed or removed.
    #[test]
    fn a_directory_replaced_by_a_link_is_never_opened() {
        let dir = tempfile::tempdir().unwrap();
        let outside = outside(dir.path());
        let tree = dir.path().join("tree");
        fs::create_dir_all(tree.join("victim")).unwrap();
        let parent = open_dir(&tree).unwrap();
        let listed = names(&parent).unwrap();
        assert_eq!(listed, [(c"victim".to_owned(), Kind::Directory)]);
        // Between the listing and the descent.
        fs::remove_dir(tree.join("victim")).unwrap();
        symlink(&outside, tree.join("victim")).unwrap();
        let device = stat_fd(parent.as_fd()).unwrap().st_dev;
        assert!(Frame::open(parent.as_fd(), c"victim".to_owned(), device).is_err());
        // What the listing said is only where to start: the link is
        // removed as a link.
        assert!(matches!(
            remove_or_open(parent.as_fd(), c"victim", Kind::Directory, device),
            Entry::Removed
        ));
        assert!(fs::symlink_metadata(tree.join("victim")).is_err());
        symlink(&outside, tree.join("victim")).unwrap();
        assert!(open_dir(&tree.join("victim")).is_err());
        assert!(remove_tree(&tree, None).unwrap());
        assert!(fs::symlink_metadata(&tree).is_err());
        assert!(outside.join("precious").exists() && outside.join("sub").join("precious").exists());
    }

    #[test]
    fn a_removal_cut_short_by_its_deadline_carries_on_in_the_next_call() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        fs::create_dir_all(&tree).unwrap();
        for index in 0..(STRIDE * 2) {
            fs::write(tree.join(index.to_string()), "x").unwrap();
        }
        let past = Instant::now() - Duration::from_secs(1);
        assert!(!remove_tree(&tree, Some(past)).unwrap());
        let left = fs::read_dir(&tree).unwrap().count();
        assert_eq!(left, STRIDE, "one stride went before the deadline was read");
        assert!(!remove_tree(&tree, Some(past)).unwrap());
        assert!(remove_tree(&tree, Some(past)).unwrap());
        assert!(fs::symlink_metadata(&tree).is_err());
    }

    /// A file is deleted, and an entry moved, only when every directory
    /// between the base and it is a real directory.
    #[test]
    fn a_file_or_entry_behind_a_link_is_neither_deleted_nor_moved() {
        let dir = tempfile::tempdir().unwrap();
        let outside = outside(dir.path());
        let base = dir.path().join("base");
        fs::create_dir_all(base.join("sub")).unwrap();
        fs::write(base.join("sub").join("precious"), "mine").unwrap();
        symlink(&outside, base.join("linked")).unwrap();
        symlink(outside.join("precious"), base.join("sub").join("link")).unwrap();
        let moved = dir.path().join("moved");

        assert!(remove_file_under(&base, Path::new("linked/sub/precious")).is_err());
        assert!(remove_file_under(&base, Path::new("linked/precious")).is_err());
        assert!(
            remove_file_under(&base, Path::new("sub/link")).is_err(),
            "a link"
        );
        assert!(
            remove_file_under(&base, Path::new("sub")).is_err(),
            "a directory"
        );
        assert!(remove_file_under(&base, Path::new("../outside/precious")).is_err());
        assert!(rename_under(&base, Path::new("linked/sub"), &moved).is_err());
        assert!(rename_under(&dir.path().join("base/linked"), Path::new("sub"), &moved).is_err());
        assert!(!moved.exists());
        assert_eq!(
            fs::read_to_string(outside.join("precious")).unwrap(),
            "keep"
        );
        assert!(outside.join("sub").join("precious").exists());

        remove_file_under(&base, Path::new("sub/precious")).unwrap();
        assert!(!base.join("sub").join("precious").exists());
        rename_under(&base, Path::new("sub"), &moved).unwrap();
        assert!(moved.join("link").symlink_metadata().is_ok() && !base.join("sub").exists());
    }

    fn walk(path: &Path, deadline: Option<Instant>) -> Walked {
        let parent = open_dir(path.parent().unwrap()).unwrap();
        let device = stat_fd(parent.as_fd()).unwrap().st_dev;
        let name = c_name(path.file_name().unwrap()).unwrap();
        remove_entry_at(parent.as_fd(), &name, deadline, device).unwrap()
    }

    /// `levels` directories named `name` inside one another under `top`,
    /// made by descriptor so the whole path may be longer than a path can
    /// be. A file sits in every level.
    fn deep(top: &Path, name: &CStr, levels: usize) {
        let mut dir = open_dir(top).unwrap();
        for _ in 0..levels {
            // SAFETY: `dir` is an open descriptor, the names valid
            // NUL-terminated strings; the file descriptor is closed at once.
            unsafe {
                let file = libc::openat(
                    dir.as_raw_fd(),
                    c"file".as_ptr(),
                    libc::O_CREAT | libc::O_WRONLY | libc::O_CLOEXEC,
                    0o600 as libc::c_uint,
                );
                assert!(file >= 0);
                libc::close(file);
                assert_eq!(libc::mkdirat(dir.as_raw_fd(), name.as_ptr(), 0o700), 0);
            }
            dir = open_dir_at(dir.as_fd(), name).unwrap();
        }
    }

    /// A tree far deeper than the levels held open, and longer than any
    /// path, goes whole with a bounded number of descriptors: the deep
    /// directories are moved up, never reached through `..` or a path.
    #[test]
    fn a_tree_deeper_than_the_open_levels_is_removed_with_bounded_descriptors() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        fs::create_dir(&tree).unwrap();
        // 200 levels of a 40-byte name: an 8 KiB path.
        let name = CString::new("d".repeat(40)).unwrap();
        let levels = 200;
        deep(&tree, &name, levels);
        fs::write(tree.join("sibling"), "x").unwrap();

        let walked = walk(&tree, None);
        assert!(walked.finished);
        assert!(
            walked.peak_depth <= MAX_DEPTH + 1,
            "{} levels were open at once",
            walked.peak_depth
        );
        assert!(walked.flattened >= levels / (MAX_DEPTH + 1));
        assert_eq!(walked.left, 0);
        assert!(fs::symlink_metadata(&tree).is_err(), "the tree is gone");
        assert!(dir.path().exists());
    }

    /// Cut short by its deadline in the middle of a deep tree, a removal
    /// leaves one tree under the same path, and later calls finish it.
    #[test]
    fn a_deep_removal_cut_short_leaves_a_tree_the_next_call_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        fs::create_dir(&tree).unwrap();
        deep(&tree, c"d", MAX_DEPTH * 3);
        for index in 0..(STRIDE * 2) {
            fs::write(tree.join(format!("wide-{index}")), "x").unwrap();
        }
        let past = Some(Instant::now() - Duration::from_secs(1));
        let mut calls = 0;
        while !remove_tree(&tree, past).unwrap() {
            assert!(fs::symlink_metadata(&tree).unwrap().is_dir());
            assert_eq!(
                fs::read_dir(dir.path()).unwrap().count(),
                1,
                "nothing was moved out of the tree"
            );
            calls += 1;
            assert!(calls < 64, "every call makes progress");
        }
        assert!(calls >= 2);
        assert!(fs::symlink_metadata(&tree).is_err());
    }

    /// What is mounted inside a tree is on another filesystem: it is never
    /// opened, listed, made writable or emptied. (No test can mount, so the
    /// walk is told the tree's own filesystem is another one.)
    #[test]
    fn a_directory_on_another_filesystem_is_never_entered() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        fs::create_dir_all(tree.join("mounted").join("inner")).unwrap();
        fs::write(tree.join("mounted").join("inner").join("precious"), "keep").unwrap();
        fs::set_permissions(tree.join("mounted"), fs::Permissions::from_mode(0o500)).unwrap();
        let parent = open_dir(dir.path()).unwrap();
        let device = stat_fd(parent.as_fd()).unwrap().st_dev;
        let elsewhere = device.wrapping_add(1);

        // The whole tree is the mount: refused, nothing touched.
        let error = remove_entry_at(parent.as_fd(), c"tree", None, elsewhere).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::CrossesDevices);
        // A mount inside: left with everything in it, the rest goes on.
        let top = open_dir(&tree).unwrap();
        fs::write(tree.join("file"), "x").unwrap();
        assert!(matches!(
            remove_or_open(top.as_fd(), c"mounted", Kind::Directory, elsewhere),
            Entry::Left(_)
        ));
        assert!(matches!(
            remove_or_open(top.as_fd(), c"file", Kind::Other, elsewhere),
            Entry::Removed
        ));
        // An unreadable directory elsewhere is not made readable either.
        fs::set_permissions(tree.join("mounted"), fs::Permissions::from_mode(0o000)).unwrap();
        assert!(matches!(
            remove_or_open(top.as_fd(), c"mounted", Kind::Directory, elsewhere),
            Entry::Left(_)
        ));
        let mode = fs::symlink_metadata(tree.join("mounted")).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o000, "its mode was not changed");
        fs::set_permissions(tree.join("mounted"), fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            fs::read_to_string(tree.join("mounted").join("inner").join("precious")).unwrap(),
            "keep"
        );
    }

    /// Names are bytes: a newline, the longest name a directory takes and
    /// (where the filesystem stores them) bytes that are not UTF-8.
    #[test]
    fn names_are_bytes_not_text() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        fs::create_dir_all(tree.join("a\nb").join("c\n")).unwrap();
        fs::write(tree.join("a\nb").join("c\n").join("\n"), "x").unwrap();
        fs::create_dir(tree.join("n".repeat(255))).unwrap();
        fs::write(tree.join("n".repeat(255)).join("f".repeat(255)), "x").unwrap();
        // APFS refuses such a name; Linux filesystems keep it.
        let raw = tree.join(OsStr::from_bytes(b"\xff\xfe-not-utf8"));
        let made = fs::create_dir(&raw).is_ok() && fs::write(raw.join(OsStr::from_bytes(b"\xc3")), "x").is_ok();
        assert!(made || cfg!(target_os = "macos"));

        let walked = walk(&tree, None);
        assert_eq!((walked.finished, walked.left), (true, 0));
        assert!(fs::symlink_metadata(&tree).is_err());
    }

    /// Removing a hard link removes one name: the file and its other name
    /// outside the tree stay. A socket and a fifo are unlinked, never
    /// opened (opening the fifo would wait for a writer for ever).
    #[test]
    fn hard_links_sockets_and_fifos_are_unlinked_never_opened() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let outside = outside(dir.path());
        let tree = dir.path().join("tree");
        fs::create_dir_all(tree.join("sub")).unwrap();
        fs::hard_link(outside.join("precious"), tree.join("sub").join("linked")).unwrap();
        assert_eq!(fs::metadata(outside.join("precious")).unwrap().nlink(), 2);
        // A short relative name: a socket path has a small limit.
        let socket = std::os::unix::net::UnixListener::bind(tree.join("s"));
        let fifo = c_name(tree.join("sub").join("fifo").as_os_str()).unwrap();
        // SAFETY: `fifo` is a valid NUL-terminated string.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);

        let walked = walk(&tree, None);
        assert_eq!((walked.finished, walked.left), (true, 0));
        assert!(fs::symlink_metadata(&tree).is_err());
        let kept = fs::metadata(outside.join("precious")).unwrap();
        assert_eq!((kept.nlink(), kept.permissions().mode() & 0o777), (1, 0o644));
        assert_eq!(
            fs::read_to_string(outside.join("precious")).unwrap(),
            "keep"
        );
        drop(socket);
    }

    /// An entry that cannot be removed stays, with the directories above
    /// it; everything beside it goes, and the call reports no error.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_entry_that_cannot_be_removed_does_not_stop_its_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        fs::create_dir_all(tree.join("a").join("stuck")).unwrap();
        fs::create_dir_all(tree.join("b").join("c")).unwrap();
        for file in ["a/one", "a/stuck/pinned", "a/two", "b/c/three", "zz"] {
            fs::write(tree.join(file), "x").unwrap();
        }
        let pinned = c_name(tree.join("a/stuck/pinned").as_os_str()).unwrap();
        // SAFETY: `pinned` is a valid NUL-terminated string.
        assert_eq!(unsafe { libc::chflags(pinned.as_ptr(), libc::UF_IMMUTABLE) }, 0);

        let walked = walk(&tree, None);
        // SAFETY: as above.
        assert_eq!(unsafe { libc::chflags(pinned.as_ptr(), 0) }, 0);
        assert!(walked.finished);
        // The file, and the three directories that still hold it.
        assert_eq!(walked.left, 4);
        let mut left: Vec<_> = fs::read_dir(tree.join("a"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        left.sort();
        assert_eq!(left, ["stuck"]);
        assert_eq!(fs::read_dir(&tree).unwrap().count(), 1);
        assert!(tree.join("a/stuck/pinned").exists());
        assert!(remove_tree(&tree, None).unwrap());
        assert!(fs::symlink_metadata(&tree).is_err());
    }

    /// The directory that holds the tree is opened once and must be a real
    /// directory; a name that is not one plain name is refused.
    #[test]
    fn the_tree_is_removed_relative_to_its_real_parent_only() {
        let dir = tempfile::tempdir().unwrap();
        let outside = outside(dir.path());
        let base = dir.path().join("base");
        fs::create_dir_all(base.join("real").join("tree")).unwrap();
        symlink(&outside, base.join("link")).unwrap();

        // The parent is a link: nothing behind it is removed.
        assert!(remove_tree(&base.join("link").join("sub"), None).is_err());
        assert!(remove_tree_under(&base, Path::new("link/sub"), None).is_err());
        assert!(remove_tree_under(&base.join("link"), Path::new("sub"), None).is_err());
        for refused in ["..", "real/..", "../outside", "real/../../outside/sub", "/", "."] {
            assert!(
                remove_tree_under(&base, Path::new(refused), None).is_err(),
                "{refused}"
            );
        }
        assert!(remove_tree_under(&base, &outside.join("sub"), None).is_err(), "absolute");
        assert!(remove_tree(Path::new("/"), None).is_err());
        assert!(remove_tree(&base.join("real").join(".."), None).is_err());
        assert!(outside.join("sub").join("precious").exists() && base.join("real/tree").exists());

        assert!(remove_tree_under(&base, Path::new("real/tree"), None).unwrap());
        assert!(!base.join("real/tree").exists() && base.join("real").exists());
        // Nothing there, and no directory on the way: nothing to do, or an error.
        assert!(remove_tree_under(&base, Path::new("real/tree"), None).unwrap());
        assert!(remove_tree_under(&base, Path::new("gone/tree"), None).is_err());
    }

    /// The only lines that differ on Linux: the mode of an unreadable
    /// directory is changed through an `O_PATH` descriptor, never through a
    /// link put under its name.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn linux_changes_the_mode_of_a_directory_and_never_through_a_link() {
        let dir = tempfile::tempdir().unwrap();
        let outside = outside(dir.path());
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o500)).unwrap();
        let base = dir.path().join("base");
        fs::create_dir_all(base.join("locked")).unwrap();
        fs::write(base.join("file"), "x").unwrap();
        fs::set_permissions(base.join("file"), fs::Permissions::from_mode(0o400)).unwrap();
        fs::set_permissions(base.join("locked"), fs::Permissions::from_mode(0o000)).unwrap();
        symlink(&outside, base.join("link")).unwrap();
        let parent = open_dir(&base).unwrap();
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o7777;

        chmod_dir_at(parent.as_fd(), c"locked", 0o700).unwrap();
        assert_eq!(mode(&base.join("locked")), 0o700);
        assert!(chmod_dir_at(parent.as_fd(), c"link", 0o777).is_err());
        assert!(chmod_dir_at(parent.as_fd(), c"file", 0o777).is_err());
        assert!(chmod_dir_at(parent.as_fd(), c"missing", 0o777).is_err());
        assert_eq!((mode(&outside), mode(&base.join("file"))), (0o500, 0o400));
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
    }

    /// The same, on every system: a directory nobody can read is opened by
    /// first giving its owner access; a link under the name is not touched.
    #[test]
    fn an_unreadable_directory_is_made_accessible_and_a_link_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let outside = outside(dir.path());
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o500)).unwrap();
        let base = dir.path().join("base");
        fs::create_dir_all(base.join("locked")).unwrap();
        fs::set_permissions(base.join("locked"), fs::Permissions::from_mode(0o000)).unwrap();
        symlink(&outside, base.join("link")).unwrap();
        let parent = open_dir(&base).unwrap();
        let device = stat_fd(parent.as_fd()).unwrap().st_dev;
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o7777;

        grant_owner_access_at(parent.as_fd(), c"link", device);
        assert_eq!(mode(&outside), 0o500);
        grant_owner_access_at(parent.as_fd(), c"locked", device);
        assert_eq!(mode(&base.join("locked")), 0o700);
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
    }
}
