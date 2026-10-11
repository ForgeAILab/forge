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
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Component, Path},
    time::Instant,
};

/// Entries removed between two looks at the deadline. A tree smaller than
/// this always goes in one call, so every pass makes progress.
const STRIDE: usize = 256;

fn c_name(name: &OsStr) -> io::Result<CString> {
    CString::new(name.as_bytes()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
}

fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

const DIR_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;

/// Open the directory at `path`. The last component is never followed when
/// it is a link; the components before it are the caller's own.
fn open_dir(path: &Path) -> io::Result<OwnedFd> {
    let path = c_name(path.as_os_str())?;
    // SAFETY: `path` is a valid NUL-terminated string; the returned
    // descriptor is owned by nobody else.
    unsafe {
        let fd = check(libc::open(path.as_ptr(), DIR_FLAGS))?;
        Ok(OwnedFd::from_raw_fd(fd))
    }
}

/// Open the directory `name` inside `parent`; fails when `name` is a link
/// or anything else that is not a directory.
fn open_dir_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    // SAFETY: `parent` is an open directory descriptor and `name` a valid
    // NUL-terminated string; the returned descriptor is owned by nobody else.
    unsafe {
        let fd = check(libc::openat(parent, name.as_ptr(), DIR_FLAGS))?;
        Ok(OwnedFd::from_raw_fd(fd))
    }
}

fn unlink_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    // SAFETY: `parent` is an open directory descriptor and `name` a valid
    // NUL-terminated string.
    check(unsafe { libc::unlinkat(parent, name.as_ptr(), flags) }).map(drop)
}

/// Give the owner read, write and search on the open directory `dir`, so its
/// entries can be listed and removed. Through the descriptor: never a path.
fn make_owner_writable(dir: RawFd) {
    // SAFETY: `dir` is an open descriptor and `stat` is plain data the call
    // fills in.
    unsafe {
        let mut stat: libc::stat = std::mem::zeroed();
        if libc::fstat(dir, &mut stat) == 0 && stat.st_mode & 0o700 != 0o700 {
            let _ = libc::fchmod(dir, (stat.st_mode | 0o700) & 0o7777);
        }
    }
}

/// The names in the open directory `dir`, without `.` and `..`.
fn names(dir: &OwnedFd) -> io::Result<Vec<CString>> {
    let copy = dir.try_clone()?;
    // SAFETY: `copy` is an open directory descriptor whose ownership passes
    // to the stream (closed by `closedir`); every entry is read before the
    // next `readdir` call and its name copied out.
    unsafe {
        use std::os::fd::IntoRawFd;
        let raw = copy.into_raw_fd();
        let stream = libc::fdopendir(raw);
        if stream.is_null() {
            let error = io::Error::last_os_error();
            libc::close(raw);
            return Err(error);
        }
        libc::rewinddir(stream);
        let mut names = Vec::new();
        loop {
            let entry = libc::readdir(stream);
            if entry.is_null() {
                break;
            }
            let name = CStr::from_ptr((*entry).d_name.as_ptr());
            if name.to_bytes() != b"." && name.to_bytes() != b".." {
                names.push(name.to_owned());
            }
        }
        libc::closedir(stream);
        Ok(names)
    }
}

struct Frame {
    dir: OwnedFd,
    /// The name of `dir` in the frame below (in the tree's parent for the
    /// first frame).
    name: CString,
    pending: Vec<CString>,
}

impl Frame {
    fn open(parent: RawFd, name: CString) -> io::Result<Self> {
        let dir = match open_dir_at(parent, &name) {
            Ok(dir) => dir,
            // A directory the owner cannot read: a run left it that way.
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                grant_owner_access_at(parent, &name);
                open_dir_at(parent, &name)?
            }
            Err(error) => return Err(error),
        };
        make_owner_writable(dir.as_raw_fd());
        let pending = names(&dir)?;
        Ok(Self { dir, name, pending })
    }
}

/// Give the owner access to the directory `name` in `parent`, which could
/// not be opened for reading. Never through a link.
fn grant_owner_access_at(parent: RawFd, name: &CStr) {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // `fchmodat` cannot refuse a link on Linux. Open the entry itself
        // without following it (`O_PATH` needs no permission on it), make
        // sure it is a directory, and change it through the descriptor.
        // SAFETY: `parent` is an open directory descriptor, `name` a valid
        // NUL-terminated string, `stat` plain data the call fills in.
        unsafe {
            let fd = libc::openat(
                parent,
                name.as_ptr(),
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            );
            if fd < 0 {
                return;
            }
            let fd = OwnedFd::from_raw_fd(fd);
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd.as_raw_fd(), &mut stat) == 0
                && stat.st_mode & libc::S_IFMT == libc::S_IFDIR
            {
                let _ = std::fs::set_permissions(
                    format!("/proc/self/fd/{}", fd.as_raw_fd()),
                    <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(
                        (stat.st_mode | 0o700) & 0o7777,
                    ),
                );
            }
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        // SAFETY: `parent` is an open directory descriptor, `name` a valid
        // NUL-terminated string, `stat` plain data the call fills in.
        unsafe {
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstatat(parent, name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) == 0
                && stat.st_mode & libc::S_IFMT == libc::S_IFDIR
            {
                let _ = libc::fchmodat(
                    parent,
                    name.as_ptr(),
                    (stat.st_mode | 0o700) & 0o7777,
                    libc::AT_SYMLINK_NOFOLLOW,
                );
            }
        }
    }
}

/// Whether removing `name` failed because it is a directory (`EISDIR` on
/// Linux, `EPERM` elsewhere).
fn is_a_directory(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::EISDIR | libc::EPERM))
}

/// Remove the entry at `path` with everything in it, including directories
/// a run left read-only. Nothing is ever followed: a link, at `path` or
/// anywhere below it, is removed as the entry it is, also when it replaced
/// a directory while this ran.
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
    let name = c_name(name)?;
    match unlink_at(parent.as_raw_fd(), &name, 0) {
        Ok(()) => return Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) if !is_a_directory(&error) => return Err(error),
        Err(_) => {}
    }
    let mut stack = vec![Frame::open(parent.as_raw_fd(), name)?];
    let mut removed = 0_usize;
    while let Some(frame) = stack.last_mut() {
        let Some(child) = frame.pending.pop() else {
            let done = stack.pop().expect("a frame was just seen");
            let above = stack
                .last()
                .map_or(parent.as_raw_fd(), |frame| frame.dir.as_raw_fd());
            // Fails when an entry inside could not be removed; it then stays.
            let _ = unlink_at(above, &done.name, libc::AT_REMOVEDIR);
            removed += 1;
            if !stack.is_empty() && out_of_time(removed, deadline) {
                return Ok(false);
            }
            continue;
        };
        let dir = frame.dir.as_raw_fd();
        match unlink_at(dir, &child, 0) {
            Err(error) if is_a_directory(&error) => {
                // Too deep to hold a descriptor per level, unreadable, or
                // no longer a directory: left where it is, and the walk
                // goes on past it.
                if let Ok(frame) = Frame::open(dir, child) {
                    stack.push(frame);
                }
            }
            _ => {
                removed += 1;
                if out_of_time(removed, deadline) {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn out_of_time(removed: usize, deadline: Option<Instant>) -> bool {
    removed.is_multiple_of(STRIDE) && deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

/// Open the directory reached from `base` through `components`, each of
/// which must be a plain name of a real directory.
fn descend<'a>(
    base: &Path,
    components: impl IntoIterator<Item = Component<'a>>,
) -> io::Result<OwnedFd> {
    let mut dir = open_dir(base)?;
    for component in components {
        let Component::Normal(name) = component else {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        };
        dir = open_dir_at(dir.as_raw_fd(), &c_name(name)?)?;
    }
    Ok(dir)
}

fn split(relative: &Path) -> io::Result<(Vec<Component<'_>>, CString)> {
    let mut components: Vec<Component<'_>> = relative.components().collect();
    let Some(Component::Normal(name)) = components.pop() else {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    };
    Ok((components, c_name(name)?))
}

/// Delete the regular file `relative` under the directory `base`. Fails,
/// and deletes nothing, when `base` or any directory on the way is a link
/// (or is no longer a directory) or the entry is not a regular file.
pub(crate) fn remove_file_under(base: &Path, relative: &Path) -> io::Result<()> {
    let (parents, name) = split(relative)?;
    let dir = descend(base, parents)?;
    // SAFETY: `dir` is an open directory descriptor, `name` a valid
    // NUL-terminated string, `stat` plain data the call fills in.
    let regular = unsafe {
        let mut stat: libc::stat = std::mem::zeroed();
        check(libc::fstatat(
            dir.as_raw_fd(),
            name.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        ))?;
        stat.st_mode & libc::S_IFMT == libc::S_IFREG
    };
    if !regular {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    unlink_at(dir.as_raw_fd(), &name, 0)
}

/// Rename the entry `relative` under the directory `base` to `target`
/// (whose own directory is the caller's). Fails, and moves nothing, when
/// `base` or any directory on the way to the entry is a link.
pub(crate) fn rename_under(base: &Path, relative: &Path, target: &Path) -> io::Result<()> {
    let (parents, name) = split(relative)?;
    let dir = descend(base, parents)?;
    let target = c_name(target.as_os_str())?;
    // SAFETY: `dir` is an open directory descriptor; both names are valid
    // NUL-terminated strings.
    check(unsafe {
        libc::renameat(
            dir.as_raw_fd(),
            name.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
        )
    })
    .map(drop)
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
        assert_eq!(fs::read_to_string(outside.join("precious")).unwrap(), "keep");
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
        assert_eq!(listed, [c"victim".to_owned()]);
        // Between the listing and the descent.
        fs::remove_dir(tree.join("victim")).unwrap();
        symlink(&outside, tree.join("victim")).unwrap();
        assert!(Frame::open(parent.as_raw_fd(), c"victim".to_owned()).is_err());
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
        assert!(remove_file_under(&base, Path::new("sub/link")).is_err(), "a link");
        assert!(remove_file_under(&base, Path::new("sub")).is_err(), "a directory");
        assert!(remove_file_under(&base, Path::new("../outside/precious")).is_err());
        assert!(rename_under(&base, Path::new("linked/sub"), &moved).is_err());
        assert!(rename_under(&dir.path().join("base/linked"), Path::new("sub"), &moved).is_err());
        assert!(!moved.exists());
        assert_eq!(fs::read_to_string(outside.join("precious")).unwrap(), "keep");
        assert!(outside.join("sub").join("precious").exists());

        remove_file_under(&base, Path::new("sub/precious")).unwrap();
        assert!(!base.join("sub").join("precious").exists());
        rename_under(&base, Path::new("sub"), &moved).unwrap();
        assert!(moved.join("link").symlink_metadata().is_ok() && !base.join("sub").exists());
    }
}
