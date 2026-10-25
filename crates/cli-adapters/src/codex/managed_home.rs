//! Managed Codex home preparation and writable sandbox roots.

use super::*;

/// The part of the Task root's environment a Codex execution can write.
///
/// - `yolo` runs without a sandbox: everything.
/// - A managed Task runs under workspace-write with only the roots Forge
///   grants. The per-run temp directory and the build directory are granted
///   (see `managed_writable_roots`) when they really are where the Task root
///   says: a path that resolves anywhere else (a planted link) is dropped, so
///   the run keeps the scratch directory of its managed home and builds in
///   the worktree.
/// - Any other execution uses the sandbox of the operator's own Codex
///   configuration, which Forge does not control: nothing is redirected,
///   and no compiler-cache wrapper is passed.
pub(super) fn codex_admitted_sandbox(
    sandbox: executors::sandbox::SandboxEnv,
    worktree: &Path,
    managed_task: bool,
    is_yolo: bool,
) -> executors::sandbox::SandboxEnv {
    if is_yolo {
        return sandbox;
    }
    let Some(task_roots) = worktree
        .parent()
        .and_then(Path::parent)
        .filter(|_| managed_task)
    else {
        return executors::sandbox::SandboxEnv::none();
    };
    let in_place = |path: &Path| {
        path.strip_prefix(task_roots).is_ok_and(|suffix| {
            resolve_path_for_overlap(path) == resolve_path_for_overlap(task_roots).join(suffix)
        })
    };
    let mut sandbox = sandbox;
    if sandbox.tmp_dir().is_some_and(|tmp| !in_place(tmp)) {
        sandbox = sandbox.without_tmp();
    }
    if sandbox
        .build_dir("CARGO_TARGET_DIR")
        .is_some_and(|build| !in_place(build))
    {
        sandbox = sandbox.without_build();
    }
    // The shared compiler cache reaches a sandboxed Codex only through a
    // wrapper known to compile uncached when it cannot reach its store
    // (the sandbox may deny it the daemon or server it talks to), and only
    // into a store that is the directory its path names, not a link.
    let usable = |cache: &executors::compiler_cache::CacheEnv| {
        cache.kind().fails_open()
            && cache.dir().is_some_and(|store| {
                store
                    .parent()
                    .zip(store.file_name())
                    .is_some_and(|(parent, name)| {
                        resolve_path_for_overlap(store)
                            == resolve_path_for_overlap(parent).join(name)
                    })
            })
    };
    if sandbox.compiler_cache().is_some_and(|cache| !usable(cache)) {
        sandbox = sandbox.without_compiler_cache();
    }
    sandbox
}

/// Move a Task's managed home from its pre-0.14 place beside the logs into
/// the Task root, so a Task that resumes after the upgrade keeps its Codex
/// sessions. Only the server layout (`logs/<project>/<task>/`) is per Task;
/// the old daemon home was shared by every execution and is left alone.
/// Any failure leaves a fresh home to be created.
pub(super) fn adopt_legacy_managed_home(legacy: Option<&Path>, home: &Path, task_id: &str) {
    let Some(legacy) = legacy else { return };
    let per_task = legacy
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == task_id);
    let is_dir = |path: &Path| {
        path.symlink_metadata()
            .is_ok_and(|metadata| metadata.file_type().is_dir())
    };
    if !per_task || home.symlink_metadata().is_ok() || !is_dir(legacy) {
        return;
    }
    let Some(parent) = home.parent() else { return };
    // One level, never through a link: `.forge-task` exists and is Forge's.
    let _ = std::fs::create_dir(parent);
    if is_dir(parent)
        && let Err(error) = std::fs::rename(legacy, home)
    {
        tracing::warn!(from = %legacy.display(), to = %home.display(), %error, "could not move the managed Codex home into the Task root; a fresh one is created");
    }
}

pub(super) fn ambient_codex_home() -> PathBuf {
    std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| dirs_path("codex"))
}

/// Codex rejects writable roots with a symlink component (for example a
/// relocated `~/.npm` or macOS `/tmp`). Resolve the deepest existing ancestor
/// and keep any not-yet-created remainder as is.
/// Never grant a resolved root that contains a protected authority directory.
/// Resolve both sides, including relative links and chains, before comparison.
pub(super) fn safe_writable_root(
    root: &Path,
    default_root: &Path,
    home: Option<&Path>,
    worktree: &Path,
    daemon_root: Option<&Path>,
    managed_home: Option<&Path>,
) -> Option<PathBuf> {
    let resolved = resolve_path_for_overlap(root);
    let contains = |path: &Path| resolve_path_for_overlap(path).starts_with(&resolved);
    let overlaps = |path: &Path| {
        let path = resolve_path_for_overlap(path);
        resolved.starts_with(&path) || path.starts_with(&resolved)
    };
    let scratch = managed_home.map(|home| resolve_path_for_overlap(&home.join("task-scratch")));
    let inside_protected = daemon_root.is_some_and(|root| overlaps(&root.join(".forge")))
        || worktree.parent().is_some_and(overlaps)
        || managed_home.is_some_and(|home| overlaps(home) && scratch.as_ref() != Some(&resolved))
        || home.is_some_and(|home| {
            [
                ".ssh",
                ".gnupg",
                ".aws",
                ".kube",
                ".docker",
                ".config/gh",
                ".codex",
                ".claude",
                ".forge",
            ]
            .iter()
            .any(|name| overlaps(&home.join(name)))
        });
    let inside_other_task = worktree
        .parent()
        .and_then(Path::parent)
        .is_some_and(|tasks| resolved.starts_with(resolve_path_for_overlap(tasks)));
    if !cache_root_is_disjoint_from_task(root, worktree)
        || resolved.parent().is_none()
        || home
            .into_iter()
            .chain([worktree])
            .chain(worktree.parent())
            .chain(managed_home)
            .any(contains)
        || home.is_some_and(|home| aliases_home_ancestor(&resolved, home))
        || inside_protected
        || inside_other_task
        || redirected_volume_refusal(
            root,
            &resolved,
            default_root,
            home,
            redirected_root_is_whole_volume,
        )
    {
        tracing::warn!(root = %root.display(), resolved = %resolved.display(), "dropped unsafe Codex writable root");
        return None;
    }
    Some(resolved)
}

fn redirected_volume_refusal(
    root: &Path,
    resolved: &Path,
    default_root: &Path,
    home: Option<&Path>,
    whole_volume: impl FnOnce(&Path) -> bool,
) -> bool {
    // A relocation variable is an explicit choice. Only a default cache
    // redirected by a symlink is subject to the whole-volume check.
    if lexical_normalize(root) != lexical_normalize(default_root) {
        return false;
    }
    // Resolve HOME, but not the default cache's own symlinks. A symlink in
    // HOME must not make all otherwise ordinary caches look redirected.
    let default = home
        .and_then(|home| {
            default_root
                .strip_prefix(home)
                .ok()
                .map(|suffix| resolve_path_for_overlap(home).join(suffix))
        })
        .unwrap_or_else(|| normalize_platform_path(&lexical_normalize(default_root)));
    resolved != default && whole_volume(resolved)
}

fn normalize_macos_data_path(path: &Path) -> PathBuf {
    path.strip_prefix("/System/Volumes/Data")
        .map(|suffix| Path::new("/").join(suffix))
        .unwrap_or_else(|_| path.to_path_buf())
}

fn normalize_platform_path(path: &Path) -> PathBuf {
    if cfg!(target_os = "macos") {
        normalize_macos_data_path(path)
    } else {
        path.to_path_buf()
    }
}

#[cfg(unix)]
fn aliases_home_ancestor(root: &Path, home: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(root) = std::fs::metadata(root) else {
        return false;
    };
    resolve_path_for_overlap(home).ancestors().any(|ancestor| {
        std::fs::metadata(ancestor)
            .is_ok_and(|metadata| (root.dev(), root.ino()) == (metadata.dev(), metadata.ino()))
    })
}

#[cfg(not(unix))]
fn aliases_home_ancestor(_root: &Path, _home: &Path) -> bool {
    false
}

// Compare mount-table strings without touching each mount: an unavailable
// network filesystem must not delay cache admission.
#[cfg(any(target_os = "linux", test))]
fn mount_table_contains_root(table: &str, root: &Path) -> bool {
    table
        .lines()
        .filter_map(|line| line.split_whitespace().nth(4))
        .any(|mount| {
            let mount = mount
                .replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\012", "\n")
                .replace("\\134", "\\");
            normalize_platform_path(Path::new(&mount)) == root
        })
}

fn redirected_root_is_whole_volume(root: &Path) -> bool {
    #[cfg(target_os = "linux")]
    if let Ok(table) = std::fs::read_to_string("/proc/self/mountinfo") {
        return mount_table_contains_root(&table, root);
    }
    root == inferred_mount_root(root)
}

fn inferred_mount_root(root: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for path in root.ancestors() {
            if let Some(parent) = path.parent()
                && let (Ok(child), Ok(parent)) =
                    (std::fs::metadata(path), std::fs::metadata(parent))
                && child.dev() != parent.dev()
            {
                return path.to_path_buf();
            }
        }
    }
    root.ancestors().last().unwrap_or(root).to_path_buf()
}

/// Package-manager cache directories a managed Task may write so dependency
/// installs and builds work inside the Codex sandbox. Paths honor the
/// managers' own relocation variables and need not exist yet.
pub(super) fn package_cache_roots(
    home: &Path,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Vec<PathBuf> {
    let from_env = |key: &str| {
        env(key)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    };
    let cargo_home = from_env("CARGO_HOME").unwrap_or_else(|| home.join(".cargo"));
    let go_mod_cache = from_env("GOMODCACHE").unwrap_or_else(|| {
        from_env("GOPATH")
            .unwrap_or_else(|| home.join("go"))
            .join("pkg/mod")
    });
    let mut roots = vec![
        from_env("npm_config_cache").unwrap_or_else(|| home.join(".npm")),
        cargo_home.join("registry"),
        cargo_home.join("git"),
        go_mod_cache,
        // XDG cache: pnpm, yarn, pip, go-build and others on Linux.
        from_env("XDG_CACHE_HOME").unwrap_or_else(|| home.join(".cache")),
        home.join(".local/share/pnpm"),
    ];
    if cfg!(target_os = "macos") {
        roots.push(home.join("Library/Caches"));
        roots.push(home.join("Library/pnpm"));
    }
    roots
}

/// A relocated package cache must never widen the sandbox back over the Task
/// directory that contains the host-owned canonical plan and staging area.
/// Compare resolved paths in both directions: a cache inside the Task root and
/// an ancestor cache that contains the Task root are equally unsafe.
fn cache_root_is_disjoint_from_task(cache_root: &Path, worktree: &Path) -> bool {
    let task_root = worktree.parent().unwrap_or(worktree);
    let cache_root = resolve_path_for_overlap(cache_root);
    let task_root = resolve_path_for_overlap(task_root);
    !cache_root.starts_with(&task_root) && !task_root.starts_with(&cache_root)
}

pub(super) fn resolve_path_for_overlap(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    };
    let normalized = lexical_normalize(&absolute);
    let mut existing = normalized.as_path();
    let mut suffix = Vec::new();
    loop {
        if let Ok(canonical) = std::fs::canonicalize(existing) {
            return normalize_platform_path(
                &suffix
                    .into_iter()
                    .rev()
                    .fold(canonical, |path, component| path.join(component)),
            );
        }
        let Some(name) = existing.file_name() else {
            return normalize_platform_path(&normalized);
        };
        suffix.push(name.to_os_string());
        let Some(parent) = existing.parent() else {
            return normalize_platform_path(&normalized);
        };
        existing = parent;
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(value) => normalized.push(value),
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

/// Build the smallest Codex home a managed Task needs. Authentication is the
/// only shared state; personal configuration inputs are deliberately absent.
/// The home lives beside Task logs, outside the worktree sandbox, so a Worker
/// cannot seed rules for a later execution.
pub(super) fn prepare_managed_codex_home(
    managed_home: &Path,
    ambient_home: &Path,
) -> Result<(), ExecutorError> {
    std::fs::create_dir_all(managed_home).map_err(|error| {
        ExecutorError::Other(format!(
            "failed to create managed Codex home {}: {error}",
            managed_home.display()
        ))
    })?;
    let home_metadata = managed_home.symlink_metadata().map_err(ExecutorError::Io)?;
    if home_metadata.file_type().is_symlink() || !home_metadata.is_dir() {
        return Err(ExecutorError::Other(format!(
            "managed Codex home is not a Forge-owned directory: {}",
            managed_home.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(managed_home, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| {
                ExecutorError::Other(format!(
                    "failed to protect managed Codex home {}: {error}",
                    managed_home.display()
                ))
            },
        )?;
    }

    // Codex materializes config.toml (project trust), bundled system skills,
    // and a plugins directory in its home while it runs. They are runtime
    // cache, not durable authority: discard them before every execution and
    // recreate only Forge's canonical config. This also makes a second attempt
    // safe when it reuses the Task log directory. Files that Codex does not
    // create remain fail-closed.
    //
    // `plugins` used to sit on the fail-closed list below, which meant the
    // first Codex execution in a Task always succeeded and every later one in
    // the same log directory died deterministically with "forbidden
    // configuration input" — Codex had created the directory itself during the
    // first run. Every re-review and every retry after a review was therefore
    // unreachable; observed as three identical reviewer failures on one Task
    // whose first reviewer attempt had completed normally.
    reset_managed_runtime_path(&managed_home.join("config.toml"))?;
    reset_managed_runtime_path(&managed_home.join("skills"))?;
    reset_managed_runtime_path(&managed_home.join("task-scratch"))?;
    reset_managed_runtime_path(&managed_home.join("plugins"))?;
    // These two Codex never writes. A repository or a user planting one is the
    // case this guard exists for, so they stay fail-closed.
    for forbidden in ["AGENTS.md", "hooks.json"] {
        let path = managed_home.join(forbidden);
        if path.exists() {
            return Err(ExecutorError::Other(format!(
                "managed Codex home contains forbidden configuration input: {}",
                path.display()
            )));
        }
    }
    ensure_managed_config(managed_home)?;
    ensure_managed_rules(managed_home)?;
    let scratch = managed_home.join("task-scratch");
    std::fs::create_dir(&scratch).map_err(ExecutorError::Io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o700))
            .map_err(ExecutorError::Io)?;
    }

    let source_auth = ambient_home.join("auth.json");
    if !source_auth.is_file() {
        // Environment- or keyring-backed authentication does not need a file.
        return Ok(());
    }
    let managed_auth = managed_home.join("auth.json");
    if managed_auth.symlink_metadata().is_ok() {
        let source = source_auth.canonicalize().map_err(ExecutorError::Io)?;
        let linked = managed_auth.canonicalize().map_err(ExecutorError::Io)?;
        if linked != source {
            return Err(ExecutorError::Other(format!(
                "managed Codex authentication link points at an unexpected file: {}",
                managed_auth.display()
            )));
        }
        return Ok(());
    }

    #[cfg(unix)]
    std::os::unix::fs::symlink(&source_auth, &managed_auth).map_err(ExecutorError::Io)?;
    #[cfg(windows)]
    std::fs::hard_link(&source_auth, &managed_auth).map_err(ExecutorError::Io)?;
    Ok(())
}

fn reset_managed_runtime_path(path: &Path) -> Result<(), ExecutorError> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            // `task-scratch` is the Worker's TMPDIR, so it holds whatever the
            // project's tests leave behind — including directories a test
            // made read-only and never restored. Removing an entry needs
            // write permission on its parent, so `remove_dir_all` failed with
            // EACCES on every later execution of the Task.
            #[cfg(unix)]
            make_directories_owner_writable(path).map_err(ExecutorError::Io)?;
            std::fs::remove_dir_all(path).map_err(ExecutorError::Io)
        }
        Ok(_) => std::fs::remove_file(path).map_err(ExecutorError::Io),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ExecutorError::Io(error)),
    }
}

/// Grant the owner `rwx` on every real directory under `root` so its entries
/// can be listed and unlinked. Symlinks are never followed, so nothing outside
/// `root` is touched.
#[cfg(unix)]
fn make_directories_owner_writable(root: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let mode = dir.symlink_metadata()?.permissions().mode();
        if mode & 0o700 != 0o700 {
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode | 0o700))?;
        }
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() && !file_type.is_symlink() {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
}

fn ensure_managed_config(managed_home: &Path) -> Result<(), ExecutorError> {
    let path = managed_home.join("config.toml");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(path).map_err(ExecutorError::Io)?;
    file.write_all(MANAGED_CONFIG.as_bytes())
        .map_err(ExecutorError::Io)
}

fn ensure_managed_rules(managed_home: &Path) -> Result<(), ExecutorError> {
    let rules_dir = managed_home.join("rules");
    match rules_dir.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ExecutorError::Other(format!(
                "managed Codex rules path is not a Forge-owned directory: {}",
                rules_dir.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&rules_dir).map_err(ExecutorError::Io)?;
        }
        Err(error) => return Err(ExecutorError::Io(error)),
    }

    let expected = rules_dir.join(MANAGED_RULES_FILE);
    for entry in std::fs::read_dir(&rules_dir).map_err(ExecutorError::Io)? {
        let entry = entry.map_err(ExecutorError::Io)?;
        if entry.path() != expected {
            return Err(ExecutorError::Other(format!(
                "managed Codex home contains an unexpected rules file: {}",
                entry.path().display()
            )));
        }
    }
    match std::fs::read_to_string(&expected) {
        Ok(contents) if contents == MANAGED_RULES => return Ok(()),
        Ok(_) => {
            return Err(ExecutorError::Other(format!(
                "managed Codex Task boundary rules were modified: {}",
                expected.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ExecutorError::Io(error)),
    }

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(&expected).map_err(ExecutorError::Io)?;
    file.write_all(MANAGED_RULES.as_bytes())
        .map_err(ExecutorError::Io)
}

#[cfg(test)]
mod tests;
