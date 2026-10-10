use std::{env, path::PathBuf};

#[must_use]
pub fn default_config_path() -> PathBuf {
    default_data_dir().join("forge.yaml")
}

#[must_use]
pub fn default_data_dir() -> PathBuf {
    assert!(
        !(env::var_os("FORGE_TEST_FORBID_DEFAULT_DATA_DIR").as_deref()
            == Some(std::ffi::OsStr::new("1"))
            && env::current_exe().is_ok_and(|executable| {
                executable.parent().and_then(std::path::Path::file_name)
                    == Some(std::ffi::OsStr::new("deps"))
            })),
        "a test tried to use the real Forge data directory; inject a tempfile::TempDir via \
         ForgeConfig::with_data_dir, ConfigOverrides::data_dir (with an explicit config path), \
         or FORGE_DATA_DIR instead of resolving the default"
    );
    home_dir().map_or_else(|| PathBuf::from(".forge"), |home| home.join(".forge"))
}

#[must_use]
pub fn data_dir_from_env() -> PathBuf {
    env::var("FORGE_DATA_DIR")
        .ok()
        .filter(|value| !value.is_empty())
        .map(|value| expand_path(&value))
        .unwrap_or_else(default_data_dir)
}

/// Where a server that never chose a workspace root keeps Task worktrees,
/// repository clones and execution logs: beside its database, so the
/// operating system's temp cleaner never removes them.
#[must_use]
pub fn default_workspace_root(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("worktrees")
}

/// The default workspace root of releases before the root moved into the
/// data directory. Only ever read: a server that finds its data there keeps
/// using it (and says so) until the operator runs
/// `forge --migrate-workspace-root`.
#[must_use]
pub fn legacy_temp_workspace_root() -> PathBuf {
    env::temp_dir().join("forge").join("worktrees")
}

pub(crate) fn expand_path(value: &str) -> PathBuf {
    let with_home = if value == "~" {
        home_dir()
            .map(|home| home.to_string_lossy().into_owned())
            .unwrap_or_else(|| value.to_owned())
    } else if let Some(rest) = value.strip_prefix("~/") {
        home_dir()
            .map(|home| home.join(rest).to_string_lossy().into_owned())
            .unwrap_or_else(|| value.to_owned())
    } else {
        value.to_owned()
    };

    PathBuf::from(expand_env_vars(&with_home))
}

fn expand_env_vars(value: &str) -> String {
    let mut output = String::new();
    let mut chars = value.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '$' {
            output.push(ch);
            continue;
        }

        if chars.peek() == Some(&'{') {
            chars.next();
            let mut name = String::new();
            for next in chars.by_ref() {
                if next == '}' {
                    break;
                }
                name.push(next);
            }
            output.push_str(&env::var(name).unwrap_or_default());
            continue;
        }

        let mut name = String::new();
        while let Some(next) = chars.peek().copied() {
            if next == '_' || next.is_ascii_alphanumeric() {
                chars.next();
                name.push(next);
            } else {
                break;
            }
        }

        if name.is_empty() {
            output.push('$');
        } else {
            output.push_str(&env::var(name).unwrap_or_default());
        }
    }

    output
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
}
