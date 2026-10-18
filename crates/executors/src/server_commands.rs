//! Process-wide demo policy. Set once, before composing the server runtime.
//! Daemons are separate processes and retain their own execution policy.
//!
//! One switch decides every demo refusal: commands from user configuration,
//! agent turns inside the server process, and repositories on the server's
//! own filesystem. Each refusal has its own code; all of them read this flag.
use std::sync::atomic::{AtomicBool, Ordering};

pub const CODE: &str = "server_commands_disabled";
pub const MESSAGE: &str = "This demo does not run commands";
pub const AGENTS_CODE: &str = "server_agents_disabled";
pub const AGENTS_MESSAGE: &str = "This demo does not run agents or connect model providers";
pub const LOCATION_CODE: &str = "server_location_refused";
pub const LOCATION_MESSAGE: &str =
    "This demo reads nothing from the server and accepts only https:// repositories at a public address";
static DISABLED: AtomicBool = AtomicBool::new(false);

/// Git configuration every Git process of a manager-only server runs with.
/// Forge's own Git (clone, fetch, merge, inspection) still runs against
/// repositories a visitor names, so it must not be steerable by them: no HTTP
/// redirect (one could lead to an internal host, and to plain http there), no
/// network protocol but https, and no program a repository can name (hooks, a
/// filesystem monitor, a bare repository embedded in checked-out content).
/// Local paths and bundles keep Git's default `user` policy for Forge's own
/// transfers. Command-scope configuration outranks every repository's own.
const GIT_CONFIG: &[(&str, &str)] = &[
    ("http.followRedirects", "false"),
    ("protocol.allow", "never"),
    ("protocol.https.allow", "always"),
    ("protocol.file.allow", "user"),
    ("core.hooksPath", "/dev/null"),
    ("core.fsmonitor", "false"),
    ("safe.bareRepository", "explicit"),
    ("submodule.recurse", "false"),
    ("fetch.recurseSubmodules", "false"),
    // A remote that trickles bytes cannot hold a Git process forever.
    ("http.lowSpeedLimit", "1000"),
    ("http.lowSpeedTime", "60"),
];

/// Trusted command-scope Git settings for the selected demo policy.
/// Callers that discard inherited Git configuration must install these from
/// index zero, setting every key/value and the count to this slice's length.
/// The explicit flag also lets callers test the policy without disabling the
/// process-wide, monotonic switch.
pub fn git_config_policy(manager_only: bool) -> &'static [(&'static str, &'static str)] {
    if manager_only {
        GIT_CONFIG
    } else {
        &[]
    }
}

/// Monotonic: configuration, templates and requests cannot re-enable commands.
pub fn disable() {
    static GIT_POLICY: std::sync::Once = std::sync::Once::new();
    DISABLED.store(true, Ordering::SeqCst);
    GIT_POLICY.call_once(restrict_git);
}

/// Every Git child inherits the process environment; Forge spawns Git from
/// many places. Commands that discard inherited Git configuration reapply
/// `git_config_policy` themselves. Entries already in the process environment
/// are kept and these are appended after them.
fn restrict_git() {
    let policy = git_config_policy(manager_only());
    let base = std::env::var("GIT_CONFIG_COUNT")
        .ok()
        .and_then(|count| count.trim().parse::<usize>().ok())
        .unwrap_or(0);
    for (offset, (key, value)) in policy.iter().enumerate() {
        std::env::set_var(format!("GIT_CONFIG_KEY_{}", base + offset), key);
        std::env::set_var(format!("GIT_CONFIG_VALUE_{}", base + offset), value);
    }
    std::env::set_var("GIT_CONFIG_COUNT", (base + policy.len()).to_string());
    // No credential prompt can wait on a terminal for a public remote.
    std::env::set_var("GIT_TERMINAL_PROMPT", "0");
}

/// True when the server is a manager only (demo mode).
pub fn manager_only() -> bool {
    DISABLED.load(Ordering::SeqCst)
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("server_commands_disabled: This demo does not run commands")]
pub struct ServerCommandsDisabled;

pub fn ensure_allowed() -> Result<(), ServerCommandsDisabled> {
    if manager_only() {
        Err(ServerCommandsDisabled)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("server_agents_disabled: This demo does not run agents or connect model providers")]
pub struct ServerAgentsDisabled;

/// Native/embedded agent turns and provider connections run in the server
/// process; a manager-only server refuses them.
pub fn ensure_agents_allowed() -> Result<(), ServerAgentsDisabled> {
    if manager_only() {
        Err(ServerAgentsDisabled)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_config_policy_is_empty_outside_demo() {
        assert!(git_config_policy(false).is_empty());
        assert_eq!(git_config_policy(true), GIT_CONFIG);
        assert!(!git_config_policy(true).is_empty());
    }

    #[test]
    fn server_commands_policy_is_monotonic_and_shared_with_agents() {
        const CHILD: &str = "FORGE_SERVER_COMMANDS_POLICY_TEST";
        if std::env::var_os(CHILD).is_none() {
            // Disabling is irreversible: keep other executor tests in normal mode.
            assert!(!manager_only());
            assert!(ensure_allowed().is_ok());
            assert!(ensure_agents_allowed().is_ok());
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "server_commands::tests::server_commands_policy_is_monotonic_and_shared_with_agents",
                ])
                .env(CHILD, "1")
                // An operator's own entry precedes the appended demo policy.
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "user.useConfigOnly")
                .env("GIT_CONFIG_VALUE_0", "true")
                .status()
                .unwrap();
            assert!(status.success());
            assert!(!manager_only());
            return;
        }
        for _ in 0..2 {
            disable();
            assert!(manager_only());
            // Appended once, after the inherited entry, however often called.
            let count = GIT_CONFIG.len() + 1;
            assert_eq!(
                std::env::var("GIT_CONFIG_COUNT").unwrap(),
                count.to_string()
            );
            assert_eq!(
                std::env::var("GIT_CONFIG_KEY_0").unwrap(),
                "user.useConfigOnly"
            );
            for (index, (key, value)) in GIT_CONFIG.iter().enumerate() {
                assert_eq!(
                    std::env::var(format!("GIT_CONFIG_KEY_{}", index + 1)).unwrap(),
                    *key
                );
                assert_eq!(
                    std::env::var(format!("GIT_CONFIG_VALUE_{}", index + 1)).unwrap(),
                    *value
                );
            }
            assert_eq!(std::env::var("GIT_TERMINAL_PROMPT").unwrap(), "0");
            assert_eq!(
                ensure_allowed().unwrap_err().to_string(),
                format!("{CODE}: {MESSAGE}")
            );
            assert_eq!(
                ensure_agents_allowed().unwrap_err().to_string(),
                format!("{AGENTS_CODE}: {AGENTS_MESSAGE}")
            );
        }
    }
}
