//! Machine-local policy, installed by the process entrypoint. Never sent over
//! the daemon protocol: remote commands use the receiving machine's policy.
use config::RunBudget;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    sync::{Arc, OnceLock, RwLock},
};
use tokio::process::Command;

#[derive(Debug, Default)]
pub struct MachineRunPolicy(RwLock<RunBudget>);
impl MachineRunPolicy {
    pub fn new(budget: RunBudget) -> Self {
        Self(RwLock::new(budget))
    }
    pub fn get(&self) -> RunBudget {
        *self.0.read().unwrap_or_else(|p| p.into_inner())
    }
    pub fn update(&self, cap: Option<Option<u32>>, jobs: Option<Option<u32>>, nice: Option<u32>) {
        let mut budget = self.0.write().unwrap_or_else(|p| p.into_inner());
        if let Some(cap) = cap {
            budget.max_concurrent_runs = cap;
        }
        if let Some(jobs) = jobs {
            budget.build_jobs_per_run = jobs;
        }
        if let Some(nice) = nice {
            budget.run_nice = nice;
        }
    }
}
fn policy_slot() -> &'static RwLock<Arc<MachineRunPolicy>> {
    static POLICY: OnceLock<RwLock<Arc<MachineRunPolicy>>> = OnceLock::new();
    POLICY.get_or_init(|| RwLock::new(Arc::new(MachineRunPolicy::default())))
}
pub fn machine_policy() -> Arc<MachineRunPolicy> {
    Arc::clone(&policy_slot().read().unwrap_or_else(|p| p.into_inner()))
}
pub fn install_machine_policy(policy: Arc<MachineRunPolicy>) {
    *policy_slot().write().unwrap_or_else(|p| p.into_inner()) = policy;
}

pub const BUILD_ENV_KEYS: [&str; 5] = [
    "CARGO_BUILD_JOBS",
    "RUST_TEST_THREADS",
    "MAKEFLAGS",
    "CMAKE_BUILD_PARALLEL_LEVEL",
    "GOFLAGS",
];
/// Patch only the five build variables so launch-path environment controls remain intact.
/// Project > operator process > Forge budget. Preserve non-UTF8 operator values.
pub fn build_environment(
    budget: RunBudget,
    project: &BTreeMap<String, String>,
    operator: impl Fn(&str) -> Option<OsString>,
) -> BTreeMap<OsString, OsString> {
    let mut env = BTreeMap::new();
    let jobs = budget.build_jobs();
    for key in BUILD_ENV_KEYS {
        let declared = project.get(key);
        #[cfg(windows)]
        let declared = declared.or_else(|| {
            project
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))
                .map(|(_, value)| value)
        });
        if let Some(value) = declared {
            env.insert(key.into(), value.into());
        } else if let Some(value) = operator(key) {
            env.insert(key.into(), value);
        } else if jobs > 0 {
            let value = match key {
                "MAKEFLAGS" => format!("-j{jobs}"),
                "GOFLAGS" => format!("-p={jobs}"),
                _ => jobs.to_string(),
            };
            env.insert(key.into(), value.into());
        }
    }
    env
}

pub fn apply(command: &mut Command, project: &BTreeMap<String, String>) {
    apply_budget(command, machine_policy().get(), project);
}
pub fn apply_budget(command: &mut Command, budget: RunBudget, project: &BTreeMap<String, String>) {
    command.envs(build_environment(budget, project, |key| {
        std::env::var_os(key)
    }));
    lower_priority(command, budget.run_nice);
}

#[cfg(unix)]
fn lower_priority(command: &mut Command, increment: u32) {
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixDatagram;
    if increment == 0 {
        return;
    }
    // Logging in pre_exec can deadlock after fork. A single parent-side reader
    // logs the first failure; the child only uses async-signal-safe syscalls.
    static REPORTER: OnceLock<Option<UnixDatagram>> = OnceLock::new();
    let fd = REPORTER
        .get_or_init(|| match UnixDatagram::pair() {
            Ok((sender, receiver)) => {
                let _ = sender.set_nonblocking(true);
                std::thread::spawn(move || {
                    let mut logged = false;
                    while receiver.recv(&mut [0]).is_ok() {
                        if !logged {
                            tracing::warn!("could not lower run CPU priority; continuing run");
                            logged = true;
                        }
                    }
                });
                Some(sender)
            }
            Err(error) => {
                tracing::warn!(%error, "could not initialize run priority failure reporting");
                None
            }
        })
        .as_ref()
        .map(AsRawFd::as_raw_fd);
    let increment = increment.min(19) as i32;
    // SAFETY: this child callback uses only getpriority/setpriority/write;
    // no allocation, locks, Rust logging, or parent priority changes.
    unsafe {
        command.pre_exec(move || {
            let priority = libc::getpriority(libc::PRIO_PROCESS, 0);
            if libc::setpriority(libc::PRIO_PROCESS, 0, (priority + increment).min(19)) != 0 {
                if let Some(fd) = fd {
                    let _ = libc::write(fd, b"!".as_ptr().cast(), 1);
                }
            }
            Ok(())
        });
    }
}
#[cfg(not(unix))]
fn lower_priority(_command: &mut Command, _increment: u32) {}

/// Read the calling process's Unix priority without spawning an operator tool.
#[cfg(unix)]
pub fn current_niceness() -> i32 {
    // SAFETY: getpriority reads the calling process and has no pointer arguments.
    unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn project_operator_budget_precedence_and_disabled() {
        let project = BTreeMap::from([("CARGO_BUILD_JOBS".into(), "project".into())]);
        let budget = RunBudget {
            build_jobs_per_run: Some(3),
            ..Default::default()
        };
        let env = build_environment(budget, &project, |key| {
            (key == "RUST_TEST_THREADS" || key == "CARGO_BUILD_JOBS").then(|| "operator".into())
        });
        for (key, value) in [
            ("CARGO_BUILD_JOBS", "project"),
            ("RUST_TEST_THREADS", "operator"),
            ("MAKEFLAGS", "-j3"),
            ("CMAKE_BUILD_PARALLEL_LEVEL", "3"),
            ("GOFLAGS", "-p=3"),
        ] {
            assert_eq!(env.get(std::ffi::OsStr::new(key)).unwrap(), value);
        }
        let disabled = build_environment(
            RunBudget {
                build_jobs_per_run: Some(0),
                ..budget
            },
            &project,
            |_| None,
        );
        assert_eq!(disabled.len(), 1);
        assert!(build_environment(
            RunBudget {
                build_jobs_per_run: Some(0),
                ..budget
            },
            &BTreeMap::new(),
            |_| None
        )
        .is_empty());
    }
    #[test]
    fn budget_does_not_restore_removed_git_environment() {
        let project = BTreeMap::from([
            ("GIT_DIR".into(), "outside".into()),
            ("CARGO_BUILD_JOBS".into(), "project".into()),
        ]);
        let mut command = Command::new("git");
        command.envs(&project).env_remove("GIT_DIR");
        apply_budget(
            &mut command,
            RunBudget {
                run_nice: 0,
                ..Default::default()
            },
            &project,
        );
        let vars: BTreeMap<_, _> = command.as_std().get_envs().collect();
        assert_eq!(vars.get(std::ffi::OsStr::new("GIT_DIR")), Some(&None));
        assert_eq!(
            vars.get(std::ffi::OsStr::new("CARGO_BUILD_JOBS")),
            Some(&Some(std::ffi::OsStr::new("project")))
        );
    }

    #[test]
    fn live_updates_recompute_automatic_budget_and_preserve_omitted_settings() {
        let policy = MachineRunPolicy::default();
        policy.update(Some(Some(1)), None, Some(7));
        assert_eq!(
            policy.get().build_jobs(),
            config::logical_cores().max(1) as u32
        );
        policy.update(Some(Some(0)), None, None);
        assert_eq!(
            policy.get().build_jobs(),
            config::resolved_build_jobs_for_cores(None, Some(0), config::logical_cores())
        );
        assert_eq!(policy.get().run_nice, 7);
        policy.update(None, Some(Some(0)), Some(0));
        assert_eq!(policy.get().build_jobs(), 0);
        policy.update(None, Some(None), None);
        assert!(policy.get().build_jobs() >= 1);
        assert_eq!(policy.get().run_nice, 0);
    }
    #[cfg(unix)]
    #[test]
    fn priority_probe() {
        println!("FORGE_NICE={}", current_niceness());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn child_niceness_increment_and_zero() {
        async fn nice(budget: RunBudget) -> i32 {
            let mut cmd = Command::new(std::env::current_exe().unwrap());
            cmd.args([
                "--exact",
                "run_process::tests::priority_probe",
                "--nocapture",
            ]);
            apply_budget(&mut cmd, budget, &BTreeMap::new());
            let output = cmd.output().await.unwrap();
            assert!(output.status.success());
            let text = String::from_utf8(output.stdout).unwrap();
            text.lines()
                .find_map(|line| line.split_once("FORGE_NICE=").map(|(_, value)| value))
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        }
        let off = RunBudget {
            run_nice: 0,
            ..Default::default()
        };
        let baseline = nice(off).await;
        assert_eq!(baseline, current_niceness());
        assert_eq!(
            nice(RunBudget { run_nice: 5, ..off }).await,
            (baseline + 5).min(19)
        );
        assert_eq!(nice(off).await, baseline);
    }
}
