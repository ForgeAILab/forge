//! What a request states about the environment its commands will see.
//!
//! The canonical CI policy is the only one whose result may be reused. Its
//! steps inherit the owner's environment exactly as the frozen policy's do,
//! so its identity names that environment instead of restricting it: a
//! salted digest over everything a step inherits (names and values, minus
//! `check_executor::ENVIRONMENT_IDENTITY_DENYLIST`) and over the Project
//! values. The inherited answer is durably frozen once per owner environment
//! generation. Project values are combined with that answer again before a
//! run: if they changed, its receipt is a verdict but is never reused. A
//! missing inherited answer fails typed before identity admission.
use crate::{Result, ServiceError};
use api_types::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock, Weak},
};

/// The `system_setting` key of the salt below. Never listed or writable
/// through the admin settings API.
pub const VALUE_REVISION_SALT_KEY: &str = "check_value_revision_salt";

/// Environment values may be secrets, so an identity never holds one or a
/// public hash of one. Every digest of a value is keyed with a random salt
/// this server created once and keeps in its own database: equal values give
/// equal digests for as long as the installation lives, across restarts.
/// Losing the salt changes every identity, so stored results are simply not
/// found again.
async fn salt(db: &db::SqliteDb) -> Result<String> {
    let read = || db.check_value_revision_salt(VALUE_REVISION_SALT_KEY);
    if let Some(salt) = read().await? {
        return Ok(salt);
    }
    // First use. Two first users race on the key: one row wins and both read
    // it back.
    db.insert_check_value_revision_salt(
        VALUE_REVISION_SALT_KEY,
        format!("{}{}", db::new_uuid_v4(), db::new_uuid_v4()),
        db::now_rfc3339(),
    )
    .await?;
    read()
        .await?
        .ok_or_else(|| ServiceError::invalid_operation("check value salt was not stored"))
}

/// The opaque revision of one Project value.
pub async fn value_revision(db: &db::SqliteDb, key: &str, value: &str) -> Result<String> {
    canonical_digest_with_schema(
        "forge.check-value-revision/1",
        &serde_json::json!({ "salt": salt(db).await?, "key": key, "value": value }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

/// A server start is a new owner environment generation. Only the cache key
/// uses this id: equal observed inputs still produce equal identities after
/// restart. Profile changes require a restart; tool versions are not probed.
fn server_environment_generation() -> &'static str {
    static GENERATION: OnceLock<String> = OnceLock::new();
    GENERATION.get_or_init(db::new_uuid_v4)
}

// Tests that change the process environment model an owner restart explicitly.
// Other installations in the same parallel test process keep their generation.
#[cfg(test)]
static TEST_GENERATIONS: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();

/// Installations whose login shell does not answer, for a test of the
/// unavailable path. Scoped by installation like the generations above.
/// Under `test-support` too: an api test drives the owner's Retry of a
/// review entry through REST with it.
#[cfg(any(test, feature = "test-support"))]
static TEST_UNANSWERED: Mutex<std::collections::BTreeSet<String>> =
    Mutex::new(std::collections::BTreeSet::new());

#[cfg(any(test, feature = "test-support"))]
pub async fn silence_login_shell_for_test(db: &db::SqliteDb, silent: bool) -> Result<()> {
    let installation = salt(db).await?;
    let mut unanswered = TEST_UNANSWERED.lock().unwrap_or_else(|p| p.into_inner());
    if silent {
        unanswered.insert(installation);
    } else {
        unanswered.remove(&installation);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn renew_environment_generation_for_test(db: &db::SqliteDb) -> Result<()> {
    let installation = salt(db).await?;
    TEST_GENERATIONS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(installation, db::new_uuid_v4());
    Ok(())
}

fn inherited_revision(
    salt: &str,
    inherited: &check_executor::InheritedEnvironment,
) -> Result<String> {
    canonical_digest_with_schema(
        "forge.check-inherited-environment/2",
        &serde_json::json!({ "salt": salt, "inherited": inherited.identity_values() }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

/// The shell's version, salted like every other stored revision: the
/// attestation row holds no name or value the shell reported.
fn shell_revision(salt: &str, shell: &str) -> Result<String> {
    canonical_digest_with_schema(
        "forge.check-shell-revision/1",
        &serde_json::json!({ "salt": salt, "shell": shell }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

/// Project values stay current; the inherited revision is the owner's
/// immutable, durable answer for this environment generation.
fn environment_revision(
    salt: &str,
    inherited_revision: &str,
    project: &BTreeMap<String, String>,
) -> Result<String> {
    canonical_digest_with_schema(
        "forge.check-execution-environment/2",
        &serde_json::json!({ "salt": salt, "inherited_revision": inherited_revision, "project": project }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

type ProbeLock = tokio::sync::Mutex<()>;
/// Installation/generation scoped single-flight, including independently
/// composed DB handles. Dead entries hold no database or environment values.
fn probe_lock(salt_revision: &str, generation: &str) -> Arc<ProbeLock> {
    type Locks = BTreeMap<(String, String), Weak<ProbeLock>>;
    static LOCKS: OnceLock<Mutex<Locks>> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    locks.retain(|_, lock| lock.strong_count() > 0);
    let key = (salt_revision.to_owned(), generation.to_owned());
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(ProbeLock::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

/// Attest once per server/installation/environment generation. Every
/// identity and owner execution reads that same durable answer. No answer
/// is a typed transient failure before check admission, never NotAttested.
/// Tool versions behind unchanged exported values remain unobserved. Profile
/// changes require a new owner environment generation (server restart).
pub async fn attest(
    db: &db::SqliteDb,
    project: &BTreeMap<String, String>,
) -> Result<ServerCheckExecutionInputs> {
    let generation = server_environment_generation().to_owned();
    #[cfg(test)]
    let generation = {
        let installation = salt(db).await?;
        TEST_GENERATIONS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&installation)
            .cloned()
            .unwrap_or(generation)
    };
    #[cfg(any(test, feature = "test-support"))]
    let silent = {
        let installation = salt(db).await?;
        TEST_UNANSWERED
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&installation)
    };
    #[cfg(any(test, feature = "test-support"))]
    if silent {
        return attest_for_generation(db, project, &generation, || async {
            Err(check_executor::ProbeUnanswered::TimedOut)
        })
        .await;
    }
    attest_for_generation(
        db,
        project,
        &generation,
        check_executor::inherited_environment,
    )
    .await
}

async fn attest_for_generation<F, Fut>(
    db: &db::SqliteDb,
    project: &BTreeMap<String, String>,
    generation: &str,
    probe: F,
) -> Result<ServerCheckExecutionInputs>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<
        Output = std::result::Result<
            check_executor::InheritedEnvironment,
            check_executor::ProbeUnanswered,
        >,
    >,
{
    let salt = salt(db).await?;
    let salt_revision = canonical_digest_with_schema(
        "forge.check-attestation-salt/1",
        &serde_json::json!({ "salt": salt }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let read = || db.check_environment_attestation("server", generation, &salt_revision);
    let stored = match read().await? {
        Some(stored) => stored,
        None => {
            let lock = probe_lock(&salt_revision, generation);
            let _guard = lock.lock().await;
            // A racing identity builder may have supplied the one answer.
            match read().await? {
                Some(stored) => stored,
                None => {
                    #[cfg(test)]
                    let started = std::time::Instant::now();
                    let probed = probe().await;
                    #[cfg(test)]
                    probe_log::record(&salt, started.elapsed(), &probed);
                    let inherited = probed.map_err(|unanswered| {
                        tracing::warn!(%unanswered, "check environment attestation unavailable; retrying before identity admission");
                        ServiceError::Review(::review::ReviewError::CheckEnvironmentUnavailable)
                    })?;
                    db.store_check_environment_attestation(
                        "server",
                        generation,
                        &salt_revision,
                        &db::CheckEnvironmentAttestation {
                            inherited_revision: inherited_revision(&salt, &inherited)?,
                            shell_revision: shell_revision(&salt, &inherited.shell_revision)?,
                        },
                    )
                    .await?
                }
            }
        }
    };
    Ok(ServerCheckExecutionInputs {
        toolchain_revision: "path-not-probed".into(),
        environment_revision: environment_revision(&salt, &stored.inherited_revision, project)?,
        asset_revisions: BTreeMap::new(),
        secret_revisions: BTreeMap::new(),
        shell_revision: stored.shell_revision,
        runner_revision: format!("{CANONICAL_CI_POLICY}@{}", env!("CARGO_PKG_VERSION")),
    })
}

/// Every probe an installation's identity builds made, for a test's failure
/// message: how long it took, whether it answered, and which inherited
/// variables differ from the probe before it. Names only, never a value.
#[cfg(test)]
pub(crate) mod probe_log {
    use std::collections::BTreeMap;
    use std::hash::{Hash, Hasher};
    use std::sync::Mutex;

    struct Probe {
        elapsed_ms: u128,
        answer: Result<BTreeMap<String, u64>, String>,
    }
    static LOG: Mutex<Vec<(String, Probe)>> = Mutex::new(Vec::new());

    pub(super) fn record(
        installation: &str,
        elapsed: std::time::Duration,
        probed: &Result<check_executor::InheritedEnvironment, check_executor::ProbeUnanswered>,
    ) {
        let answer = match probed {
            Ok(inherited) => Ok(inherited
                .identity_values()
                .into_iter()
                .map(|(key, value)| {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    value.hash(&mut hasher);
                    (key.to_owned(), hasher.finish())
                })
                .chain([("(shell revision)".to_owned(), {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    inherited.shell_revision.hash(&mut hasher);
                    hasher.finish()
                })])
                .collect()),
            Err(unanswered) => Err(unanswered.to_string()),
        };
        LOG.lock().unwrap_or_else(|p| p.into_inner()).push((
            installation.to_owned(),
            Probe {
                elapsed_ms: elapsed.as_millis(),
                answer,
            },
        ));
    }

    /// One line per probe of this installation, in order.
    pub(crate) async fn count(db: &db::SqliteDb) -> usize {
        let installation = super::salt(db).await.unwrap_or_default();
        LOG.lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(of, _)| *of == installation)
            .count()
    }

    pub(crate) async fn describe(db: &db::SqliteDb) -> String {
        let installation = super::salt(db).await.unwrap_or_default();
        let log = LOG.lock().unwrap_or_else(|p| p.into_inner());
        let mut lines = Vec::new();
        let mut previous: Option<&BTreeMap<String, u64>> = None;
        for (_, probe) in log.iter().filter(|(of, _)| *of == installation) {
            lines.push(match &probe.answer {
                Err(unanswered) => format!("{} ms: no answer ({unanswered})", probe.elapsed_ms),
                Ok(values) => {
                    let differs = previous.map(|previous| {
                        previous
                            .keys()
                            .chain(values.keys())
                            .filter(|key| previous.get(*key) != values.get(*key))
                            .collect::<std::collections::BTreeSet<_>>()
                    });
                    previous = Some(values);
                    format!(
                        "{} ms: answered, {} variables, differs from the answer before in {:?}",
                        probe.elapsed_ms,
                        values.len(),
                        differs.unwrap_or_default()
                    )
                }
            });
        }
        lines.join("\n")
    }
}

/// The digest input of `spec` as its owner will execute it. `project` is the
/// Project environment in force now.
///
/// A canonical spec is reusable only when it is scoped to its worktree (the
/// run sees that worktree's ignored files) and the owner could attest its
/// environment. Otherwise the identity is unattested: the check runs and is
/// never reused.
pub async fn digest_input(
    db: &db::SqliteDb,
    spec: CheckSpec,
    project: &BTreeMap<String, String>,
) -> Result<CheckDigestInput> {
    let canonical = spec.execution_policy == CANONICAL_CI_POLICY;
    let mut environment = BTreeMap::new();
    for key in spec.commands.iter().flat_map(|c| c.environment_keys.iter()) {
        let value = match project.get(key) {
            Some(value) if canonical => {
                CheckEnvironmentValue::SecretRevision(value_revision(db, key, value).await?)
            }
            // The frozen policies inherit the owner's whole environment and
            // attest none of it: nothing is controlled, nothing is reused.
            _ => CheckEnvironmentValue::Volatile,
        };
        environment.insert(key.clone(), value);
    }
    let attested = if canonical && matches!(spec.scope, CheckScope::Workspace { .. }) {
        Some(attest(db, project).await?)
    } else {
        None
    };
    let environment_identity = match attested {
        Some(inputs) => inputs.identity().map_err(ServiceError::invalid_operation)?,
        None => CheckEnvironmentIdentity::NotAttested,
    };
    Ok(CheckDigestInput {
        spec,
        environment,
        environment_identity,
        execution_revision: CheckExecutionRevision {
            number: 0,
            audit_ref: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> db::SqliteDb {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        db::SqliteDb::new(pool)
    }

    fn inherited() -> check_executor::InheritedEnvironment {
        check_executor::InheritedEnvironment {
            values: BTreeMap::from([("PATH".to_owned(), "/fixture/bin".to_owned())]),
            shell_revision: "fixture-bash".to_owned(),
        }
    }

    #[tokio::test]
    async fn check_identity_reuses_the_stored_answer_without_waiting_for_an_absent_probe() {
        let db = store().await;
        let project = BTreeMap::new();
        let first =
            attest_for_generation(&db, &project, "generation", || async { Ok(inherited()) })
                .await
                .unwrap();
        let again = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            attest_for_generation(&db, &project, "generation", || async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                Err(check_executor::ProbeUnanswered::TimedOut)
            }),
        )
        .await
        .expect("a stored attestation must not launch or wait for another probe")
        .unwrap();
        assert_eq!(
            serde_json::to_value(first).unwrap(),
            serde_json::to_value(again).unwrap()
        );
    }

    #[tokio::test]
    async fn check_identity_refuses_an_absent_first_attestation() {
        let db = store().await;
        assert!(
            attest_for_generation(&db, &BTreeMap::new(), "generation", || async {
                Err(check_executor::ProbeUnanswered::TimedOut)
            })
            .await
            .is_err(),
            "an absent probe must fail typed, never build NotAttested"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn check_identity_concurrent_builders_probe_once_and_reopened_handles_reuse() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let db = Arc::new(store().await);
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut builders = Vec::new();
        for _ in 0..8 {
            let db = db.clone();
            let calls = calls.clone();
            let barrier = barrier.clone();
            builders.push(tokio::spawn(async move {
                barrier.wait().await;
                attest_for_generation(&db, &BTreeMap::new(), "generation", || async {
                    calls.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    Ok(inherited())
                })
                .await
                .unwrap()
            }));
        }
        let first = builders.remove(0).await.unwrap();
        for builder in builders {
            assert_eq!(builder.await.unwrap(), first);
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let reopened = db::SqliteDb::new(db.pool().clone());
        let again = attest_for_generation(&reopened, &BTreeMap::new(), "generation", || async {
            panic!("a new DB handle must read the stored answer")
        })
        .await
        .unwrap();
        assert_eq!(again, first);
    }

    #[tokio::test]
    async fn check_identity_retries_initialization_and_changes_only_with_stated_inputs() {
        let db = store().await;
        let none = BTreeMap::new();
        let failed = attest_for_generation(&db, &none, "generation", || async {
            Err(check_executor::ProbeUnanswered::TimedOut)
        })
        .await
        .unwrap_err();
        assert!(matches!(
            failed,
            ServiceError::Review(::review::ReviewError::CheckEnvironmentUnavailable)
        ));
        assert!(crate::worker_runtime::queue::retryable(&failed));
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_environment_attestation")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(rows, 0, "an absent answer never populates the cache");
        let first = attest_for_generation(&db, &none, "generation", || async { Ok(inherited()) })
            .await
            .unwrap();
        let changed = BTreeMap::from([("FLAVOR".to_owned(), "new-project-value".to_owned())]);
        let project_changed = attest_for_generation(&db, &changed, "generation", || async {
            panic!("Project values must be combined with the stored answer")
        })
        .await
        .unwrap();
        assert_ne!(
            first.environment_revision,
            project_changed.environment_revision
        );
        let new_generation = attest_for_generation(&db, &none, "new-generation", || async {
            let mut changed = inherited();
            changed.values.insert("PATH".into(), "/other/bin".into());
            Ok(changed)
        })
        .await
        .unwrap();
        assert_ne!(
            first.environment_revision,
            new_generation.environment_revision
        );
        let same_inputs = attest_for_generation(&db, &none, "restarted-generation", || async {
            Ok(inherited())
        })
        .await
        .unwrap();
        assert_eq!(
            same_inputs, first,
            "a restart with equal observed inputs preserves identity"
        );
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT inherited_revision,shell_revision FROM check_environment_attestation",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert!(rows
            .iter()
            .all(|(revision, _)| !revision.contains("/fixture/bin")
                && !revision.contains("/other/bin")));
    }

    /// Plan 3.4 F: the wrapper an operator's login environment names is in
    /// the environment revision, by value; so is one the Project sets.
    /// Forge's own per-run wrapper is in neither map (it is set on the
    /// run's command), so turning the shared cache on or off changes nothing
    /// here: `the_compiler_cache_wrapper_does_not_change_a_check_identity`.
    #[test]
    fn an_operator_or_project_compiler_wrapper_changes_the_environment_revision() {
        let inherited = |wrapper: Option<&str>| check_executor::InheritedEnvironment {
            values: wrapper
                .map(|value| ("RUSTC_WRAPPER".to_owned(), value.to_owned()))
                .into_iter()
                .chain([("PATH".to_owned(), "/usr/bin".to_owned())])
                .collect(),
            shell_revision: "bash".to_owned(),
        };
        let none = BTreeMap::new();
        let revision = |wrapper, project: &BTreeMap<String, String>| {
            environment_revision(
                "salt",
                &inherited_revision("salt", &inherited(wrapper)).unwrap(),
                project,
            )
            .unwrap()
        };
        let off = revision(None, &none);
        assert_eq!(off, revision(None, &none));
        let sccache = revision(Some("/operator/sccache"), &none);
        assert_ne!(sccache, off);
        assert_ne!(sccache, revision(Some("/operator/kache"), &none));
        let project = BTreeMap::from([("RUSTC_WRAPPER".to_owned(), String::new())]);
        assert_ne!(revision(None, &project), off);
    }
}
