-- No persistent running flag: restart retries an interrupted job after its
-- deadline. The clone operation itself is idempotent on its owning daemon.
CREATE TABLE repo_provision_retry (
    repo_id TEXT NOT NULL REFERENCES repo(id) ON DELETE CASCADE,
    runtime_id TEXT NOT NULL REFERENCES runtime(id) ON DELETE CASCADE,
    location_id TEXT REFERENCES repo_location(id) ON DELETE SET NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    checks_digest TEXT NOT NULL DEFAULT '',
    connection_id INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    PRIMARY KEY (repo_id, runtime_id)
);

-- The newly defaulted check scope participates in canonical serialization.
-- Recompute digests at startup without changing existing verdicts or pauses:
-- all checks previously accepted by this build default to workspace scope.
-- Leaving the old digest would strand a paused Project's scheduled re-check.
UPDATE project_machine_readiness SET checks_digest = '';
