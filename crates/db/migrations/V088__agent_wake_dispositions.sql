-- Durable wake delivery outcomes and the Gate C consumer cutover.
--
-- A source event may be reconsidered after a deferred/setup-required outcome,
-- so disposition history is append-only.  `agent_wake_disposition_current`
-- is the one mutable pointer for a (consumer, source event) and keeps the
-- source-event/consumer identity unique without rewriting historical
-- provenance.

CREATE TABLE agent_wake_disposition (
    id                           TEXT PRIMARY KEY CHECK (length(trim(id)) > 0),
    consumer_name                TEXT NOT NULL CHECK (length(trim(consumer_name)) > 0),
    source_event_id              TEXT NOT NULL REFERENCES domain_event(id) ON DELETE RESTRICT,
    source_event_sequence        INTEGER NOT NULL REFERENCES domain_event(sequence) ON DELETE RESTRICT,
    attempt_number               INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 16),
    max_attempts                 INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 16),
    disposition                  TEXT NOT NULL CHECK (disposition IN (
                                     'turn_admitted',
                                     'deterministically_suppressed',
                                     'deferred',
                                     'setup_required'
                                 )),
    reason                       TEXT NOT NULL CHECK (length(trim(reason)) > 0 AND length(reason) <= 512),
    turn_job_id                  TEXT REFERENCES agent_chat_turn_job(id) ON DELETE RESTRICT,
    attention_id                 TEXT REFERENCES attention_projection(id) ON DELETE RESTRICT,
    retry_at                     TEXT,
    incident_key                 TEXT CHECK (incident_key IS NULL OR length(incident_key) <= 256),
    incident_digest              TEXT CHECK (incident_digest IS NULL OR length(incident_digest) <= 256),
    binding_id                   TEXT CHECK (binding_id IS NULL OR length(trim(binding_id)) BETWEEN 1 AND 256),
    binding_version              INTEGER CHECK (binding_version IS NULL OR binding_version >= 1),
    profile_id                   TEXT CHECK (profile_id IS NULL OR length(trim(profile_id)) BETWEEN 1 AND 256),
    profile_version              INTEGER CHECK (profile_version IS NULL OR profile_version >= 1),
    provenance_json              TEXT CHECK (
                                     provenance_json IS NULL
                                     OR (length(provenance_json) <= 8192 AND json_valid(provenance_json))
                                 ),
    parent_disposition_id        TEXT REFERENCES agent_wake_disposition(id) ON DELETE RESTRICT,
    created_at                   TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at                   TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    version                      INTEGER NOT NULL DEFAULT 1 CHECK (version = 1),
    CHECK (attempt_number <= max_attempts),
    CHECK (
        (disposition = 'turn_admitted' AND turn_job_id IS NOT NULL
            AND attention_id IS NULL AND retry_at IS NULL)
        OR (disposition = 'deterministically_suppressed'
            AND turn_job_id IS NULL AND attention_id IS NULL AND retry_at IS NULL)
        OR (disposition = 'deferred' AND turn_job_id IS NULL
            AND attention_id IS NULL AND retry_at IS NOT NULL)
        OR (disposition = 'setup_required' AND turn_job_id IS NULL
            AND attention_id IS NOT NULL AND retry_at IS NULL)
    ),
    UNIQUE (consumer_name, source_event_id, attempt_number)
);

CREATE INDEX idx_agent_wake_disposition_source
    ON agent_wake_disposition(source_event_id, consumer_name, attempt_number);
CREATE INDEX idx_agent_wake_disposition_parent
    ON agent_wake_disposition(parent_disposition_id)
    WHERE parent_disposition_id IS NOT NULL;

CREATE TRIGGER agent_wake_disposition_event_identity_guard
BEFORE INSERT ON agent_wake_disposition
WHEN NOT EXISTS (
    SELECT 1
    FROM domain_event
    WHERE id = NEW.source_event_id
      AND sequence = NEW.source_event_sequence
)
BEGIN
    SELECT RAISE(ABORT, 'wake disposition source event identity does not match sequence');
END;

CREATE TRIGGER agent_wake_disposition_immutable_update
BEFORE UPDATE ON agent_wake_disposition
BEGIN
    SELECT RAISE(ABORT, 'wake dispositions are immutable attempts');
END;

CREATE TABLE agent_wake_disposition_current (
    consumer_name                TEXT NOT NULL CHECK (length(trim(consumer_name)) > 0),
    source_event_id              TEXT NOT NULL REFERENCES domain_event(id) ON DELETE RESTRICT,
    disposition_id               TEXT NOT NULL UNIQUE REFERENCES agent_wake_disposition(id) ON DELETE RESTRICT,
    attempt_number               INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 16),
    updated_at                   TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    version                      INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    PRIMARY KEY (consumer_name, source_event_id)
);

CREATE INDEX idx_agent_wake_disposition_due
    ON agent_wake_disposition_current(consumer_name, updated_at, attempt_number);

-- A wake incident is leased globally for its canonical scope/incident, not
-- once per historical responder identity.  Rebinding while an old lease is
-- still live must therefore fail closed rather than allowing two workers to
-- process the same incident in parallel.  The lease row for the same
-- identity is exempted on update so a worker may renew its own lease.
CREATE TRIGGER agent_wake_lease_incident_global_insert
BEFORE INSERT ON agent_wake_lease
WHEN EXISTS (
    SELECT 1
    FROM agent_wake_lease AS existing
    WHERE existing.scope_type = NEW.scope_type
      AND existing.scope_id = NEW.scope_id
      AND existing.incident_key = NEW.incident_key
      AND existing.identity_id != NEW.identity_id
      AND existing.leased_until > NEW.updated_at
)
BEGIN
    SELECT RAISE(ABORT, 'wake incident already has an active lease');
END;

CREATE TRIGGER agent_wake_lease_incident_global_update
BEFORE UPDATE OF identity_id, scope_type, scope_id, incident_key,
    leased_until, updated_at ON agent_wake_lease
WHEN EXISTS (
    SELECT 1
    FROM agent_wake_lease AS existing
    WHERE existing.scope_type = NEW.scope_type
      AND existing.scope_id = NEW.scope_id
      AND existing.incident_key = NEW.incident_key
      AND existing.identity_id != OLD.identity_id
      AND existing.leased_until > NEW.updated_at
)
BEGIN
    SELECT RAISE(ABORT, 'wake incident already has an active lease');
END;

-- Install-time cutover is captured while the migration's IMMEDIATE
-- transaction holds the write lock.  Events appended after this statement
-- therefore receive a larger sequence and remain visible to the consumer;
-- runtime must never lazily replace this row with MAX(domain_event.sequence).
CREATE TABLE event_consumer_cutover (
    consumer_name                TEXT PRIMARY KEY CHECK (length(trim(consumer_name)) > 0),
    cutover_sequence             INTEGER NOT NULL CHECK (cutover_sequence >= 0),
    reason                       TEXT NOT NULL CHECK (length(trim(reason)) > 0 AND length(reason) <= 256),
    created_at                   TEXT NOT NULL CHECK (length(trim(created_at)) > 0)
);

CREATE TRIGGER event_consumer_cutover_immutable_update
BEFORE UPDATE ON event_consumer_cutover
BEGIN
    SELECT RAISE(ABORT, 'event consumer cutovers are immutable');
END;

CREATE TRIGGER event_consumer_cutover_immutable_delete
BEFORE DELETE ON event_consumer_cutover
BEGIN
    SELECT RAISE(ABORT, 'event consumer cutovers are immutable');
END;

INSERT INTO event_consumer_cutover (
    consumer_name, cutover_sequence, reason, created_at
)
SELECT
    'agent-wake-turns',
    COALESCE((SELECT MAX(sequence) FROM domain_event), 0),
    'agent-wake-turns-install-cutover',
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE NOT EXISTS (
    SELECT 1 FROM event_consumer_cutover WHERE consumer_name = 'agent-wake-turns'
);

INSERT INTO event_consumer_cursor (
    consumer_name, last_sequence, version, updated_at
)
SELECT
    cutover.consumer_name,
    cutover.cutover_sequence,
    1,
    cutover.created_at
FROM event_consumer_cutover AS cutover
WHERE NOT EXISTS (
    SELECT 1 FROM event_consumer_cursor
    WHERE consumer_name = cutover.consumer_name
);

-- An upgrade may already have a cursor row created by an earlier runtime.
-- Pre-install events must not replay, so move only a lagging cursor to the
-- immutable install cutover while preserving a cursor that is already ahead.
UPDATE event_consumer_cursor
SET last_sequence = (
        SELECT cutover.cutover_sequence
        FROM event_consumer_cutover AS cutover
        WHERE cutover.consumer_name = event_consumer_cursor.consumer_name
    ),
    version = version + 1,
    updated_at = (
        SELECT cutover.created_at
        FROM event_consumer_cutover AS cutover
        WHERE cutover.consumer_name = event_consumer_cursor.consumer_name
    )
WHERE consumer_name = 'agent-wake-turns'
  AND last_sequence < (
        SELECT cutover.cutover_sequence
        FROM event_consumer_cutover AS cutover
        WHERE cutover.consumer_name = event_consumer_cursor.consumer_name
    );

-- Turn admission freezes current responder provenance.  Existing queued jobs
-- retain NULLs and remain processable conservatively; new admission paths
-- populate the fields before the job becomes runnable.
ALTER TABLE agent_chat_turn_job ADD COLUMN responder_binding_id TEXT;
ALTER TABLE agent_chat_turn_job ADD COLUMN responder_binding_version INTEGER
    CHECK (responder_binding_version IS NULL OR responder_binding_version >= 1);
ALTER TABLE agent_chat_turn_job ADD COLUMN responder_identity_version INTEGER
    CHECK (responder_identity_version IS NULL OR responder_identity_version >= 1);
ALTER TABLE agent_chat_turn_job ADD COLUMN profile_version INTEGER
    CHECK (profile_version IS NULL OR profile_version >= 1);
ALTER TABLE agent_chat_turn_job ADD COLUMN operating_skill_revision_id TEXT
    CHECK (operating_skill_revision_id IS NULL OR length(trim(operating_skill_revision_id)) BETWEEN 1 AND 256);
ALTER TABLE agent_chat_turn_job ADD COLUMN policy_revision TEXT
    CHECK (policy_revision IS NULL OR length(policy_revision) <= 256);
ALTER TABLE agent_chat_turn_job ADD COLUMN policy_digest TEXT
    CHECK (policy_digest IS NULL OR length(policy_digest) <= 256);
ALTER TABLE agent_chat_turn_job ADD COLUMN permission_policy_digest TEXT
    CHECK (permission_policy_digest IS NULL OR length(permission_policy_digest) <= 256);
ALTER TABLE agent_chat_turn_job ADD COLUMN tool_policy_digest TEXT
    CHECK (tool_policy_digest IS NULL OR length(tool_policy_digest) <= 256);
ALTER TABLE agent_chat_turn_job ADD COLUMN admission_digest TEXT
    CHECK (admission_digest IS NULL OR length(trim(admission_digest)) BETWEEN 1 AND 256);
ALTER TABLE agent_chat_turn_job ADD COLUMN canonical_scope_provenance_json TEXT
    CHECK (
        canonical_scope_provenance_json IS NULL
        OR (length(canonical_scope_provenance_json) <= 8192
            AND json_valid(canonical_scope_provenance_json))
    );
