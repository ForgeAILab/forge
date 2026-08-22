-- A single visible Main Chat message may first run under the account baseline
-- and then continue under Product Genesis after a typed genesis.start control
-- transfer. Preserve every existing turn while removing only the historical
-- one-turn-per-trigger restriction; dedupe_key remains the replay boundary.

PRAGMA foreign_keys = OFF;

CREATE TABLE agent_chat_turn_job_new (
    id                                  TEXT PRIMARY KEY,
    chat_id                             TEXT NOT NULL REFERENCES agent_chat(id) ON DELETE CASCADE,
    triggering_message_id               TEXT NOT NULL REFERENCES agent_chat_message(id) ON DELETE CASCADE,
    responder_identity_id               TEXT REFERENCES agent_identity(id) ON DELETE SET NULL,
    profile_id                          TEXT REFERENCES agent_profile(id) ON DELETE SET NULL,
    canonical_scope_type                TEXT NOT NULL CHECK (canonical_scope_type = 'agent_chat'),
    canonical_scope_id                  TEXT NOT NULL,
    status                              TEXT NOT NULL DEFAULT 'queued'
                                            CHECK (status IN (
                                                'queued', 'leased', 'awaiting_input', 'retry_wait',
                                                'succeeded', 'failed', 'cancelled'
                                            )),
    pending_interaction_id              TEXT REFERENCES protected_interaction(id) ON DELETE SET NULL,
    dedupe_key                          TEXT NOT NULL UNIQUE,
    lease_owner                         TEXT,
    leased_until                        TEXT,
    attempt_count                       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts                        INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 16),
    next_attempt_at                     TEXT,
    response_message_id                 TEXT REFERENCES agent_chat_message(id) ON DELETE SET NULL,
    error_code                          TEXT,
    error_message                       TEXT,
    correlation_id                      TEXT NOT NULL,
    causation_id                        TEXT,
    causation_depth                     INTEGER NOT NULL DEFAULT 0 CHECK (causation_depth BETWEEN 0 AND 16),
    version                             INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at                          TEXT NOT NULL,
    updated_at                          TEXT NOT NULL,
    responder_binding_id                TEXT,
    responder_binding_version           INTEGER CHECK (responder_binding_version IS NULL OR responder_binding_version >= 1),
    responder_identity_version          INTEGER CHECK (responder_identity_version IS NULL OR responder_identity_version >= 1),
    profile_version                     INTEGER CHECK (profile_version IS NULL OR profile_version >= 1),
    operating_skill_revision_id         TEXT,
    policy_revision                     TEXT,
    policy_digest                       TEXT,
    permission_policy_digest            TEXT,
    tool_policy_digest                  TEXT,
    admission_digest                    TEXT,
    canonical_scope_provenance_json     TEXT CHECK (
                                             canonical_scope_provenance_json IS NULL
                                             OR json_valid(canonical_scope_provenance_json)
                                         ),
    CHECK (canonical_scope_id = chat_id),
    CHECK (error_code IS NULL OR length(error_code) <= 128),
    CHECK (error_message IS NULL OR length(error_message) <= 2048)
);

INSERT INTO agent_chat_turn_job_new (
    id, chat_id, triggering_message_id, responder_identity_id, profile_id,
    canonical_scope_type, canonical_scope_id, status, pending_interaction_id,
    dedupe_key, lease_owner, leased_until, attempt_count, max_attempts,
    next_attempt_at, response_message_id, error_code, error_message,
    correlation_id, causation_id, causation_depth, version, created_at,
    updated_at, responder_binding_id, responder_binding_version,
    responder_identity_version, profile_version, operating_skill_revision_id,
    policy_revision, policy_digest, permission_policy_digest,
    tool_policy_digest, admission_digest, canonical_scope_provenance_json
)
SELECT
    id, chat_id, triggering_message_id, responder_identity_id, profile_id,
    canonical_scope_type, canonical_scope_id, status, pending_interaction_id,
    dedupe_key, lease_owner, leased_until, attempt_count, max_attempts,
    next_attempt_at, response_message_id, error_code, error_message,
    correlation_id, causation_id, causation_depth, version, created_at,
    updated_at, responder_binding_id, responder_binding_version,
    responder_identity_version, profile_version, operating_skill_revision_id,
    policy_revision, policy_digest, permission_policy_digest,
    tool_policy_digest, admission_digest, canonical_scope_provenance_json
FROM agent_chat_turn_job;

DROP TABLE agent_chat_turn_job;
ALTER TABLE agent_chat_turn_job_new RENAME TO agent_chat_turn_job;

CREATE INDEX idx_agent_chat_turn_dispatch
    ON agent_chat_turn_job(status, next_attempt_at, created_at, id);
CREATE INDEX idx_agent_chat_turn_chat
    ON agent_chat_turn_job(chat_id, created_at ASC, id ASC);
CREATE INDEX idx_agent_chat_turn_trigger
    ON agent_chat_turn_job(chat_id, triggering_message_id, created_at ASC, id ASC);
CREATE UNIQUE INDEX idx_agent_chat_turn_active_lease
    ON agent_chat_turn_job(chat_id)
    WHERE status = 'leased';
CREATE INDEX idx_agent_chat_turn_pending_interaction
    ON agent_chat_turn_job(pending_interaction_id)
    WHERE pending_interaction_id IS NOT NULL;

PRAGMA foreign_keys = ON;
