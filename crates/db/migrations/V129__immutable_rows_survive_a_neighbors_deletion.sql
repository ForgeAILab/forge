-- Several append-only / immutable tables paired an `ON DELETE SET NULL`
-- foreign key with an unconditional `BEFORE UPDATE ... RAISE(ABORT)`
-- trigger on the very same column's table. `SET NULL` is a physical
-- UPDATE, so the moment the referenced row (a task, an execution, an
-- agent profile, a domain event, ...) was deleted independently of the
-- owning Project, SQLite's own cascade collided with the immutability
-- guard and aborted the whole delete -- most visibly, this broke Project
-- deletion for any Project that had ever produced a memory item, an Agent
-- Chat message with a profile/handoff, or similar.
--
-- These columns are historical references, not integrity-bearing edges:
-- an immutable row is meant to keep pointing at "the task this was about"
-- even after that task is gone, exactly like `domain_event` already does
-- with zero enforced foreign keys. The fix drops the FK enforcement on
-- each such column (it becomes a plain, unenforced reference column that
-- can dangle) rather than relaxing the immutability guarantee itself.
--
-- Two delete-side triggers had the same shape one level up: an
-- unconditional `BEFORE DELETE ... RAISE(ABORT)` on a table that also
-- cascades in from `agent_chat` or `project`. Those get the same
-- `project_deletion_guard`-scoped WHEN clause already used by
-- `agent_chat_message_immutable_delete` / `agent_handoff_immutable_delete`,
-- so the bounded Project teardown can still reach them.
--
-- Each table below is rebuilt via a TEMP-table stash rather than the usual
-- create-new/DROP-old/`ALTER TABLE ... RENAME TO` sequence: renaming a
-- table back onto a name with unrelated triggers on unrelated tables
-- (here, `agent_identity`'s profile-guard triggers) makes SQLite reload
-- its schema mid-batch under this driver's execution path, which fires an
-- unrelated INSERT this migration never issues. Landing the new table
-- directly under its final name sidesteps that reload entirely.
PRAGMA foreign_keys = OFF;

-- memory_item: task_id, execution_id, room_id, owner_identity_id,
-- publication_source_id, supersedes_id, source_event_id lose their FK.
-- project_id stays CASCADE: a memory item is scoped to its Project and is
-- meant to go with it.
DROP TRIGGER memory_item_ai;
DROP TRIGGER memory_item_ad;
DROP TRIGGER memory_item_immutable_update;

CREATE TEMP TABLE memory_item_stash AS SELECT * FROM memory_item;
DROP TABLE memory_item;

CREATE TABLE memory_item (
    row_id                 INTEGER PRIMARY KEY,
    id                     TEXT NOT NULL UNIQUE,
    project_id             TEXT REFERENCES project(id) ON DELETE CASCADE,
    task_id                TEXT,
    execution_id           TEXT,
    room_id                TEXT,
    scope_type             TEXT NOT NULL
                                CHECK (scope_type IN (
                                    'account', 'project', 'task', 'agent_chat'
                                )),
    scope_id               TEXT NOT NULL,
    visibility             TEXT NOT NULL DEFAULT 'project'
                                CHECK (visibility IN (
                                    'private', 'chat', 'project', 'account'
                                )),
    owner_identity_id      TEXT,
    authority              TEXT NOT NULL DEFAULT 'observation'
                                CHECK (authority IN (
                                    'observation', 'hypothesis', 'proposal', 'decision',
                                    'verified_fact', 'procedure'
                                )),
    sensitivity            TEXT NOT NULL DEFAULT 'internal'
                                CHECK (sensitivity IN ('public', 'internal', 'restricted', 'secret')),
    retention_priority     INTEGER NOT NULL DEFAULT 0,
    provenance_json        TEXT NOT NULL DEFAULT '{}',
    publication_source_id  TEXT,
    supersedes_id          TEXT,
    valid_from             TEXT,
    valid_until            TEXT,
    source_event_id        TEXT,
    source_scope_type      TEXT,
    source_scope_id        TEXT,
    source_revision        TEXT,
    source_room_sequence   INTEGER,
    source_type            TEXT NOT NULL,
    kind                   TEXT NOT NULL,
    title                  TEXT NOT NULL,
    summary                TEXT,
    body                   TEXT NOT NULL,
    metadata_json          TEXT NOT NULL DEFAULT '{}',
    confidence             TEXT,
    quality_score          INTEGER,
    created_by_type        TEXT,
    created_by_id          TEXT,
    created_at             TEXT NOT NULL
);

INSERT INTO memory_item SELECT * FROM memory_item_stash;
DROP TABLE memory_item_stash;

CREATE INDEX idx_memory_item_project ON memory_item(project_id);
CREATE INDEX idx_memory_item_task ON memory_item(task_id);
CREATE INDEX idx_memory_item_room ON memory_item(room_id);
CREATE INDEX idx_memory_item_scope
    ON memory_item(scope_type, scope_id, created_at DESC, id DESC);
CREATE INDEX idx_memory_item_owner ON memory_item(owner_identity_id, visibility);
CREATE INDEX idx_memory_item_authority
    ON memory_item(authority, retention_priority DESC, created_at DESC);
CREATE INDEX idx_memory_item_source_scope
    ON memory_item(source_scope_type, source_scope_id, source_room_sequence);
CREATE INDEX idx_memory_item_created_at ON memory_item(created_at);

CREATE TRIGGER memory_item_ai
AFTER INSERT ON memory_item
BEGIN
    INSERT INTO memory_item_fts(rowid, title, summary, body)
    VALUES (new.row_id, new.title, new.summary, new.body);
END;

CREATE TRIGGER memory_item_ad
AFTER DELETE ON memory_item
BEGIN
    INSERT INTO memory_item_fts(memory_item_fts, rowid, title, summary, body)
    VALUES ('delete', old.row_id, old.title, old.summary, old.body);
END;

CREATE TRIGGER memory_item_immutable_update
BEFORE UPDATE ON memory_item
BEGIN
    SELECT RAISE(ABORT, 'memory items are append-only');
END;

-- memory_lifecycle_assertion: related_memory_id, source_event_id lose their FK.
-- memory_item_id stays CASCADE (an assertion belongs to its memory item).
DROP TRIGGER memory_lifecycle_assertion_immutable_update;

CREATE TEMP TABLE memory_lifecycle_assertion_stash AS SELECT * FROM memory_lifecycle_assertion;
DROP TABLE memory_lifecycle_assertion;

CREATE TABLE memory_lifecycle_assertion (
    id                  TEXT PRIMARY KEY,
    memory_item_id      TEXT NOT NULL REFERENCES memory_item(id) ON DELETE CASCADE,
    assertion_type      TEXT NOT NULL
                            CHECK (assertion_type IN (
                                'published', 'promoted', 'superseded', 'retracted',
                                'disputed', 'expired', 'evidence'
                            )),
    related_memory_id   TEXT,
    reason              TEXT,
    evidence_json       TEXT NOT NULL DEFAULT '{}',
    asserted_by_type    TEXT NOT NULL,
    asserted_by_id      TEXT,
    source_event_id     TEXT,
    created_at          TEXT NOT NULL
);

INSERT INTO memory_lifecycle_assertion SELECT * FROM memory_lifecycle_assertion_stash;
DROP TABLE memory_lifecycle_assertion_stash;

CREATE INDEX idx_memory_lifecycle_item
    ON memory_lifecycle_assertion(memory_item_id, created_at ASC, id ASC);
CREATE INDEX idx_memory_lifecycle_relation
    ON memory_lifecycle_assertion(related_memory_id, assertion_type);

CREATE TRIGGER memory_lifecycle_assertion_immutable_update
BEFORE UPDATE ON memory_lifecycle_assertion
BEGIN
    SELECT RAISE(ABORT, 'memory lifecycle assertions are append-only');
END;

-- agent_chat_message: profile_id, handoff_id lose their FK. chat_id stays
-- CASCADE (a message belongs to its Agent Chat).
DROP TRIGGER agent_chat_message_immutable_delete;
DROP TRIGGER agent_chat_message_immutable_update;

CREATE TEMP TABLE agent_chat_message_stash AS SELECT * FROM agent_chat_message;
DROP TABLE agent_chat_message;

CREATE TABLE agent_chat_message (
    id                         TEXT PRIMARY KEY,
    chat_id                    TEXT NOT NULL REFERENCES agent_chat(id) ON DELETE CASCADE,
    sequence                   INTEGER NOT NULL CHECK (sequence >= 0),
    author_type                TEXT NOT NULL CHECK (author_type IN ('user', 'agent', 'system', 'handoff')),
    author_id                  TEXT,
    content                    TEXT NOT NULL,
    content_guard_json         TEXT NOT NULL DEFAULT '{}',
    sensitivity                TEXT NOT NULL DEFAULT 'internal'
                                   CHECK (sensitivity IN ('public', 'internal', 'restricted')),
    status                     TEXT NOT NULL CHECK (status IN ('complete', 'failed', 'cancelled')),
    outcome                    TEXT,
    model                      TEXT,
    profile_id                 TEXT,
    session_id                 TEXT,
    context_manifest_id        TEXT,
    token_usage_json           TEXT,
    duration_ms                INTEGER,
    error                      TEXT,
    correlation_id             TEXT NOT NULL,
    causation_id               TEXT,
    handoff_id                 TEXT,
    source_type                TEXT NOT NULL DEFAULT 'native'
                                   CHECK (source_type IN ('native', 'room', 'conversation', 'handoff')),
    source_id                  TEXT,
    source_message_id          TEXT,
    source_room_id             TEXT,
    source_conversation_id     TEXT,
    source_sequence            INTEGER,
    source_metadata_json       TEXT NOT NULL DEFAULT '{}',
    created_at                 TEXT NOT NULL,
    UNIQUE (chat_id, sequence),
    CHECK (error IS NULL OR length(error) <= 2048),
    CHECK (source_sequence IS NULL OR source_sequence >= 0)
);

INSERT INTO agent_chat_message SELECT * FROM agent_chat_message_stash;
DROP TABLE agent_chat_message_stash;

CREATE INDEX idx_agent_chat_message_chat_sequence
    ON agent_chat_message(chat_id, sequence ASC);
CREATE INDEX idx_agent_chat_message_source
    ON agent_chat_message(source_type, source_id, source_sequence);
CREATE INDEX idx_agent_chat_message_handoff
    ON agent_chat_message(handoff_id);

CREATE TRIGGER agent_chat_message_immutable_delete
BEFORE DELETE ON agent_chat_message
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    JOIN agent_chat c ON c.project_id = g.project_id
    WHERE c.id = OLD.chat_id
)
BEGIN
    SELECT RAISE(ABORT, 'Agent Chat messages are immutable');
END;

CREATE TRIGGER agent_chat_message_immutable_update
BEFORE UPDATE ON agent_chat_message
BEGIN
    SELECT RAISE(ABORT, 'Agent Chat messages are immutable');
END;

-- agent_profile: daemon_id loses its FK. identity_id stays CASCADE.
DROP TRIGGER agent_profile_immutable;
DROP TRIGGER agent_profile_credential_guard;

CREATE TEMP TABLE agent_profile_stash AS SELECT * FROM agent_profile;
DROP TABLE agent_profile;

CREATE TABLE agent_profile (
    id                         TEXT PRIMARY KEY,
    identity_id                TEXT NOT NULL REFERENCES agent_identity(id) ON DELETE CASCADE,
    backend_kind               TEXT NOT NULL CHECK (backend_kind IN ('cli', 'native')),
    executor_type              TEXT NOT NULL,
    provider                   TEXT,
    model                      TEXT,
    reasoning_effort           TEXT,
    permission_policy          TEXT,
    prompt_template            TEXT,
    capabilities_json          TEXT NOT NULL DEFAULT '[]',
    tool_policy_json           TEXT NOT NULL DEFAULT '{}',
    config_json                TEXT NOT NULL DEFAULT '{}',
    credential_ref             TEXT,
    daemon_id                  TEXT,
    version                    INTEGER NOT NULL DEFAULT 1,
    created_at                 TEXT NOT NULL,
    updated_at                 TEXT NOT NULL
);

INSERT INTO agent_profile SELECT * FROM agent_profile_stash;
DROP TABLE agent_profile_stash;

CREATE INDEX idx_agent_profile_identity
    ON agent_profile(identity_id, created_at DESC, id DESC);
CREATE INDEX idx_agent_profile_executor
    ON agent_profile(executor_type, backend_kind);

CREATE TRIGGER agent_profile_immutable
BEFORE UPDATE ON agent_profile
BEGIN
    SELECT RAISE(ABORT, 'agent profiles are immutable');
END;

CREATE TRIGGER agent_profile_credential_guard
BEFORE INSERT ON agent_profile
WHEN NEW.credential_ref IS NOT NULL
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM credential_handle WHERE id = NEW.credential_ref
        )
        THEN RAISE(ABORT, 'agent profile credential handle does not exist')
    END;
END;

-- context_manifest: agent_session_id loses its FK. identity_id and
-- context_scope_id stay CASCADE.
DROP TRIGGER context_manifest_immutable_update;
DROP TRIGGER context_manifest_reject_legacy_room_insert;
DROP TRIGGER context_manifest_reject_legacy_room_update;
DROP TRIGGER context_manifest_immutable_delete;

CREATE TEMP TABLE context_manifest_stash AS SELECT * FROM context_manifest;
DROP TABLE context_manifest;

CREATE TABLE context_manifest (
    id                       TEXT PRIMARY KEY,
    identity_id              TEXT NOT NULL REFERENCES agent_identity(id) ON DELETE CASCADE,
    agent_session_id         TEXT,
    context_scope_id         TEXT NOT NULL REFERENCES agent_context_scope(id) ON DELETE CASCADE,
    scope_type               TEXT NOT NULL CHECK (scope_type IN (
                                 'account', 'project', 'room', 'task', 'agent_chat'
                             )),
    scope_id                 TEXT NOT NULL,
    policy_revision          TEXT NOT NULL,
    domain_revision          TEXT NOT NULL,
    lcm_binding_revision     TEXT,
    runtime_manifest_id      TEXT,
    runtime_manifest_fingerprint TEXT,
    combined_fingerprint     TEXT NOT NULL,
    request_fingerprint      TEXT NOT NULL,
    created_at               TEXT NOT NULL
);

INSERT INTO context_manifest SELECT * FROM context_manifest_stash;
DROP TABLE context_manifest_stash;

CREATE TRIGGER context_manifest_immutable_update
BEFORE UPDATE ON context_manifest
BEGIN
    SELECT RAISE(ABORT, 'context manifests are immutable');
END;

CREATE TRIGGER context_manifest_reject_legacy_room_insert
BEFORE INSERT ON context_manifest
WHEN NEW.scope_type = 'room'
BEGIN
    SELECT RAISE(ABORT, 'Room scopes are retired; use an Agent Chat scope');
END;

CREATE TRIGGER context_manifest_reject_legacy_room_update
BEFORE UPDATE OF scope_type, scope_id ON context_manifest
WHEN NEW.scope_type = 'room'
BEGIN
    SELECT RAISE(ABORT, 'Room scopes are retired; use an Agent Chat scope');
END;

CREATE TRIGGER context_manifest_immutable_delete
BEFORE DELETE ON context_manifest
WHEN EXISTS (
    SELECT 1 FROM agent_context_scope AS scope
    WHERE scope.id = OLD.context_scope_id
)
BEGIN
    SELECT RAISE(ABORT, 'context manifests are immutable');
END;

-- agent_handoff: author_identity_id loses its FK. source_chat_id and
-- target_chat_id stay CASCADE.
DROP TRIGGER agent_handoff_immutable_delete;
DROP TRIGGER agent_handoff_immutable_update;

CREATE TEMP TABLE agent_handoff_stash AS SELECT * FROM agent_handoff;
DROP TABLE agent_handoff;

CREATE TABLE agent_handoff (
    id                         TEXT PRIMARY KEY,
    source_chat_id             TEXT NOT NULL REFERENCES agent_chat(id) ON DELETE CASCADE,
    target_chat_id             TEXT NOT NULL REFERENCES agent_chat(id) ON DELETE CASCADE,
    source_message_id          TEXT,
    source_turn_job_id         TEXT,
    target_message_id          TEXT,
    target_turn_job_id         TEXT,
    author_identity_id         TEXT,
    content                    TEXT NOT NULL,
    content_guard_json         TEXT NOT NULL DEFAULT '{}',
    source_revisions_json      TEXT NOT NULL DEFAULT '[]',
    status                     TEXT NOT NULL DEFAULT 'pending'
                                   CHECK (status IN ('pending', 'delivered', 'failed', 'cancelled')),
    error_code                 TEXT,
    correlation_id             TEXT NOT NULL,
    causation_id               TEXT,
    dedupe_key                 TEXT NOT NULL UNIQUE,
    created_at                 TEXT NOT NULL,
    updated_at                 TEXT NOT NULL,
    CHECK (source_chat_id != target_chat_id),
    CHECK (error_code IS NULL OR length(error_code) <= 128)
);

INSERT INTO agent_handoff SELECT * FROM agent_handoff_stash;
DROP TABLE agent_handoff_stash;

CREATE INDEX idx_agent_handoff_target
    ON agent_handoff(target_chat_id, created_at ASC, id ASC);
CREATE INDEX idx_agent_handoff_source
    ON agent_handoff(source_chat_id, created_at ASC, id ASC);

CREATE TRIGGER agent_handoff_immutable_delete
BEFORE DELETE ON agent_handoff
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    JOIN agent_chat c ON c.project_id = g.project_id
    WHERE c.id = OLD.source_chat_id OR c.id = OLD.target_chat_id
)
BEGIN
    SELECT RAISE(ABORT, 'Agent handoffs are immutable');
END;

CREATE TRIGGER agent_handoff_immutable_update
BEFORE UPDATE ON agent_handoff
BEGIN
    SELECT RAISE(ABORT, 'Agent handoffs are immutable');
END;

-- agent_chat_topic: starting_message_id loses its FK, and the delete guard
-- gains the same project_deletion_guard WHEN clause agent_chat_message and
-- agent_handoff already use, since a topic cascades from its Agent Chat.
DROP TRIGGER agent_chat_topic_immutable_update;
DROP TRIGGER agent_chat_topic_immutable_delete;

CREATE TEMP TABLE agent_chat_topic_stash AS SELECT * FROM agent_chat_topic;
DROP TABLE agent_chat_topic;

CREATE TABLE agent_chat_topic (
    id                          TEXT PRIMARY KEY,
    chat_id                     TEXT NOT NULL REFERENCES agent_chat(id) ON DELETE CASCADE,
    sequence                    INTEGER NOT NULL CHECK (sequence >= 0),
    label                       TEXT NOT NULL CHECK (length(label) BETWEEN 1 AND 200),
    summary                     TEXT CHECK (summary IS NULL OR length(summary) <= 2000),
    starting_message_id         TEXT,
    starting_message_sequence   INTEGER NOT NULL CHECK (starting_message_sequence >= 0),
    principal_type               TEXT NOT NULL CHECK (principal_type IN ('user', 'system')),
    principal_id                TEXT,
    created_at                  TEXT NOT NULL,
    UNIQUE (chat_id, sequence)
);

INSERT INTO agent_chat_topic SELECT * FROM agent_chat_topic_stash;
DROP TABLE agent_chat_topic_stash;

CREATE INDEX idx_agent_chat_topic_chat
    ON agent_chat_topic(chat_id, sequence ASC);

CREATE TRIGGER agent_chat_topic_immutable_update
BEFORE UPDATE ON agent_chat_topic
BEGIN
    SELECT RAISE(ABORT, 'Main Chat topics are immutable');
END;

CREATE TRIGGER agent_chat_topic_immutable_delete
BEFORE DELETE ON agent_chat_topic
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    JOIN agent_chat c ON c.project_id = g.project_id
    WHERE c.id = OLD.chat_id
)
BEGIN
    SELECT RAISE(ABORT, 'Main Chat topics are immutable');
END;

-- project_release_media_pin: legacy_task_media_id loses its FK.
DROP TRIGGER project_release_media_pin_scope_guard;
DROP TRIGGER project_release_media_pin_gc_guard_after_insert;
DROP TRIGGER project_release_media_pin_immutable_update;
DROP TRIGGER project_release_media_pin_immutable_delete;

CREATE TEMP TABLE project_release_media_pin_stash AS SELECT * FROM project_release_media_pin;
DROP TABLE project_release_media_pin;

CREATE TABLE project_release_media_pin (
    id                    TEXT PRIMARY KEY,
    project_id            TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    release_id            TEXT NOT NULL REFERENCES project_release(id) ON DELETE RESTRICT,
    asset_id              TEXT NOT NULL REFERENCES media_asset(id) ON DELETE RESTRICT,
    attachment_id         TEXT REFERENCES project_media_attachment(id) ON DELETE RESTRICT,
    legacy_task_media_id  TEXT,
    asset_checksum        TEXT NOT NULL CHECK (length(trim(asset_checksum)) > 0),
    attachment_digest     TEXT NOT NULL CHECK (length(trim(attachment_digest)) > 0),
    availability          TEXT NOT NULL DEFAULT 'available'
                              CHECK (availability IN ('available', 'quarantined', 'redacted', 'purged')),
    pin_digest            TEXT NOT NULL,
    created_at            TEXT NOT NULL,
    UNIQUE (release_id, asset_id, attachment_id)
);

INSERT INTO project_release_media_pin SELECT * FROM project_release_media_pin_stash;
DROP TABLE project_release_media_pin_stash;

CREATE UNIQUE INDEX idx_project_release_media_pin_identity
    ON project_release_media_pin(release_id, asset_id, COALESCE(attachment_id, ''));
CREATE INDEX idx_project_release_media_pin_asset
    ON project_release_media_pin(asset_id, availability, created_at DESC);
CREATE INDEX idx_project_release_media_pin_release
    ON project_release_media_pin(release_id, id);

CREATE TRIGGER project_release_media_pin_scope_guard
BEFORE INSERT ON project_release_media_pin
BEGIN
    SELECT CASE
        WHEN length(trim(NEW.asset_checksum)) = 0
        THEN RAISE(ABORT, 'Release media pin asset checksum is required')
        WHEN length(trim(NEW.attachment_digest)) = 0
        THEN RAISE(ABORT, 'Release media pin attachment digest is required')
        WHEN NOT EXISTS (SELECT 1 FROM project_release WHERE id = NEW.release_id AND project_id = NEW.project_id)
        THEN RAISE(ABORT, 'Release media pin release is cross-Project')
        WHEN NOT EXISTS (SELECT 1 FROM media_asset WHERE id = NEW.asset_id AND project_id = NEW.project_id)
        THEN RAISE(ABORT, 'Release media pin asset is cross-Project')
        WHEN EXISTS (
            SELECT 1 FROM media_asset
            WHERE id = NEW.asset_id AND project_id = NEW.project_id
              AND checksum IS NOT NULL AND checksum != NEW.asset_checksum
        ) THEN RAISE(ABORT, 'Release media pin asset checksum does not match asset')
        WHEN EXISTS (
            SELECT 1 FROM media_asset
            WHERE id = NEW.asset_id
              AND (gc_state IN ('gc_queued', 'deleted') OR availability = 'purged')
        ) THEN RAISE(ABORT, 'Release media pin asset is unavailable')
        WHEN NEW.attachment_id IS NOT NULL
         AND NOT EXISTS (
             SELECT 1 FROM project_media_attachment
             WHERE id = NEW.attachment_id AND asset_id = NEW.asset_id AND project_id = NEW.project_id
         ) THEN RAISE(ABORT, 'Release media pin attachment is cross-Project')
        WHEN NEW.attachment_id IS NOT NULL
         AND NOT EXISTS (
             SELECT 1 FROM project_media_attachment
             WHERE id = NEW.attachment_id
               AND asset_id = NEW.asset_id
               AND project_id = NEW.project_id
               AND deleted_at IS NULL
               AND availability != 'purged'
         ) THEN RAISE(ABORT, 'Release media pin attachment is unavailable')
    END;
END;

CREATE TRIGGER project_release_media_pin_gc_guard_after_insert
AFTER INSERT ON project_release_media_pin
BEGIN
    UPDATE media_asset
    SET gc_state = 'referenced', gc_candidate_at = NULL, deleted_at = NULL,
        gc_lease_owner = NULL, gc_lease_expires_at = NULL,
        version = version + 1, updated_at = NEW.created_at
    WHERE id = NEW.asset_id;
END;

CREATE TRIGGER project_release_media_pin_immutable_update
BEFORE UPDATE ON project_release_media_pin
BEGIN
    SELECT RAISE(ABORT, 'Release media pins are immutable');
END;

CREATE TRIGGER project_release_media_pin_immutable_delete
BEFORE DELETE ON project_release_media_pin
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g WHERE g.project_id = OLD.project_id
)
BEGIN
    SELECT RAISE(ABORT, 'Release media pins are immutable');
END;

-- forge_memory_source_binding: no column shape change, only its delete
-- guard gains the same project_deletion_guard WHEN clause, since it
-- cascades in from project (and, unused today, from task/legacy_room too).
DROP TRIGGER forge_memory_source_binding_immutable_delete;

CREATE TRIGGER forge_memory_source_binding_immutable_delete
BEFORE DELETE ON forge_memory_source_binding
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    WHERE g.project_id = OLD.project_id
       OR g.project_id = (SELECT t.project_id FROM task t WHERE t.id = OLD.task_id)
       OR g.project_id = (SELECT r.owning_project_id FROM legacy_room r WHERE r.id = OLD.room_id)
)
BEGIN
    SELECT RAISE(ABORT, 'memory source bindings are immutable');
END;

PRAGMA foreign_keys = ON;
