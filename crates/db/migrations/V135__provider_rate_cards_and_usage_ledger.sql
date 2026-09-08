-- Provider pricing and usage accounting persistence.
--
-- This migration adds the forward-looking storage and performs the
-- data-preserving V022 execution-usage import.  Each historical aggregate is
-- retained as one legacy candidate, settled invocation, and usage event.  The
-- old source table is dropped only after the copy and typed validation succeed,
-- so the ledger becomes the sole execution-usage authority at the cutover.
--
-- Source/provider identifiers below are data, not integrity-bearing edges.
-- Provider entries and CLI runtimes may be revoked or removed while their
-- non-secret pricing subject revisions, admitted selections, and usage
-- provenance remain readable.  Project/account ownership edges are retained
-- only for rows that belong to those scopes and are allowed to cascade with
-- the existing deletion contracts.

-- A catalog snapshot is immutable.  The mutable state/pointer below records
-- the last successful check and the active last-known-good snapshot, so a
-- 304 or a failed refresh never edits an immutable snapshot.
CREATE TABLE pricing_catalog_snapshot (
    id                  TEXT NOT NULL PRIMARY KEY,
    source_kind         TEXT NOT NULL
                            CHECK (source_kind IN ('models_dev_catalog')),
    source_url          TEXT NOT NULL
                            CHECK (source_url = 'https://models.dev/api.json'),
    http_etag           TEXT,
    payload_sha256      TEXT NOT NULL CHECK (length(payload_sha256) = 64),
    parser_revision     TEXT NOT NULL CHECK (length(trim(parser_revision)) > 0),
    revision_digest     TEXT NOT NULL UNIQUE CHECK (length(trim(revision_digest)) > 0),
    payload_json        TEXT NOT NULL CHECK (json_valid(payload_json)),
    fetched_at          TEXT NOT NULL CHECK (length(trim(fetched_at)) > 0),
    created_at          TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    UNIQUE (source_kind, payload_sha256, parser_revision)
);

CREATE TRIGGER pricing_catalog_snapshot_immutable_update
BEFORE UPDATE ON pricing_catalog_snapshot
BEGIN
    SELECT RAISE(ABORT, 'pricing catalog snapshots are immutable');
END;

CREATE TRIGGER pricing_catalog_snapshot_immutable_delete
BEFORE DELETE ON pricing_catalog_snapshot
BEGIN
    SELECT RAISE(ABORT, 'pricing catalog snapshots are immutable');
END;

CREATE TABLE pricing_catalog_state (
    id                      TEXT NOT NULL PRIMARY KEY
                                CHECK (id = 'models_dev_catalog'),
    active_snapshot_id      TEXT REFERENCES pricing_catalog_snapshot(id) ON DELETE RESTRICT,
    state                   TEXT NOT NULL
                                CHECK (state IN ('absent', 'fresh', 'stale', 'refresh_failed')),
    http_etag               TEXT,
    last_checked_at         TEXT,
    last_successful_check_at TEXT,
    stale_after             TEXT,
    last_error_code         TEXT CHECK (last_error_code IS NULL OR length(last_error_code) <= 128),
    last_idempotency_key     TEXT UNIQUE
                                CHECK (last_idempotency_key IS NULL
                                    OR length(trim(last_idempotency_key)) > 0),
    version                 INTEGER NOT NULL DEFAULT 1
                                CHECK (typeof(version) = 'integer' AND version >= 1),
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    CHECK (
        (state = 'absent' AND active_snapshot_id IS NULL)
        OR state = 'refresh_failed'
        OR (state IN ('fresh', 'stale') AND active_snapshot_id IS NOT NULL)
    ),
    CHECK (state != 'refresh_failed' OR last_error_code IS NOT NULL)
);

CREATE INDEX idx_pricing_catalog_state_snapshot
    ON pricing_catalog_state(active_snapshot_id);

-- Pricing mutations may be retried after a later mutation has advanced the
-- mutable pointer.  Keep one immutable receipt for every operation/key pair;
-- the pointer columns above remain a compact current-state projection, while
-- this table is the durable all-key idempotency authority.
CREATE TABLE pricing_operation_receipt (
    operation_scope TEXT NOT NULL
                            CHECK (length(trim(operation_scope)) > 0
                                AND length(operation_scope) <= 512),
    idempotency_key  TEXT NOT NULL
                            CHECK (length(trim(idempotency_key)) > 0
                                AND length(idempotency_key) <= 256),
    request_digest   TEXT NOT NULL
                            CHECK (length(trim(request_digest)) = 64),
    result_json      TEXT NOT NULL CHECK (json_valid(result_json)),
    created_at       TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    PRIMARY KEY (operation_scope, idempotency_key)
);

CREATE INDEX idx_pricing_operation_receipt_created
    ON pricing_operation_receipt(created_at DESC, operation_scope, idempotency_key);

CREATE TRIGGER pricing_operation_receipt_immutable_update
BEFORE UPDATE ON pricing_operation_receipt
BEGIN
    SELECT RAISE(ABORT, 'pricing operation receipts are immutable');
END;

CREATE TRIGGER pricing_operation_receipt_immutable_delete
BEFORE DELETE ON pricing_operation_receipt
BEGIN
    SELECT RAISE(ABORT, 'pricing operation receipts are immutable');
END;

CREATE TRIGGER pricing_catalog_state_identity_immutable_update
BEFORE UPDATE OF id, created_at ON pricing_catalog_state
BEGIN
    SELECT RAISE(ABORT, 'pricing catalog state identity is immutable');
END;

-- One immutable rate revision represents one normalized provider/model row in
-- a catalog snapshot or one exact user-entered override.  The nullable rate
-- buckets distinguish an absent price from an explicit zero price.
CREATE TABLE pricing_rate_revision (
    id                              TEXT NOT NULL PRIMARY KEY,
    source_kind                     TEXT NOT NULL
                                        CHECK (source_kind IN (
                                            'models_dev_catalog', 'manual_override'
                                        )),
    owner_user_id                   TEXT REFERENCES user(id) ON DELETE SET NULL,
    catalog_snapshot_id            TEXT REFERENCES pricing_catalog_snapshot(id) ON DELETE RESTRICT,
    catalog_provider_id            TEXT,
    catalog_model_id               TEXT,
    pricing_subject_revision_id    TEXT,
    pricing_subject_revision_digest TEXT,
    runtime_model                  TEXT,
    source_model_key               TEXT
                                        CHECK (source_model_key IS NULL
                                            OR length(trim(source_model_key)) > 0),
    source_last_updated             TEXT,
    currency                        TEXT NOT NULL DEFAULT 'USD'
                                        CHECK (currency = 'USD'),
    input_nano_usd_per_million      INTEGER
                                        CHECK (input_nano_usd_per_million IS NULL
                                            OR (typeof(input_nano_usd_per_million) = 'integer'
                                                AND input_nano_usd_per_million >= 0
                                                AND input_nano_usd_per_million <= 1000000000000000)),
    output_nano_usd_per_million     INTEGER
                                        CHECK (output_nano_usd_per_million IS NULL
                                            OR (typeof(output_nano_usd_per_million) = 'integer'
                                                AND output_nano_usd_per_million >= 0
                                                AND output_nano_usd_per_million <= 1000000000000000)),
    cache_read_nano_usd_per_million INTEGER
                                        CHECK (cache_read_nano_usd_per_million IS NULL
                                            OR (typeof(cache_read_nano_usd_per_million) = 'integer'
                                                AND cache_read_nano_usd_per_million >= 0
                                                AND cache_read_nano_usd_per_million <= 1000000000000000)),
    cache_write_nano_usd_per_million INTEGER
                                        CHECK (cache_write_nano_usd_per_million IS NULL
                                            OR (typeof(cache_write_nano_usd_per_million) = 'integer'
                                                AND cache_write_nano_usd_per_million >= 0
                                                AND cache_write_nano_usd_per_million <= 1000000000000000)),
    tiers_json                      TEXT NOT NULL DEFAULT '[]'
                                        CHECK (json_valid(tiers_json)),
    legacy_context_over_200k_json   TEXT
                                        CHECK (legacy_context_over_200k_json IS NULL
                                            OR json_valid(legacy_context_over_200k_json)),
    context_tier_state              TEXT NOT NULL DEFAULT 'none'
                                        CHECK (context_tier_state IN (
                                            'none', 'resolved', 'threshold_unknown'
                                        )),
    received_rates_json             TEXT NOT NULL DEFAULT '{}'
                                        CHECK (json_valid(received_rates_json)),
    rate_digest                     TEXT NOT NULL UNIQUE
                                        CHECK (length(trim(rate_digest)) > 0),
    effective_at                    TEXT NOT NULL CHECK (length(trim(effective_at)) > 0),
    created_at                      TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    CHECK (
        (source_kind = 'models_dev_catalog'
            AND catalog_snapshot_id IS NOT NULL
            AND catalog_provider_id IS NOT NULL
            AND length(trim(catalog_provider_id)) > 0
            AND catalog_model_id IS NOT NULL
            AND length(trim(catalog_model_id)) > 0
            AND owner_user_id IS NULL
            AND pricing_subject_revision_id IS NULL
            AND pricing_subject_revision_digest IS NULL
            AND runtime_model IS NULL)
        OR
        (source_kind = 'manual_override'
            AND catalog_snapshot_id IS NULL
            AND catalog_provider_id IS NULL
            AND catalog_model_id IS NULL
            AND pricing_subject_revision_id IS NOT NULL
            AND length(trim(pricing_subject_revision_id)) > 0
            AND pricing_subject_revision_digest IS NOT NULL
            AND length(trim(pricing_subject_revision_digest)) > 0
            AND runtime_model IS NOT NULL
            AND length(trim(runtime_model)) > 0)
    ),
    CHECK (catalog_provider_id IS NULL = (catalog_model_id IS NULL))
);

CREATE UNIQUE INDEX ux_pricing_rate_catalog_model
    ON pricing_rate_revision(catalog_snapshot_id, catalog_provider_id, catalog_model_id)
    WHERE catalog_snapshot_id IS NOT NULL;
CREATE INDEX idx_pricing_rate_catalog_lookup
    ON pricing_rate_revision(catalog_provider_id, catalog_model_id, effective_at DESC);
CREATE INDEX idx_pricing_rate_manual_subject_model
    ON pricing_rate_revision(pricing_subject_revision_id, runtime_model, effective_at DESC)
    WHERE source_kind = 'manual_override';

-- Account deletion's ON DELETE SET NULL action is the one system-owned
-- metadata transition permitted on an otherwise immutable rate row; it must
-- not be blocked by the immutability guard.
CREATE TRIGGER pricing_rate_revision_immutable_update
BEFORE UPDATE ON pricing_rate_revision
WHEN NOT (
    OLD.owner_user_id IS NOT NEW.owner_user_id
    AND NEW.owner_user_id IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'pricing rate revisions are immutable');
END;

CREATE TRIGGER pricing_rate_revision_owner_null_guard
BEFORE UPDATE OF owner_user_id ON pricing_rate_revision
WHEN OLD.owner_user_id IS NOT NEW.owner_user_id
    AND NEW.owner_user_id IS NULL
    AND (
        OLD.id IS NOT NEW.id
        OR OLD.source_kind IS NOT NEW.source_kind
        OR OLD.catalog_snapshot_id IS NOT NEW.catalog_snapshot_id
        OR OLD.catalog_provider_id IS NOT NEW.catalog_provider_id
        OR OLD.catalog_model_id IS NOT NEW.catalog_model_id
        OR OLD.pricing_subject_revision_id IS NOT NEW.pricing_subject_revision_id
        OR OLD.pricing_subject_revision_digest IS NOT NEW.pricing_subject_revision_digest
        OR OLD.runtime_model IS NOT NEW.runtime_model
        OR OLD.source_model_key IS NOT NEW.source_model_key
        OR OLD.source_last_updated IS NOT NEW.source_last_updated
        OR OLD.currency IS NOT NEW.currency
        OR OLD.input_nano_usd_per_million IS NOT NEW.input_nano_usd_per_million
        OR OLD.output_nano_usd_per_million IS NOT NEW.output_nano_usd_per_million
        OR OLD.cache_read_nano_usd_per_million IS NOT NEW.cache_read_nano_usd_per_million
        OR OLD.cache_write_nano_usd_per_million IS NOT NEW.cache_write_nano_usd_per_million
        OR OLD.tiers_json IS NOT NEW.tiers_json
        OR OLD.legacy_context_over_200k_json IS NOT NEW.legacy_context_over_200k_json
        OR OLD.context_tier_state IS NOT NEW.context_tier_state
        OR OLD.received_rates_json IS NOT NEW.received_rates_json
        OR OLD.rate_digest IS NOT NEW.rate_digest
        OR OLD.effective_at IS NOT NEW.effective_at
        OR OLD.created_at IS NOT NEW.created_at
    )
BEGIN
    SELECT RAISE(ABORT, 'pricing rate owner clearing cannot retarget a revision');
END;

CREATE TRIGGER pricing_rate_revision_immutable_delete
BEFORE DELETE ON pricing_rate_revision
BEGIN
    SELECT RAISE(ABORT, 'pricing rate revisions are immutable');
END;

-- A pricing subject is the logical provider entry or CLI runtime.  Its
-- current revision/version is mutable for optimistic updates, while each
-- subject revision is immutable and contains only non-secret billable
-- identity fields.  Source IDs are deliberately unenforced references.
CREATE TABLE pricing_subject (
    id                          TEXT NOT NULL PRIMARY KEY,
    owner_user_id               TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    subject_kind                TEXT NOT NULL
                                    CHECK (subject_kind IN ('provider_entry', 'cli_runtime')),
    provider_entry_id           TEXT,
    daemon_id                   TEXT,
    executor_type               TEXT,
    current_revision_id         TEXT,
    state                       TEXT NOT NULL DEFAULT 'active'
                                    CHECK (state IN ('active', 'retired')),
    last_idempotency_key        TEXT
                                    CHECK (last_idempotency_key IS NULL
                                        OR length(trim(last_idempotency_key)) > 0),
    last_update_digest          TEXT
                                    CHECK (last_update_digest IS NULL
                                        OR length(trim(last_update_digest)) > 0),
    version                     INTEGER NOT NULL DEFAULT 1
                                    CHECK (typeof(version) = 'integer' AND version >= 1),
    created_at                  TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at                  TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    CHECK (
        (subject_kind = 'provider_entry'
            AND provider_entry_id IS NOT NULL
            AND length(trim(provider_entry_id)) > 0
            AND daemon_id IS NULL
            AND executor_type IS NULL)
        OR
        (subject_kind = 'cli_runtime'
            AND provider_entry_id IS NULL
            AND daemon_id IS NOT NULL
            AND length(trim(daemon_id)) > 0
            AND executor_type IS NOT NULL
            AND length(trim(executor_type)) > 0)
    )
);

CREATE UNIQUE INDEX ux_pricing_subject_provider_entry
    ON pricing_subject(owner_user_id, provider_entry_id)
    WHERE subject_kind = 'provider_entry';
CREATE UNIQUE INDEX ux_pricing_subject_cli_runtime
    ON pricing_subject(owner_user_id, daemon_id, executor_type)
    WHERE subject_kind = 'cli_runtime';
CREATE INDEX idx_pricing_subject_owner_state
    ON pricing_subject(owner_user_id, state, updated_at DESC, id DESC);

-- SQLite cascades child rows while the parent DELETE is still in progress.
-- Keep a narrow account-teardown marker so the retirement-only subject guards
-- reject direct deletes without blocking the user-owned ON DELETE CASCADE.
CREATE TABLE pricing_account_deletion_guard (
    owner_user_id TEXT NOT NULL PRIMARY KEY CHECK (length(trim(owner_user_id)) > 0)
);

CREATE TRIGGER pricing_account_deletion_guard_begin
BEFORE DELETE ON user
BEGIN
    INSERT OR IGNORE INTO pricing_account_deletion_guard (owner_user_id)
    VALUES (OLD.id);
END;

CREATE TRIGGER pricing_account_deletion_guard_end
AFTER DELETE ON user
BEGIN
    DELETE FROM pricing_account_deletion_guard
    WHERE owner_user_id = OLD.id;
END;

CREATE TRIGGER pricing_subject_immutable_delete
BEFORE DELETE ON pricing_subject
WHEN EXISTS (
    SELECT 1 FROM user
    WHERE user.id = OLD.owner_user_id
)
AND NOT EXISTS (
    SELECT 1 FROM pricing_account_deletion_guard
    WHERE owner_user_id = OLD.owner_user_id
)
BEGIN
    SELECT RAISE(ABORT, 'pricing subjects require retirement or account teardown');
END;

CREATE TABLE pricing_subject_revision (
    id                          TEXT NOT NULL PRIMARY KEY,
    subject_id                  TEXT NOT NULL,
    owner_user_id               TEXT REFERENCES user(id) ON DELETE SET NULL,
    revision                    INTEGER NOT NULL
                                    CHECK (typeof(revision) = 'integer' AND revision >= 1),
    revision_digest             TEXT NOT NULL
                                    CHECK (length(trim(revision_digest)) > 0),
    subject_kind                TEXT NOT NULL
                                    CHECK (subject_kind IN ('provider_entry', 'cli_runtime')),
    provider_entry_id           TEXT,
    daemon_id                   TEXT,
    executor_type               TEXT,
    provider_kind               TEXT NOT NULL,
    credential_method           TEXT NOT NULL,
    endpoint_class              TEXT NOT NULL,
    runtime_fingerprint         TEXT,
    schema_revision             TEXT NOT NULL
                                    CHECK (length(trim(schema_revision)) > 0),
    non_secret_identity_json    TEXT NOT NULL DEFAULT '{}'
                                    CHECK (json_valid(non_secret_identity_json)),
    created_at                  TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    UNIQUE (subject_id, revision),
    UNIQUE (subject_id, revision_digest),
    CHECK (
        (subject_kind = 'provider_entry'
            AND provider_entry_id IS NOT NULL
            AND daemon_id IS NULL
            AND executor_type IS NULL)
        OR
        (subject_kind = 'cli_runtime'
            AND provider_entry_id IS NULL
            AND daemon_id IS NOT NULL
            AND executor_type IS NOT NULL)
    ),
    CHECK (length(trim(provider_kind)) > 0),
    CHECK (length(trim(credential_method)) > 0),
    CHECK (length(trim(endpoint_class)) > 0)
);

CREATE INDEX idx_pricing_subject_revision_subject
    ON pricing_subject_revision(subject_id, revision DESC, id DESC);
CREATE INDEX idx_pricing_subject_revision_owner
    ON pricing_subject_revision(owner_user_id, created_at DESC, id DESC);

CREATE TRIGGER pricing_subject_revision_scope_guard_insert
BEFORE INSERT ON pricing_subject_revision
BEGIN
    SELECT CASE
        WHEN NEW.owner_user_id IS NULL
            OR NOT EXISTS (
                SELECT 1 FROM pricing_subject s
                WHERE s.id = NEW.subject_id
                  AND s.owner_user_id = NEW.owner_user_id
                  AND s.subject_kind = NEW.subject_kind
                  AND s.provider_entry_id IS NEW.provider_entry_id
                  AND s.daemon_id IS NEW.daemon_id
                  AND s.executor_type IS NEW.executor_type
            )
        THEN RAISE(ABORT, 'pricing subject revision must match its subject identity')
    END;
END;

CREATE TRIGGER pricing_subject_current_revision_guard_insert
BEFORE INSERT ON pricing_subject
WHEN NEW.current_revision_id IS NOT NULL
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject_revision r
            WHERE r.id = NEW.current_revision_id
              AND r.subject_id = NEW.id
              AND r.subject_kind = NEW.subject_kind
              AND (r.owner_user_id IS NULL OR r.owner_user_id = NEW.owner_user_id)
        ) THEN RAISE(ABORT, 'pricing subject current revision must belong to subject')
    END;
END;

CREATE TRIGGER pricing_subject_current_revision_guard_update
BEFORE UPDATE OF current_revision_id ON pricing_subject
WHEN NEW.current_revision_id IS NOT NULL
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject_revision r
            WHERE r.id = NEW.current_revision_id
              AND r.subject_id = NEW.id
              AND r.subject_kind = NEW.subject_kind
              AND (r.owner_user_id IS NULL OR r.owner_user_id = NEW.owner_user_id)
        ) THEN RAISE(ABORT, 'pricing subject current revision must belong to subject')
    END;
END;

CREATE TRIGGER pricing_subject_identity_immutable_update
BEFORE UPDATE OF id, owner_user_id, subject_kind, provider_entry_id,
    daemon_id, executor_type, created_at
ON pricing_subject
WHEN OLD.id IS NOT NEW.id
    OR OLD.owner_user_id IS NOT NEW.owner_user_id
    OR OLD.subject_kind IS NOT NEW.subject_kind
    OR OLD.provider_entry_id IS NOT NEW.provider_entry_id
    OR OLD.daemon_id IS NOT NEW.daemon_id
    OR OLD.executor_type IS NOT NEW.executor_type
    OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'pricing subjects cannot be retargeted');
END;

CREATE TRIGGER pricing_subject_retirement_guard_update
BEFORE UPDATE OF state ON pricing_subject
WHEN OLD.state = 'retired' AND NEW.state != 'retired'
BEGIN
    SELECT RAISE(ABORT, 'retired pricing subjects cannot be reactivated');
END;

-- As with rate revisions, account teardown may clear only the owner metadata;
-- all billable identity fields remain immutable.
CREATE TRIGGER pricing_subject_revision_immutable_update
BEFORE UPDATE ON pricing_subject_revision
WHEN NOT (
    OLD.owner_user_id IS NOT NEW.owner_user_id
    AND NEW.owner_user_id IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'pricing subject revisions are immutable');
END;

CREATE TRIGGER pricing_subject_revision_owner_null_guard
BEFORE UPDATE OF owner_user_id ON pricing_subject_revision
WHEN OLD.owner_user_id IS NOT NEW.owner_user_id
    AND NEW.owner_user_id IS NULL
    AND (
        OLD.id IS NOT NEW.id
        OR OLD.subject_id IS NOT NEW.subject_id
        OR OLD.revision IS NOT NEW.revision
        OR OLD.revision_digest IS NOT NEW.revision_digest
        OR OLD.subject_kind IS NOT NEW.subject_kind
        OR OLD.provider_entry_id IS NOT NEW.provider_entry_id
        OR OLD.daemon_id IS NOT NEW.daemon_id
        OR OLD.executor_type IS NOT NEW.executor_type
        OR OLD.provider_kind IS NOT NEW.provider_kind
        OR OLD.credential_method IS NOT NEW.credential_method
        OR OLD.endpoint_class IS NOT NEW.endpoint_class
        OR OLD.runtime_fingerprint IS NOT NEW.runtime_fingerprint
        OR OLD.schema_revision IS NOT NEW.schema_revision
        OR OLD.non_secret_identity_json IS NOT NEW.non_secret_identity_json
        OR OLD.created_at IS NOT NEW.created_at
    )
BEGIN
    SELECT RAISE(ABORT, 'pricing subject owner clearing cannot retarget a revision');
END;

CREATE TRIGGER pricing_subject_revision_immutable_delete
BEFORE DELETE ON pricing_subject_revision
BEGIN
    SELECT RAISE(ABORT, 'pricing subject revisions are immutable');
END;

-- Manual overrides are immutable but must be tied to one exact subject
-- revision/runtime model.  The subject revision reference is intentionally
-- plain so disconnect/deletion cannot erase historical rate provenance; this
-- admission-time guard validates its digest and account ownership instead.
CREATE TRIGGER pricing_manual_rate_revision_guard_insert
BEFORE INSERT ON pricing_rate_revision
WHEN NEW.source_kind = 'manual_override'
BEGIN
    SELECT CASE
        WHEN NEW.owner_user_id IS NULL
            OR NOT EXISTS (
                SELECT 1 FROM pricing_subject_revision r
                WHERE r.id = NEW.pricing_subject_revision_id
                  AND r.revision_digest = NEW.pricing_subject_revision_digest
                  AND r.owner_user_id = NEW.owner_user_id
            )
        THEN RAISE(ABORT, 'manual rate revision must belong to its pricing subject')
    END;
END;

-- The mutable binding is versioned with CAS by its owner.  Historical
-- selections copy the immutable subject/rate references, so retiring or
-- replacing a binding cannot change an admitted invocation.
CREATE TABLE pricing_subject_binding (
    id                          TEXT NOT NULL PRIMARY KEY,
    owner_user_id               TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    subject_id                  TEXT NOT NULL,
    subject_revision_id         TEXT NOT NULL,
    subject_revision_digest     TEXT NOT NULL
                                    CHECK (length(trim(subject_revision_digest)) > 0),
    runtime_model               TEXT NOT NULL CHECK (length(trim(runtime_model)) > 0),
    source_kind                 TEXT NOT NULL
                                    CHECK (source_kind IN (
                                        'models_dev_catalog', 'manual_override'
                                    )),
    catalog_provider_id         TEXT,
    catalog_model_id            TEXT,
    rate_revision_id            TEXT NOT NULL
                                    REFERENCES pricing_rate_revision(id) ON DELETE RESTRICT,
    binding_digest              TEXT NOT NULL
                                    CHECK (length(trim(binding_digest)) > 0),
    state                       TEXT NOT NULL DEFAULT 'active'
                                    CHECK (state IN ('active', 'retired')),
    version                     INTEGER NOT NULL DEFAULT 1
                                    CHECK (typeof(version) = 'integer' AND version >= 1),
    effective_at                TEXT NOT NULL CHECK (length(trim(effective_at)) > 0),
    retired_at                  TEXT,
    created_at                  TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at                  TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    CHECK (catalog_provider_id IS NULL = (catalog_model_id IS NULL)),
    CHECK (
        (state = 'active' AND retired_at IS NULL)
        OR (state = 'retired' AND retired_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX ux_pricing_subject_binding_active
    ON pricing_subject_binding(subject_id, subject_revision_id, source_kind, runtime_model)
    WHERE state = 'active';
-- Manual and catalog sources intentionally coexist for one runtime so
-- resolution can prefer the manual row while retaining the catalog fallback.
-- A retired binding is terminal, but the same exact subject revision/model
-- may be configured again later as a new binding row.  Subject revision and
-- source kind keep old revisions/history independent from the current one.
CREATE INDEX idx_pricing_subject_binding_revision
    ON pricing_subject_binding(subject_id, subject_revision_id, runtime_model);
CREATE INDEX idx_pricing_subject_binding_owner
    ON pricing_subject_binding(owner_user_id, subject_id, state, runtime_model);

CREATE TRIGGER pricing_subject_binding_guard_insert
BEFORE INSERT ON pricing_subject_binding
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject s
            WHERE s.id = NEW.subject_id
              AND s.owner_user_id = NEW.owner_user_id
        ) THEN RAISE(ABORT, 'pricing binding subject must belong to owner')
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject_revision r
            WHERE r.id = NEW.subject_revision_id
              AND r.subject_id = NEW.subject_id
              AND r.revision_digest = NEW.subject_revision_digest
              AND (r.owner_user_id IS NULL OR r.owner_user_id = NEW.owner_user_id)
        ) THEN RAISE(ABORT, 'pricing binding subject revision is invalid')
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_rate_revision r
            WHERE r.id = NEW.rate_revision_id
              AND r.source_kind = NEW.source_kind
              AND (r.owner_user_id IS NULL OR r.owner_user_id = NEW.owner_user_id)
              AND (
                  (NEW.source_kind = 'models_dev_catalog'
                      AND r.catalog_provider_id = NEW.catalog_provider_id
                      AND r.catalog_model_id = NEW.catalog_model_id)
                  OR
                  (NEW.source_kind = 'manual_override'
                      AND r.pricing_subject_revision_id = NEW.subject_revision_id
                      AND r.pricing_subject_revision_digest = NEW.subject_revision_digest
                      AND r.runtime_model = NEW.runtime_model)
              )
        ) THEN RAISE(ABORT, 'pricing binding rate revision is invalid')
    END;
END;

CREATE TRIGGER pricing_subject_binding_guard_update
BEFORE UPDATE OF owner_user_id, subject_id, subject_revision_id,
    subject_revision_digest, runtime_model, source_kind, catalog_provider_id,
    catalog_model_id, rate_revision_id
ON pricing_subject_binding
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject s
            WHERE s.id = NEW.subject_id
              AND s.owner_user_id = NEW.owner_user_id
        ) THEN RAISE(ABORT, 'pricing binding subject must belong to owner')
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject_revision r
            WHERE r.id = NEW.subject_revision_id
              AND r.subject_id = NEW.subject_id
              AND r.revision_digest = NEW.subject_revision_digest
              AND (r.owner_user_id IS NULL OR r.owner_user_id = NEW.owner_user_id)
        ) THEN RAISE(ABORT, 'pricing binding subject revision is invalid')
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_rate_revision r
            WHERE r.id = NEW.rate_revision_id
              AND r.source_kind = NEW.source_kind
              AND (r.owner_user_id IS NULL OR r.owner_user_id = NEW.owner_user_id)
              AND (
                  (NEW.source_kind = 'models_dev_catalog'
                      AND r.catalog_provider_id = NEW.catalog_provider_id
                      AND r.catalog_model_id = NEW.catalog_model_id)
                  OR
                  (NEW.source_kind = 'manual_override'
                      AND r.pricing_subject_revision_id = NEW.subject_revision_id
                      AND r.pricing_subject_revision_digest = NEW.subject_revision_digest
                      AND r.runtime_model = NEW.runtime_model)
              )
        ) THEN RAISE(ABORT, 'pricing binding rate revision is invalid')
    END;
END;

CREATE TRIGGER pricing_subject_binding_identity_immutable_update
BEFORE UPDATE OF id, owner_user_id, subject_id, subject_revision_id,
    subject_revision_digest, runtime_model, created_at
ON pricing_subject_binding
WHEN OLD.id IS NOT NEW.id
    OR OLD.owner_user_id IS NOT NEW.owner_user_id
    OR OLD.subject_id IS NOT NEW.subject_id
    OR OLD.subject_revision_id IS NOT NEW.subject_revision_id
    OR OLD.subject_revision_digest IS NOT NEW.subject_revision_digest
    OR OLD.runtime_model IS NOT NEW.runtime_model
    OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'pricing bindings cannot retarget a subject/model');
END;

CREATE TRIGGER pricing_subject_binding_retirement_guard_update
BEFORE UPDATE OF state ON pricing_subject_binding
WHEN OLD.state = 'retired' AND NEW.state != 'retired'
BEGIN
    SELECT RAISE(ABORT, 'retired pricing bindings cannot be reactivated');
END;

CREATE TRIGGER pricing_subject_binding_retired_rate_guard_update
BEFORE UPDATE OF source_kind, catalog_provider_id, catalog_model_id,
    rate_revision_id, binding_digest, effective_at
ON pricing_subject_binding
WHEN OLD.state = 'retired'
BEGIN
    SELECT RAISE(ABORT, 'retired pricing bindings cannot change rates');
END;

CREATE TRIGGER pricing_subject_binding_immutable_delete
BEFORE DELETE ON pricing_subject_binding
WHEN EXISTS (
    SELECT 1 FROM pricing_subject
    WHERE pricing_subject.id = OLD.subject_id
)
AND NOT EXISTS (
    SELECT 1 FROM pricing_account_deletion_guard
    WHERE owner_user_id = OLD.owner_user_id
)
BEGIN
    SELECT RAISE(ABORT, 'pricing bindings require retirement or subject teardown');
END;

-- A selection is the immutable admission-time decision for one candidate in
-- a frozen route.  Every admitted candidate gets one row, including a
-- candidate skipped before external work; only a candidate that reaches a
-- provider later receives an invocation link.  It may be explicitly
-- unpriced; that is different from a missing selection and is retained for
-- truthful coverage reporting.
CREATE TABLE pricing_selection (
    id                          TEXT NOT NULL PRIMARY KEY,
    owner_user_id               TEXT REFERENCES user(id) ON DELETE CASCADE,
    project_id                  TEXT REFERENCES project(id) ON DELETE CASCADE,
    domain_kind                 TEXT NOT NULL
                                    CHECK (domain_kind IN ('execution', 'chat', 'inquiry')),
    surface                     TEXT NOT NULL
                                    CHECK (surface IN (
                                        'task_execution', 'project_chat', 'main_chat',
                                        'genesis_chat', 'main_inquiry'
                                    )),
    source_id                   TEXT NOT NULL,
    execution_id                TEXT,
    task_id                     TEXT,
    candidate_key               TEXT
                                    CHECK (candidate_key IS NULL OR length(trim(candidate_key)) > 0),
    attempt_ordinal             INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(attempt_ordinal) = 'integer'
                                        AND attempt_ordinal >= 0),
    invocation_id               TEXT UNIQUE
                                    REFERENCES usage_invocation(id) ON DELETE SET NULL,
    subject_id                  TEXT,
    subject_revision_id         TEXT,
    subject_revision_digest     TEXT,
    binding_id                  TEXT,
    rate_revision_id            TEXT REFERENCES pricing_rate_revision(id) ON DELETE RESTRICT,
    catalog_snapshot_id         TEXT REFERENCES pricing_catalog_snapshot(id) ON DELETE RESTRICT,
    catalog_freshness            TEXT CHECK (catalog_freshness IS NULL OR catalog_freshness IN (
                                        'fresh', 'stale', 'refresh_failed', 'not_applicable'
                                    )),
    runtime_model               TEXT,
    admitted_provider_id        TEXT,
    admitted_model_id           TEXT,
    source_kind                 TEXT
                                    CHECK (source_kind IS NULL OR source_kind IN (
                                        'models_dev_catalog', 'manual_override'
                                    )),
    provenance_kind             TEXT NOT NULL DEFAULT 'runtime'
                                    CHECK (provenance_kind IN (
                                        'runtime', 'legacy_execution_aggregate',
                                        'legacy_chat', 'legacy_inquiry'
                                    )),
    selection_status             TEXT NOT NULL
                                    CHECK (selection_status IN ('priced', 'unpriced', 'invalid')),
    selection_reason             TEXT CHECK (selection_reason IS NULL OR selection_reason IN (
                                        'pending', 'unsettled', 'unmetered',
                                        'missing_provider', 'missing_model', 'missing_binding',
                                        'missing_rate', 'unresolved_tier', 'identity_mismatch',
                                        'invalid_legacy_usage'
                                    )),
    selection_digest             TEXT NOT NULL
                                    CHECK (length(trim(selection_digest)) > 0),
    selected_at                  TEXT NOT NULL CHECK (
                                    length(trim(selected_at)) > 0
                                    OR provenance_kind IN (
                                        'legacy_execution_aggregate', 'legacy_chat',
                                        'legacy_inquiry'
                                    )
                                ),
    created_at                  TEXT NOT NULL CHECK (
                                    length(trim(created_at)) > 0
                                    OR provenance_kind IN (
                                        'legacy_execution_aggregate', 'legacy_chat',
                                        'legacy_inquiry'
                                    )
                                ),
    CHECK (
        (domain_kind = 'execution' AND surface = 'task_execution')
        OR (domain_kind = 'chat' AND surface IN ('project_chat', 'main_chat', 'genesis_chat'))
        OR (domain_kind = 'inquiry' AND surface = 'main_inquiry')
    ),
    CHECK (
        (surface IN ('task_execution', 'project_chat') AND project_id IS NOT NULL)
        OR (surface IN ('main_chat', 'main_inquiry', 'genesis_chat') AND project_id IS NULL)
        OR (surface = 'genesis_chat' AND project_id IS NOT NULL)
    ),
    CHECK (subject_revision_id IS NULL = (subject_revision_digest IS NULL)),
    CHECK (runtime_model IS NULL OR length(trim(runtime_model)) > 0),
    CHECK (
        (selection_status = 'priced'
            AND rate_revision_id IS NOT NULL
            AND source_kind IS NOT NULL
            AND subject_id IS NOT NULL
            AND subject_revision_id IS NOT NULL
            AND binding_id IS NOT NULL)
        OR
        (selection_status IN ('unpriced', 'invalid')
            AND rate_revision_id IS NULL)
    ),
    CHECK (selection_status != 'priced' OR length(trim(runtime_model)) > 0),
    CHECK (source_kind != 'manual_override' OR catalog_snapshot_id IS NULL),
    CHECK (source_kind != 'models_dev_catalog' OR catalog_snapshot_id IS NOT NULL),
    CHECK (
        (COALESCE(source_kind, '') = 'models_dev_catalog'
            AND catalog_snapshot_id IS NOT NULL
            AND catalog_freshness IS NOT NULL
            AND catalog_freshness IN ('fresh', 'stale', 'refresh_failed'))
        OR
        (COALESCE(source_kind, '') = 'manual_override'
            AND catalog_snapshot_id IS NULL
            AND catalog_freshness IS NOT NULL
            AND catalog_freshness = 'not_applicable')
        OR
        (source_kind IS NULL
            AND catalog_snapshot_id IS NULL
            AND catalog_freshness IS NULL)
    ),
    CHECK (
        length(trim(source_id)) > 0
        OR provenance_kind IN (
            'legacy_execution_aggregate', 'legacy_chat', 'legacy_inquiry'
        )
    ),
    CHECK (
        (provenance_kind = 'legacy_execution_aggregate'
            AND domain_kind = 'execution' AND surface = 'task_execution'
            AND project_id IS NOT NULL)
        OR (provenance_kind = 'legacy_chat'
            AND domain_kind = 'chat'
            AND surface IN ('project_chat', 'main_chat', 'genesis_chat'))
        OR (provenance_kind = 'legacy_inquiry'
            AND domain_kind = 'inquiry' AND surface = 'main_inquiry')
        OR provenance_kind = 'runtime'
    )
);

-- Legacy Project rows may legitimately have no account owner because older
-- Projects predate account ownership. Keep that exception narrow: runtime
-- and account-scoped selections still require a real user, and a non-null
-- legacy owner must be the current Project owner rather than an invented
-- account identity.
CREATE TRIGGER pricing_selection_owner_scope_guard_insert
BEFORE INSERT ON pricing_selection
BEGIN
    SELECT CASE
        WHEN NEW.owner_user_id IS NULL
             AND NOT (
                 (
                     NEW.provenance_kind IN (
                         'legacy_execution_aggregate', 'legacy_chat'
                     )
                     AND NEW.project_id IS NOT NULL
                     AND EXISTS (
                         SELECT 1 FROM project p
                         WHERE p.id = NEW.project_id
                           AND (p.owner_id IS NULL OR NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = p.owner_id
                           ))
                     )
                 )
                 OR (
                     NEW.provenance_kind = 'legacy_chat'
                     AND NEW.project_id IS NULL
                     AND EXISTS (
                         SELECT 1
                         FROM agent_chat_message m
                         JOIN agent_chat c ON c.id = m.chat_id
                         WHERE m.id = NEW.source_id
                           AND c.kind = 'account_main'
                           AND (c.account_id IS NULL OR NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = c.account_id
                           ))
                     )
                 )
                 OR (
                     NEW.provenance_kind = 'legacy_inquiry'
                     AND NEW.project_id IS NULL
                     AND EXISTS (
                         SELECT 1 FROM agent_inquiry i
                         WHERE i.id = NEW.source_id
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = i.owner_user_id
                           )
                     )
                 )
             )
            THEN RAISE(ABORT, 'selection owner is required outside legacy ownerless Projects')
        WHEN NEW.provenance_kind IN ('legacy_execution_aggregate', 'legacy_chat')
             AND NEW.project_id IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1 FROM project p
                 WHERE p.id = NEW.project_id
                   AND (
                       p.owner_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = p.owner_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy selection owner must match Project owner')
        WHEN NEW.provenance_kind = 'legacy_chat'
             AND NEW.project_id IS NULL
             AND NOT EXISTS (
                 SELECT 1
                 FROM agent_chat_message m
                 JOIN agent_chat c ON c.id = m.chat_id
                 WHERE m.id = NEW.source_id
                   AND c.kind = 'account_main'
                   AND (
                       c.account_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = c.account_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy account chat selection owner is invalid')
        WHEN NEW.provenance_kind = 'legacy_inquiry'
             AND NOT EXISTS (
                 SELECT 1 FROM agent_inquiry i
                 WHERE i.id = NEW.source_id
                   AND (
                       i.owner_user_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = i.owner_user_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy inquiry selection owner is invalid')
    END;
END;

CREATE UNIQUE INDEX ux_pricing_selection_candidate
    ON pricing_selection(
        COALESCE(owner_user_id, ''),
        COALESCE(project_id, ''),
        domain_kind,
        surface,
        source_id,
        COALESCE(candidate_key, ''),
        attempt_ordinal
    );
CREATE INDEX idx_pricing_selection_owner_time
    ON pricing_selection(owner_user_id, selected_at DESC, id DESC);
CREATE INDEX idx_pricing_selection_rate
    ON pricing_selection(rate_revision_id);
CREATE INDEX idx_pricing_selection_digest
    ON pricing_selection(selection_digest);

CREATE TRIGGER pricing_selection_subject_guard_insert
BEFORE INSERT ON pricing_selection
WHEN NEW.subject_revision_id IS NOT NULL
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject_revision r
            WHERE r.id = NEW.subject_revision_id
              AND r.subject_id = NEW.subject_id
              AND r.revision_digest = NEW.subject_revision_digest
              AND (r.owner_user_id IS NULL OR r.owner_user_id = NEW.owner_user_id)
        ) THEN RAISE(ABORT, 'pricing selection subject revision is invalid')
    END;
END;

CREATE TRIGGER pricing_selection_rate_guard_insert
BEFORE INSERT ON pricing_selection
WHEN NEW.rate_revision_id IS NOT NULL
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_rate_revision r
            WHERE r.id = NEW.rate_revision_id
              AND r.source_kind = NEW.source_kind
              AND (
                  (NEW.source_kind = 'models_dev_catalog'
                      AND r.owner_user_id IS NULL
                      AND r.catalog_snapshot_id = NEW.catalog_snapshot_id)
                  OR
                  (NEW.source_kind = 'manual_override'
                      AND r.owner_user_id = NEW.owner_user_id
                      AND r.pricing_subject_revision_id = NEW.subject_revision_id
                      AND r.pricing_subject_revision_digest = NEW.subject_revision_digest
                      AND r.runtime_model = NEW.runtime_model)
              )
        ) THEN RAISE(ABORT, 'pricing selection rate revision is invalid')
    END;
END;

CREATE TRIGGER pricing_selection_binding_guard_insert
BEFORE INSERT ON pricing_selection
WHEN NEW.selection_status = 'priced'
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_subject_binding b
            WHERE b.id = NEW.binding_id
              AND b.owner_user_id = NEW.owner_user_id
              AND b.subject_id = NEW.subject_id
              AND b.subject_revision_id = NEW.subject_revision_id
              AND b.subject_revision_digest = NEW.subject_revision_digest
              AND b.runtime_model = NEW.runtime_model
              AND b.source_kind = NEW.source_kind
              AND b.rate_revision_id = NEW.rate_revision_id
              AND b.state = 'active'
        ) THEN RAISE(ABORT, 'priced selection must match an active exact binding')
    END;
END;

-- An invocation exists for every actual provider call, including calls that
-- settle as unmetered.  The source IDs and agent/profile labels are frozen
-- non-secret provenance; they are not foreign keys to mutable/deletable
-- domain records.
CREATE TABLE usage_invocation (
    id                          TEXT NOT NULL PRIMARY KEY,
    owner_user_id               TEXT REFERENCES user(id) ON DELETE CASCADE,
    project_id                  TEXT REFERENCES project(id) ON DELETE CASCADE,
    domain_kind                 TEXT NOT NULL
                                    CHECK (domain_kind IN ('execution', 'chat', 'inquiry')),
    surface                     TEXT NOT NULL
                                    CHECK (surface IN (
                                        'task_execution', 'project_chat', 'main_chat',
                                        'genesis_chat', 'main_inquiry'
                                    )),
    source_id                   TEXT NOT NULL,
    execution_id                TEXT,
    task_id                     TEXT,
    domain_idempotency_key      TEXT NOT NULL UNIQUE
                                        CHECK (length(trim(domain_idempotency_key)) > 0),
    candidate_key               TEXT
                                    CHECK (candidate_key IS NULL
                                        OR length(trim(candidate_key)) > 0),
    attempt_ordinal             INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(attempt_ordinal) = 'integer'
                                        AND attempt_ordinal >= 0),
    pricing_selection_id        TEXT NOT NULL UNIQUE
                                    REFERENCES pricing_selection(id) ON DELETE RESTRICT,
    admitted_provider_id        TEXT,
    admitted_model_id           TEXT,
    admitted_runtime_model      TEXT
                                    CHECK (admitted_runtime_model IS NULL
                                        OR length(trim(admitted_runtime_model)) > 0),
    pricing_subject_id          TEXT,
    pricing_subject_revision_id TEXT,
    subject_revision_digest     TEXT
                                    CHECK (pricing_subject_revision_id IS NULL
                                        = (subject_revision_digest IS NULL)),
    agent_id                    TEXT,
    profile_id                  TEXT,
    agent_name_snapshot         TEXT,
    project_name_snapshot       TEXT,
    executor_type               TEXT,
    backend_kind                TEXT,
    provenance_kind             TEXT NOT NULL DEFAULT 'runtime'
                                    CHECK (provenance_kind IN (
                                        'runtime', 'legacy_execution_aggregate',
                                        'legacy_chat', 'legacy_inquiry'
                                    )),
    lifecycle                   TEXT NOT NULL DEFAULT 'admitted'
                                    CHECK (lifecycle IN (
                                        'admitted', 'started', 'pending_settlement',
                                        'settled', 'unsettled'
                                    )),
    telemetry_state             TEXT NOT NULL DEFAULT 'pending'
                                    CHECK (telemetry_state IN (
                                        'pending', 'metered', 'unmetered', 'unsettled'
                                    )),
    terminal_reason             TEXT
                                    CHECK (terminal_reason IS NULL
                                        OR length(trim(terminal_reason)) BETWEEN 1 AND 512),
    version                     INTEGER NOT NULL DEFAULT 1
                                    CHECK (typeof(version) = 'integer' AND version >= 1),
    admitted_at                 TEXT NOT NULL CHECK (
                                    length(trim(admitted_at)) > 0
                                    OR provenance_kind IN (
                                        'legacy_execution_aggregate', 'legacy_chat',
                                        'legacy_inquiry'
                                    )
                                ),
    started_at                  TEXT
                                    CHECK (started_at IS NULL OR length(trim(started_at)) > 0
                                        OR provenance_kind IN (
                                            'legacy_execution_aggregate', 'legacy_chat',
                                            'legacy_inquiry'
                                        )),
    settled_at                  TEXT
                                    CHECK (settled_at IS NULL OR length(trim(settled_at)) > 0
                                        OR provenance_kind IN (
                                            'legacy_execution_aggregate', 'legacy_chat',
                                            'legacy_inquiry'
                                        )),
    created_at                  TEXT NOT NULL CHECK (
                                    length(trim(created_at)) > 0
                                    OR provenance_kind IN (
                                        'legacy_execution_aggregate', 'legacy_chat',
                                        'legacy_inquiry'
                                    )
                                ),
    updated_at                  TEXT NOT NULL CHECK (
                                    length(trim(updated_at)) > 0
                                    OR provenance_kind IN (
                                        'legacy_execution_aggregate', 'legacy_chat',
                                        'legacy_inquiry'
                                    )
                                ),
    CHECK (
        (domain_kind = 'execution' AND surface = 'task_execution')
        OR (domain_kind = 'chat' AND surface IN ('project_chat', 'main_chat', 'genesis_chat'))
        OR (domain_kind = 'inquiry' AND surface = 'main_inquiry')
    ),
    CHECK (
        (surface IN ('task_execution', 'project_chat') AND project_id IS NOT NULL)
        OR (surface IN ('main_chat', 'main_inquiry', 'genesis_chat') AND project_id IS NULL)
        OR (surface = 'genesis_chat' AND project_id IS NOT NULL)
    ),
    CHECK (
        length(trim(source_id)) > 0
        OR provenance_kind IN (
            'legacy_execution_aggregate', 'legacy_chat', 'legacy_inquiry'
        )
    ),
    CHECK (
        (provenance_kind = 'legacy_execution_aggregate'
            AND domain_kind = 'execution' AND surface = 'task_execution'
            AND project_id IS NOT NULL)
        OR (provenance_kind = 'legacy_chat'
            AND domain_kind = 'chat'
            AND surface IN ('project_chat', 'main_chat', 'genesis_chat'))
        OR (provenance_kind = 'legacy_inquiry'
            AND domain_kind = 'inquiry' AND surface = 'main_inquiry')
        OR provenance_kind = 'runtime'
    ),
    CHECK (
        (lifecycle IN ('admitted', 'started', 'pending_settlement')
            AND telemetry_state = 'pending')
        OR (lifecycle = 'settled' AND telemetry_state IN ('metered', 'unmetered'))
        OR (lifecycle = 'unsettled' AND telemetry_state = 'unsettled')
    ),
    CHECK (
        (lifecycle = 'admitted' AND started_at IS NULL AND settled_at IS NULL)
        OR (lifecycle IN ('started', 'pending_settlement')
            AND started_at IS NOT NULL AND settled_at IS NULL)
        OR (lifecycle = 'settled' AND started_at IS NOT NULL AND settled_at IS NOT NULL)
        OR (lifecycle = 'unsettled'
            AND started_at IS NOT NULL AND settled_at IS NOT NULL
            AND terminal_reason IS NOT NULL)
    )
);

CREATE TRIGGER usage_invocation_owner_scope_guard_insert
BEFORE INSERT ON usage_invocation
BEGIN
    SELECT CASE
        WHEN NEW.owner_user_id IS NULL
             AND NOT (
                 (
                     NEW.provenance_kind IN (
                         'legacy_execution_aggregate', 'legacy_chat'
                     )
                     AND NEW.project_id IS NOT NULL
                     AND EXISTS (
                         SELECT 1 FROM project p
                         WHERE p.id = NEW.project_id
                           AND (p.owner_id IS NULL OR NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = p.owner_id
                           ))
                     )
                 )
                 OR (
                     NEW.provenance_kind = 'legacy_chat'
                     AND NEW.project_id IS NULL
                     AND EXISTS (
                         SELECT 1
                         FROM agent_chat_message m
                         JOIN agent_chat c ON c.id = m.chat_id
                         WHERE m.id = NEW.source_id
                           AND c.kind = 'account_main'
                           AND (c.account_id IS NULL OR NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = c.account_id
                           ))
                     )
                 )
                 OR (
                     NEW.provenance_kind = 'legacy_inquiry'
                     AND NEW.project_id IS NULL
                     AND EXISTS (
                         SELECT 1 FROM agent_inquiry i
                         WHERE i.id = NEW.source_id
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = i.owner_user_id
                           )
                     )
                 )
             )
            THEN RAISE(ABORT, 'invocation owner is required outside legacy ownerless Projects')
        WHEN NEW.provenance_kind IN ('legacy_execution_aggregate', 'legacy_chat')
             AND NEW.project_id IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1 FROM project p
                 WHERE p.id = NEW.project_id
                   AND (
                       p.owner_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = p.owner_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy invocation owner must match Project owner')
        WHEN NEW.provenance_kind = 'legacy_chat'
             AND NEW.project_id IS NULL
             AND NOT EXISTS (
                 SELECT 1
                 FROM agent_chat_message m
                 JOIN agent_chat c ON c.id = m.chat_id
                 WHERE m.id = NEW.source_id
                   AND c.kind = 'account_main'
                   AND (
                       c.account_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = c.account_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy account chat invocation owner is invalid')
        WHEN NEW.provenance_kind = 'legacy_inquiry'
             AND NOT EXISTS (
                 SELECT 1 FROM agent_inquiry i
                 WHERE i.id = NEW.source_id
                   AND (
                       i.owner_user_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = i.owner_user_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy inquiry invocation owner is invalid')
    END;
END;

CREATE UNIQUE INDEX ux_usage_invocation_attempt
    ON usage_invocation(
        COALESCE(owner_user_id, ''),
        COALESCE(project_id, ''),
        domain_kind,
        surface,
        source_id,
        COALESCE(candidate_key, ''),
        attempt_ordinal
    );
CREATE INDEX idx_usage_invocation_project_time
    ON usage_invocation(project_id, admitted_at DESC, id DESC);
CREATE INDEX idx_usage_invocation_owner_time
    ON usage_invocation(owner_user_id, admitted_at DESC, id DESC);
CREATE INDEX idx_usage_invocation_lifecycle
    ON usage_invocation(lifecycle, updated_at ASC, id ASC);

-- An actual provider call must point at exactly one frozen admission
-- candidate.  The candidate row may exist without this link when routing
-- skips it, while the unique selection reference prevents two invocations
-- from claiming the same candidate.
CREATE TRIGGER usage_invocation_selection_guard_insert
BEFORE INSERT ON usage_invocation
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM pricing_selection s
            WHERE s.id = NEW.pricing_selection_id
              AND s.owner_user_id IS NEW.owner_user_id
              AND s.project_id IS NEW.project_id
              AND s.domain_kind = NEW.domain_kind
              AND s.surface = NEW.surface
              AND s.source_id = NEW.source_id
              AND s.execution_id IS NEW.execution_id
              AND s.task_id IS NEW.task_id
              AND s.candidate_key IS NEW.candidate_key
              AND s.attempt_ordinal = NEW.attempt_ordinal
              AND s.provenance_kind = NEW.provenance_kind
              AND (s.invocation_id IS NULL OR s.invocation_id = NEW.id)
              AND s.subject_id IS NEW.pricing_subject_id
              AND s.subject_revision_id IS NEW.pricing_subject_revision_id
              AND s.subject_revision_digest IS NEW.subject_revision_digest
              AND s.runtime_model IS NEW.admitted_runtime_model
              AND s.admitted_provider_id IS NEW.admitted_provider_id
              AND s.admitted_model_id IS NEW.admitted_model_id
        ) THEN RAISE(ABORT, 'usage invocation selection does not match admission')
    END;
END;

CREATE TRIGGER usage_invocation_selection_immutable_update
BEFORE UPDATE OF pricing_selection_id ON usage_invocation
WHEN OLD.pricing_selection_id IS NOT NEW.pricing_selection_id
BEGIN
    SELECT RAISE(ABORT, 'usage invocation selection is immutable');
END;

CREATE TRIGGER usage_invocation_identity_immutable_update
BEFORE UPDATE OF id, owner_user_id, project_id, domain_kind, surface, source_id,
    execution_id, task_id, domain_idempotency_key, candidate_key, attempt_ordinal,
    admitted_provider_id,
    admitted_model_id, admitted_runtime_model, pricing_selection_id,
    pricing_subject_id, pricing_subject_revision_id, subject_revision_digest,
    agent_id, profile_id, agent_name_snapshot, project_name_snapshot,
    executor_type, backend_kind, provenance_kind, admitted_at, created_at
ON usage_invocation
BEGIN
    SELECT RAISE(ABORT, 'usage invocation identity is immutable');
END;

CREATE TRIGGER usage_invocation_lifecycle_guard_update
BEFORE UPDATE OF lifecycle ON usage_invocation
WHEN OLD.lifecycle IS NOT NEW.lifecycle
    AND NOT (
        (OLD.lifecycle = 'admitted' AND NEW.lifecycle = 'started')
        OR (OLD.lifecycle = 'started'
            AND NEW.lifecycle IN ('pending_settlement', 'settled', 'unsettled'))
        OR (OLD.lifecycle = 'pending_settlement'
            AND NEW.lifecycle IN ('settled', 'unsettled'))
    )
BEGIN
    SELECT RAISE(ABORT, 'invalid usage invocation lifecycle transition');
END;

CREATE TRIGGER usage_invocation_started_at_guard_update
BEFORE UPDATE OF started_at ON usage_invocation
WHEN OLD.started_at IS NOT NEW.started_at
    AND NOT (
        OLD.started_at IS NULL
        AND NEW.started_at IS NOT NULL
        AND OLD.lifecycle = 'admitted'
        AND NEW.lifecycle = 'started'
    )
BEGIN
    SELECT RAISE(ABORT, 'usage invocation start time is immutable outside start');
END;

CREATE TRIGGER usage_invocation_settled_at_guard_update
BEFORE UPDATE OF settled_at ON usage_invocation
WHEN OLD.settled_at IS NOT NEW.settled_at
    AND NOT (
        OLD.settled_at IS NULL
        AND NEW.settled_at IS NOT NULL
        AND OLD.lifecycle IN ('started', 'pending_settlement')
        AND NEW.lifecycle IN ('settled', 'unsettled')
    )
BEGIN
    SELECT RAISE(ABORT, 'usage invocation settlement time is immutable outside settlement');
END;

CREATE TRIGGER usage_invocation_terminal_reason_guard_update
BEFORE UPDATE OF terminal_reason ON usage_invocation
WHEN OLD.terminal_reason IS NOT NEW.terminal_reason
    AND NOT (
        OLD.terminal_reason IS NULL
        AND NEW.terminal_reason IS NOT NULL
        AND OLD.lifecycle IN ('started', 'pending_settlement')
        AND NEW.lifecycle IN ('settled', 'unsettled')
    )
BEGIN
    SELECT RAISE(ABORT, 'usage invocation terminal reason is immutable outside termination');
END;

CREATE TRIGGER usage_invocation_telemetry_guard_update
BEFORE UPDATE OF telemetry_state ON usage_invocation
WHEN OLD.telemetry_state IS NOT NEW.telemetry_state
    AND NOT (
        (NEW.lifecycle = 'settled'
            AND OLD.lifecycle IN ('started', 'pending_settlement')
            AND NEW.telemetry_state IN ('metered', 'unmetered'))
        OR
        (NEW.lifecycle = 'unsettled'
            AND OLD.lifecycle IN ('started', 'pending_settlement')
            AND NEW.telemetry_state = 'unsettled')
    )
BEGIN
    SELECT RAISE(ABORT, 'usage invocation telemetry state is immutable outside settlement');
END;

-- Linking an invocation is the one permitted post-admission transition on a
-- selection.  A NULL transition is accepted only when SQLite is clearing the
-- optional back-reference as part of deleting the invocation; all frozen
-- candidate/pricing fields remain immutable.
CREATE TRIGGER pricing_selection_invocation_link_guard
BEFORE UPDATE OF invocation_id ON pricing_selection
WHEN OLD.invocation_id IS NOT NEW.invocation_id
BEGIN
    SELECT CASE
        WHEN NEW.invocation_id IS NULL
             AND EXISTS (
                 SELECT 1 FROM usage_invocation
                 WHERE id = OLD.invocation_id
             )
             AND NOT EXISTS (
                 SELECT 1 FROM project_deletion_guard g
                 WHERE g.project_id = OLD.project_id
             )
            THEN RAISE(ABORT, 'pricing selection invocation link is immutable')
        WHEN NEW.invocation_id IS NOT NULL
             AND (
                 OLD.invocation_id IS NOT NULL
                 OR NOT EXISTS (
                       SELECT 1 FROM usage_invocation i
                       WHERE i.id = NEW.invocation_id
                         AND i.pricing_selection_id = OLD.id
                       AND i.owner_user_id IS OLD.owner_user_id
                       AND i.project_id IS OLD.project_id
                       AND i.domain_kind = OLD.domain_kind
                       AND i.surface = OLD.surface
                       AND i.source_id = OLD.source_id
                       AND i.execution_id IS OLD.execution_id
                       AND i.task_id IS OLD.task_id
                       AND i.candidate_key IS OLD.candidate_key
                       AND i.attempt_ordinal = OLD.attempt_ordinal
                       AND i.provenance_kind = OLD.provenance_kind
                 )
             )
            THEN RAISE(ABORT, 'pricing selection invocation link is invalid')
    END;
END;

CREATE TRIGGER pricing_selection_immutable_update
BEFORE UPDATE OF id, owner_user_id, project_id, domain_kind, surface,
    source_id, candidate_key, attempt_ordinal, subject_id,
    subject_revision_id, subject_revision_digest, binding_id, rate_revision_id,
    catalog_snapshot_id, catalog_freshness, runtime_model, admitted_provider_id,
    admitted_model_id, source_kind, provenance_kind, execution_id, task_id,
    selection_status, selection_reason, selection_digest,
    selected_at, created_at
ON pricing_selection
BEGIN
    SELECT RAISE(ABORT, 'admitted pricing selections are immutable');
END;

CREATE TRIGGER pricing_selection_immutable_delete
BEFORE DELETE ON pricing_selection
WHEN (
    (OLD.project_id IS NOT NULL
        AND EXISTS (SELECT 1 FROM project WHERE id = OLD.project_id)
        AND NOT EXISTS (
            SELECT 1 FROM project_deletion_guard g
            WHERE g.project_id = OLD.project_id
        ))
    OR
    (OLD.project_id IS NULL
        AND EXISTS (SELECT 1 FROM user WHERE id = OLD.owner_user_id))
)
BEGIN
    SELECT RAISE(ABORT, 'pricing selections require guarded teardown');
END;

CREATE TRIGGER usage_invocation_immutable_delete
BEFORE DELETE ON usage_invocation
WHEN (
    (OLD.project_id IS NOT NULL
        AND EXISTS (SELECT 1 FROM project WHERE id = OLD.project_id)
        AND NOT EXISTS (
            SELECT 1 FROM project_deletion_guard g
            WHERE g.project_id = OLD.project_id
        ))
    OR
    (OLD.project_id IS NULL
        AND EXISTS (SELECT 1 FROM user WHERE id = OLD.owner_user_id))
)
BEGIN
    SELECT RAISE(ABORT, 'usage invocations require guarded teardown');
END;

-- Each event is a non-overlapping delta/final report identified by the
-- producer report identity.  SQLite can enforce identity and non-negative
-- buckets; the producer/service contract enforces that two distinct reports
-- never describe overlapping cumulative usage.
CREATE TABLE usage_event (
    id                              TEXT NOT NULL PRIMARY KEY,
    invocation_id                  TEXT NOT NULL
                                        REFERENCES usage_invocation(id) ON DELETE CASCADE,
    owner_user_id                  TEXT,
    project_id                     TEXT REFERENCES project(id) ON DELETE CASCADE,
    surface                        TEXT NOT NULL
                                        CHECK (surface IN (
                                            'task_execution', 'project_chat', 'main_chat',
                                            'genesis_chat', 'main_inquiry'
                                        )),
    source_id                      TEXT NOT NULL,
    execution_id                   TEXT,
    task_id                        TEXT,
    event_idempotency_key          TEXT NOT NULL UNIQUE
                                        CHECK (length(trim(event_idempotency_key)) > 0),
    source_report_id               TEXT NOT NULL
                                        CHECK (length(trim(source_report_id)) > 0
                                            OR provenance_kind != 'runtime_report'),
    report_sequence                INTEGER NOT NULL DEFAULT 0
                                        CHECK (typeof(report_sequence) = 'integer'
                                            AND report_sequence >= 0),
    report_mode                    TEXT NOT NULL
                                        CHECK (report_mode IN (
                                            'delta', 'final_snapshot', 'reported_money',
                                            'legacy_aggregate'
                                        )),
    provenance_kind                TEXT NOT NULL
                                        CHECK (provenance_kind IN (
                                            'runtime_report', 'legacy_execution_aggregate',
                                            'legacy_chat', 'legacy_inquiry'
                                        )),
    legacy_source_table            TEXT
                                        CHECK (legacy_source_table IS NULL
                                            OR length(trim(legacy_source_table)) > 0),
    legacy_source_id               TEXT,
    legacy_provider_raw            TEXT,
    legacy_provider_sqlite_type    TEXT,
    legacy_provider_sql_literal    TEXT,
    legacy_model_raw               TEXT,
    legacy_model_sqlite_type       TEXT,
    legacy_model_sql_literal       TEXT,
    legacy_counter_values_json     TEXT NOT NULL DEFAULT '{}'
                                        CHECK (json_valid(legacy_counter_values_json)
                                            AND length(legacy_counter_values_json) <= 4096),
    legacy_cost_usd_raw            TEXT,
    legacy_created_at_raw          TEXT,
    legacy_project_owner_raw       TEXT,
    legacy_invalid_usage           INTEGER NOT NULL DEFAULT 0
                                        CHECK (typeof(legacy_invalid_usage) = 'integer'
                                            AND legacy_invalid_usage IN (0, 1)),
    provider_id                    TEXT
                                        CHECK (provider_id IS NULL OR length(trim(provider_id)) > 0),
    model_id                       TEXT
                                        CHECK (model_id IS NULL OR length(trim(model_id)) > 0),
    runtime_model                  TEXT
                                        CHECK (runtime_model IS NULL OR length(trim(runtime_model)) > 0),
    candidate_key                  TEXT
                                        CHECK (candidate_key IS NULL
                                            OR length(trim(candidate_key)) > 0),
    attempt_ordinal                INTEGER NOT NULL DEFAULT 0
                                        CHECK (typeof(attempt_ordinal) = 'integer'
                                            AND attempt_ordinal >= 0),
    agent_id                       TEXT,
    profile_id                     TEXT,
    agent_name_snapshot            TEXT,
    project_name_snapshot          TEXT,
    executor_type                  TEXT,
    pricing_subject_revision_id    TEXT,
    subject_revision_digest        TEXT
                                        CHECK (pricing_subject_revision_id IS NULL
                                            = (subject_revision_digest IS NULL)),
    telemetry_state                TEXT NOT NULL
                                        CHECK (telemetry_state IN ('metered', 'unmetered')),
    input_tokens                   INTEGER CHECK (
                                        input_tokens IS NULL
                                        OR (typeof(input_tokens) = 'integer' AND input_tokens >= 0)
                                    ),
    output_tokens                  INTEGER CHECK (
                                        output_tokens IS NULL
                                        OR (typeof(output_tokens) = 'integer' AND output_tokens >= 0)
                                    ),
    cache_read_tokens              INTEGER CHECK (
                                        cache_read_tokens IS NULL
                                        OR (typeof(cache_read_tokens) = 'integer'
                                            AND cache_read_tokens >= 0)
                                    ),
    cache_write_tokens             INTEGER CHECK (
                                        cache_write_tokens IS NULL
                                        OR (typeof(cache_write_tokens) = 'integer'
                                            AND cache_write_tokens >= 0)
                                    ),
    context_tokens                 INTEGER CHECK (
                                        context_tokens IS NULL
                                        OR (typeof(context_tokens) = 'integer'
                                            AND context_tokens >= 0)
                                    ),
    selected_tier                  TEXT
                                        CHECK (selected_tier IS NULL OR length(trim(selected_tier)) > 0),
    provider_reported_nano_usd     INTEGER CHECK (
                                        provider_reported_nano_usd IS NULL
                                        OR (typeof(provider_reported_nano_usd) = 'integer'
                                            AND provider_reported_nano_usd >= 0)
                                    ),
    legacy_reported_cost_usd       REAL CHECK (
                                        legacy_reported_cost_usd IS NULL
                                        OR (typeof(legacy_reported_cost_usd) = 'real'
                                            AND legacy_reported_cost_usd >= 0
                                            AND legacy_reported_cost_usd <= 1.7976931348623157e308
                                            AND legacy_reported_cost_usd = legacy_reported_cost_usd)
                                    ),
    estimated_nano_usd             INTEGER CHECK (
                                        estimated_nano_usd IS NULL
                                        OR (typeof(estimated_nano_usd) = 'integer'
                                            AND estimated_nano_usd >= 0)
                                    ),
    cost_kind                      TEXT NOT NULL
                                        CHECK (cost_kind IN (
                                            'provider_reported', 'estimated', 'none'
                                        )),
    rate_revision_id               TEXT REFERENCES pricing_rate_revision(id) ON DELETE RESTRICT,
    catalog_snapshot_id            TEXT REFERENCES pricing_catalog_snapshot(id) ON DELETE RESTRICT,
    formula_revision               TEXT
                                        CHECK (formula_revision IS NULL
                                            OR length(trim(formula_revision)) > 0),
    retrospective                  INTEGER NOT NULL DEFAULT 0
                                        CHECK (typeof(retrospective) = 'integer'
                                            AND retrospective IN (0, 1)),
    coverage_reason_code            TEXT CHECK (
                                        coverage_reason_code IS NULL OR coverage_reason_code IN (
                                            'pending', 'unsettled', 'unmetered',
                                            'missing_provider', 'missing_model',
                                            'missing_binding', 'missing_rate',
                                            'unresolved_tier', 'identity_mismatch',
                                            'invalid_legacy_usage'
                                        )
                                    ),
    occurred_at                    TEXT NOT NULL,
    created_at                     TEXT NOT NULL CHECK (
                                        length(trim(created_at)) > 0
                                        OR provenance_kind != 'runtime_report'
                                    ),
    UNIQUE (invocation_id, source_report_id),
    UNIQUE (invocation_id, report_mode, report_sequence),
    CHECK (
        (surface IN ('task_execution', 'project_chat') AND project_id IS NOT NULL)
        OR (surface IN ('main_chat', 'main_inquiry', 'genesis_chat')
            AND project_id IS NULL)
        OR (surface = 'genesis_chat' AND project_id IS NOT NULL)
    ),
    CHECK (
        (telemetry_state = 'metered'
            AND (input_tokens IS NOT NULL OR output_tokens IS NOT NULL
                OR cache_read_tokens IS NOT NULL OR cache_write_tokens IS NOT NULL))
        OR
        (telemetry_state = 'unmetered'
            AND input_tokens IS NULL AND output_tokens IS NULL
            AND cache_read_tokens IS NULL AND cache_write_tokens IS NULL)
    ),
    CHECK (
        input_tokens IS NOT NULL OR output_tokens IS NOT NULL
        OR cache_read_tokens IS NOT NULL OR cache_write_tokens IS NOT NULL
        OR provider_reported_nano_usd IS NOT NULL
        OR legacy_reported_cost_usd IS NOT NULL
        OR (provenance_kind != 'runtime_report'
            AND legacy_counter_values_json != '{}')
    ),
    CHECK (
        (cost_kind = 'provider_reported'
            AND (provider_reported_nano_usd IS NOT NULL OR legacy_reported_cost_usd IS NOT NULL)
            AND estimated_nano_usd IS NULL
            AND coverage_reason_code IS NULL)
        OR
        (cost_kind = 'estimated'
            AND estimated_nano_usd IS NOT NULL
            AND provider_reported_nano_usd IS NULL
            AND legacy_reported_cost_usd IS NULL
            AND formula_revision IS NOT NULL
            AND coverage_reason_code IS NULL)
        OR
        (cost_kind = 'none'
            AND provider_reported_nano_usd IS NULL
            AND legacy_reported_cost_usd IS NULL
            AND estimated_nano_usd IS NULL
            AND coverage_reason_code IS NOT NULL)
    ),
    CHECK (cost_kind != 'provider_reported' OR retrospective = 0),
    CHECK (cost_kind != 'estimated' OR rate_revision_id IS NOT NULL),
    CHECK (catalog_snapshot_id IS NULL OR rate_revision_id IS NOT NULL),
    CHECK (
        length(trim(source_id)) > 0
        OR provenance_kind != 'runtime_report'
    ),
    CHECK (
        (provenance_kind = 'runtime_report'
            AND legacy_source_table IS NULL
            AND legacy_source_id IS NULL
            AND legacy_provider_raw IS NULL
            AND legacy_provider_sqlite_type IS NULL
            AND legacy_provider_sql_literal IS NULL
            AND legacy_model_raw IS NULL
            AND legacy_model_sqlite_type IS NULL
            AND legacy_model_sql_literal IS NULL
            AND legacy_cost_usd_raw IS NULL
            AND legacy_created_at_raw IS NULL
            AND legacy_project_owner_raw IS NULL
            AND legacy_invalid_usage = 0)
        OR
        (provenance_kind != 'runtime_report'
            AND legacy_source_table IS NOT NULL
            AND legacy_source_id IS NOT NULL
            AND legacy_provider_sqlite_type IS NOT NULL
            AND legacy_provider_sql_literal IS NOT NULL
            AND legacy_model_sqlite_type IS NOT NULL
            AND legacy_model_sql_literal IS NOT NULL
            AND legacy_cost_usd_raw IS NOT NULL
            AND legacy_created_at_raw IS NOT NULL
            AND legacy_project_owner_raw IS NOT NULL)
    ),
    CHECK (
        (provenance_kind = 'runtime_report'
            AND report_mode IN ('delta', 'final_snapshot', 'reported_money'))
        OR
        (provenance_kind IN (
                'legacy_execution_aggregate', 'legacy_chat', 'legacy_inquiry'
            ) AND report_mode = 'legacy_aggregate')
    ),
    CHECK (report_mode != 'reported_money' OR cost_kind = 'provider_reported'),
    CHECK (
        report_mode != 'reported_money'
        OR (telemetry_state = 'unmetered'
            AND input_tokens IS NULL
            AND output_tokens IS NULL
            AND cache_read_tokens IS NULL
            AND cache_write_tokens IS NULL)
    ),
    CHECK (
        provider_reported_nano_usd IS NULL
        OR legacy_reported_cost_usd IS NULL
    )
);

-- Keep ownership strict for runtime and account-only rows while allowing a
-- historical Project/Genesis/Task record to remain ownerless when its own
-- Project was ownerless.  The legacy owner, when present, must still match
-- the Project owner; display/raw provenance fields never confer ownership.
CREATE TRIGGER usage_event_owner_scope_guard_insert
BEFORE INSERT ON usage_event
BEGIN
    SELECT CASE
        WHEN NEW.owner_user_id IS NULL
             AND NOT (
                 (
                     NEW.project_id IS NOT NULL
                     AND NEW.provenance_kind IN (
                         'legacy_execution_aggregate', 'legacy_chat'
                     )
                     AND EXISTS (
                         SELECT 1 FROM project p
                         WHERE p.id = NEW.project_id
                           AND (p.owner_id IS NULL OR NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = p.owner_id
                           ))
                     )
                 )
                 OR (
                     NEW.provenance_kind = 'legacy_chat'
                     AND NEW.project_id IS NULL
                     AND EXISTS (
                         SELECT 1
                         FROM agent_chat_message m
                         JOIN agent_chat c ON c.id = m.chat_id
                         WHERE m.id = NEW.source_id
                           AND c.kind = 'account_main'
                           AND (c.account_id IS NULL OR NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = c.account_id
                           ))
                     )
                 )
                 OR (
                     NEW.provenance_kind = 'legacy_inquiry'
                     AND NEW.project_id IS NULL
                     AND EXISTS (
                         SELECT 1 FROM agent_inquiry i
                         WHERE i.id = NEW.source_id
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = i.owner_user_id
                           )
                     )
                 )
             )
            THEN RAISE(ABORT, 'usage event owner is required outside legacy ownerless Projects')
        WHEN NEW.provenance_kind IN ('legacy_execution_aggregate', 'legacy_chat')
             AND NEW.project_id IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1 FROM project p
                 WHERE p.id = NEW.project_id
                   AND (
                       p.owner_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = p.owner_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy usage event owner must match Project owner')
        WHEN NEW.provenance_kind = 'legacy_chat'
             AND NEW.project_id IS NULL
             AND NOT EXISTS (
                 SELECT 1
                 FROM agent_chat_message m
                 JOIN agent_chat c ON c.id = m.chat_id
                 WHERE m.id = NEW.source_id
                   AND c.kind = 'account_main'
                   AND (
                       c.account_id IS NEW.owner_user_id
                       OR (NEW.owner_user_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM user u WHERE u.id = c.account_id
                           ))
                   )
             )
            THEN RAISE(ABORT, 'legacy account chat usage owner is invalid')
        WHEN NEW.provenance_kind = 'legacy_inquiry'
             AND (
                 NEW.project_id IS NOT NULL
                 OR NOT EXISTS (
                     SELECT 1 FROM agent_inquiry i
                     WHERE i.id = NEW.source_id
                       AND (
                           i.owner_user_id IS NEW.owner_user_id
                           OR (NEW.owner_user_id IS NULL
                               AND NOT EXISTS (
                                   SELECT 1 FROM user u WHERE u.id = i.owner_user_id
                               ))
                       )
                 )
             )
            THEN RAISE(ABORT, 'legacy inquiry usage must be account-scoped')
        WHEN NEW.provenance_kind = 'runtime_report'
             AND (NEW.owner_user_id IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM user u WHERE u.id = NEW.owner_user_id
                  ))
            THEN RAISE(ABORT, 'runtime usage event owner must be a real user')
    END;
END;

CREATE INDEX idx_usage_event_invocation
    ON usage_event(invocation_id, occurred_at ASC, id ASC);
CREATE INDEX idx_usage_event_project_time
    ON usage_event(project_id, occurred_at ASC, id ASC);
CREATE INDEX idx_usage_event_owner_time
    ON usage_event(owner_user_id, occurred_at ASC, id ASC);
CREATE INDEX idx_usage_event_provider_model
    ON usage_event(provider_id, model_id, occurred_at ASC, id ASC);

-- Usage rows are provider reports for a settled invocation, not a second
-- admission authority.  Scope, candidate identity, snapshots, and selected
-- pricing provenance must match the invocation and its frozen selection;
-- actual provider/model fields may differ only for an unpriced/reporting
-- outcome, never for a Forge estimate.
CREATE TRIGGER usage_event_invocation_guard_insert
BEFORE INSERT ON usage_event
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM usage_invocation i
            JOIN pricing_selection s ON s.id = i.pricing_selection_id
            WHERE i.id = NEW.invocation_id
              AND i.owner_user_id IS NEW.owner_user_id
              AND i.project_id IS NEW.project_id
              AND i.domain_kind = CASE
                  WHEN NEW.surface = 'task_execution' THEN 'execution'
                  WHEN NEW.surface = 'main_inquiry' THEN 'inquiry'
                  ELSE 'chat'
              END
              AND i.surface = NEW.surface
              AND i.source_id = NEW.source_id
              AND i.execution_id IS NEW.execution_id
              AND i.task_id IS NEW.task_id
              AND i.candidate_key IS NEW.candidate_key
              AND i.attempt_ordinal = NEW.attempt_ordinal
              AND i.lifecycle = 'settled'
              AND i.telemetry_state = NEW.telemetry_state
              AND i.agent_id IS NEW.agent_id
              AND i.profile_id IS NEW.profile_id
              AND i.executor_type IS NEW.executor_type
              AND i.pricing_subject_revision_id IS NEW.pricing_subject_revision_id
              AND i.subject_revision_digest IS NEW.subject_revision_digest
              AND i.agent_name_snapshot IS NEW.agent_name_snapshot
              AND i.project_name_snapshot IS NEW.project_name_snapshot
              AND NEW.rate_revision_id IS s.rate_revision_id
              AND NEW.catalog_snapshot_id IS s.catalog_snapshot_id
              AND (
                  (NEW.provenance_kind = 'runtime_report'
                      AND i.provenance_kind = 'runtime')
                  OR
                  (NEW.provenance_kind = 'legacy_execution_aggregate'
                      AND i.provenance_kind = 'legacy_execution_aggregate'
                      AND i.domain_kind = 'execution')
                  OR
                  (NEW.provenance_kind = 'legacy_chat'
                      AND i.provenance_kind = 'legacy_chat'
                      AND i.domain_kind = 'chat')
                  OR
                  (NEW.provenance_kind = 'legacy_inquiry'
                      AND i.provenance_kind = 'legacy_inquiry'
                      AND i.domain_kind = 'inquiry')
              )
              AND (
                  NEW.cost_kind != 'estimated'
                  OR (NEW.provider_id IS i.admitted_provider_id
                      AND NEW.model_id IS i.admitted_model_id
                      AND NEW.runtime_model IS i.admitted_runtime_model)
              )
        ) THEN RAISE(ABORT, 'usage event does not match invocation admission')
    END;
END;

CREATE TRIGGER usage_event_immutable_update
BEFORE UPDATE ON usage_event
BEGIN
    SELECT RAISE(ABORT, 'usage events are append-only');
END;

-- A direct event delete is forbidden.  The parent-existence guard permits the
-- existing account/project cascades to remove an invocation and its child
-- events together, while preventing deletion of an event during normal use.
CREATE TRIGGER usage_event_immutable_delete
BEFORE DELETE ON usage_event
WHEN (
    (OLD.project_id IS NOT NULL
        AND EXISTS (SELECT 1 FROM project WHERE id = OLD.project_id)
        AND NOT EXISTS (
            SELECT 1 FROM project_deletion_guard g
            WHERE g.project_id = OLD.project_id
        ))
    OR
    (OLD.project_id IS NULL
        AND EXISTS (SELECT 1 FROM user WHERE id = OLD.owner_user_id))
    OR
    (OLD.project_id IS NULL
        AND OLD.provenance_kind = 'legacy_inquiry'
        AND OLD.owner_user_id IS NULL)
)
BEGIN
    SELECT RAISE(ABORT, 'usage events require guarded teardown');
END;

-- Remote terminal deliveries are durable receipts, not transport-session
-- state.  The primary key makes a report id globally unique across
-- executions; the digest binds it to the complete terminal notification.
-- Deliberately keep these receipts independent of execution/domain-event
-- foreign keys so account/project teardown cannot erase the idempotency
-- authority and permit a later report-id reuse.
CREATE TABLE execution_terminal_receipt (
    terminal_report_id TEXT NOT NULL PRIMARY KEY
                                CHECK (length(trim(terminal_report_id)) > 0
                                    AND length(terminal_report_id) <= 512),
    execution_id       TEXT NOT NULL
                                CHECK (length(trim(execution_id)) > 0),
    payload_digest     TEXT NOT NULL
                                CHECK (length(trim(payload_digest)) = 64),
    event_id           TEXT NOT NULL
                                CHECK (length(trim(event_id)) > 0),
    created_at         TEXT NOT NULL
                                CHECK (length(trim(created_at)) > 0)
);

CREATE INDEX idx_execution_terminal_receipt_execution
    ON execution_terminal_receipt(execution_id, created_at DESC, terminal_report_id);

CREATE TRIGGER execution_terminal_receipt_immutable_update
BEFORE UPDATE ON execution_terminal_receipt
BEGIN
    SELECT RAISE(ABORT, 'execution terminal receipts are immutable');
END;

CREATE TRIGGER execution_terminal_receipt_immutable_delete
BEFORE DELETE ON execution_terminal_receipt
BEGIN
    SELECT RAISE(ABORT, 'execution terminal receipts are immutable');
END;

-- Retrospective pricing is explicit.  A preview freezes the exact usage-set
-- digest and snapshot; a run is a mutable lifecycle/idempotency envelope; and
-- each estimate revision is immutable and can supersede an earlier revision
-- without rewriting the original usage event or provider-reported amount.
CREATE TABLE cost_estimation_preview (
    id                          TEXT NOT NULL PRIMARY KEY,
    owner_user_id               TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    project_id                  TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    catalog_snapshot_id         TEXT NOT NULL
                                    REFERENCES pricing_catalog_snapshot(id) ON DELETE RESTRICT,
    catalog_freshness           TEXT NOT NULL
                                    CHECK (catalog_freshness IN (
                                        'fresh', 'stale', 'refresh_failed'
                                    )),
    usage_set_digest             TEXT NOT NULL
                                    CHECK (length(trim(usage_set_digest)) > 0),
    window_from                 TEXT
                                    CHECK (window_from IS NULL OR length(trim(window_from)) > 0),
    window_to                   TEXT
                                    CHECK (window_to IS NULL OR length(trim(window_to)) > 0),
    filters_json                TEXT NOT NULL DEFAULT '{}'
                                    CHECK (json_valid(filters_json)),
    eligible_event_count        INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(eligible_event_count) = 'integer'
                                        AND eligible_event_count >= 0),
    unmatched_event_count       INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(unmatched_event_count) = 'integer'
                                        AND unmatched_event_count >= 0),
    already_reported_event_count INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(already_reported_event_count) = 'integer'
                                        AND already_reported_event_count >= 0),
    projected_cost_summary_json TEXT NOT NULL DEFAULT '{}'
                                    CHECK (json_valid(projected_cost_summary_json)),
    status                      TEXT NOT NULL DEFAULT 'active'
                                    CHECK (status IN ('active', 'expired', 'committed', 'invalidated')),
    idempotency_key             TEXT NOT NULL UNIQUE
                                    CHECK (length(trim(idempotency_key)) > 0),
    version                     INTEGER NOT NULL DEFAULT 1
                                    CHECK (typeof(version) = 'integer' AND version >= 1),
    expires_at                  TEXT NOT NULL CHECK (length(trim(expires_at)) > 0),
    created_at                  TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at                  TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    CHECK (window_from IS NULL OR window_to IS NULL OR window_from < window_to)
);

CREATE INDEX idx_cost_estimation_preview_project
    ON cost_estimation_preview(project_id, created_at DESC, id DESC);
CREATE INDEX idx_cost_estimation_preview_status
    ON cost_estimation_preview(status, expires_at ASC, id ASC);

CREATE TABLE cost_estimation_run (
    id                          TEXT NOT NULL PRIMARY KEY,
    owner_user_id               TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    project_id                  TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    preview_id                  TEXT NOT NULL
                                    REFERENCES cost_estimation_preview(id) ON DELETE CASCADE,
    catalog_snapshot_id         TEXT NOT NULL
                                    REFERENCES pricing_catalog_snapshot(id) ON DELETE RESTRICT,
    usage_set_digest             TEXT NOT NULL
                                    CHECK (length(trim(usage_set_digest)) > 0),
    status                      TEXT NOT NULL DEFAULT 'pending'
                                    CHECK (status IN (
                                        'pending', 'committed', 'failed', 'conflicted', 'superseded'
                                    )),
    applied_event_count         INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(applied_event_count) = 'integer'
                                        AND applied_event_count >= 0),
    unmatched_event_count       INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(unmatched_event_count) = 'integer'
                                        AND unmatched_event_count >= 0),
    already_reported_event_count INTEGER NOT NULL DEFAULT 0
                                    CHECK (typeof(already_reported_event_count) = 'integer'
                                        AND already_reported_event_count >= 0),
    cost_summary_json           TEXT NOT NULL DEFAULT '{}'
                                    CHECK (json_valid(cost_summary_json)),
    idempotency_key             TEXT NOT NULL UNIQUE
                                    CHECK (length(trim(idempotency_key)) > 0),
    supersedes_run_id           TEXT
                                    CHECK (supersedes_run_id IS NULL
                                        OR length(trim(supersedes_run_id)) > 0),
    version                     INTEGER NOT NULL DEFAULT 1
                                    CHECK (typeof(version) = 'integer' AND version >= 1),
    created_at                  TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at                  TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    completed_at                TEXT
                                    CHECK (completed_at IS NULL OR length(trim(completed_at)) > 0)
);

CREATE INDEX idx_cost_estimation_run_project
    ON cost_estimation_run(project_id, created_at DESC, id DESC);
CREATE INDEX idx_cost_estimation_run_status
    ON cost_estimation_run(status, updated_at ASC, id ASC);

CREATE TABLE cost_estimate_revision (
    id                          TEXT NOT NULL PRIMARY KEY,
    run_id                      TEXT NOT NULL
                                    REFERENCES cost_estimation_run(id) ON DELETE CASCADE,
    owner_user_id               TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    project_id                  TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    usage_event_id              TEXT NOT NULL,
    revision                    INTEGER NOT NULL
                                    CHECK (typeof(revision) = 'integer' AND revision >= 1),
    supersedes_revision_id      TEXT,
    state                       TEXT NOT NULL
                                    CHECK (state IN ('applied', 'unmatched', 'invalid')),
    rate_revision_id            TEXT REFERENCES pricing_rate_revision(id) ON DELETE RESTRICT,
    catalog_snapshot_id         TEXT REFERENCES pricing_catalog_snapshot(id) ON DELETE RESTRICT,
    estimated_nano_usd          INTEGER CHECK (
                                        estimated_nano_usd IS NULL
                                        OR (typeof(estimated_nano_usd) = 'integer'
                                            AND estimated_nano_usd >= 0)
                                    ),
    formula_revision             TEXT
                                    CHECK (formula_revision IS NULL
                                        OR length(trim(formula_revision)) > 0),
    retrospective               INTEGER NOT NULL DEFAULT 1
                                    CHECK (typeof(retrospective) = 'integer'
                                        AND retrospective = 1),
    reason_code                 TEXT CHECK (
                                    reason_code IS NULL OR reason_code IN (
                                        'missing_provider', 'missing_model', 'missing_binding',
                                        'missing_rate', 'unresolved_tier',
                                        'identity_mismatch', 'invalid_legacy_usage'
                                    )
                                ),
    estimate_digest             TEXT NOT NULL UNIQUE
                                    CHECK (length(trim(estimate_digest)) > 0),
    created_at                  TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    UNIQUE (usage_event_id, revision),
    UNIQUE (run_id, usage_event_id),
    CHECK (
        (state = 'applied'
            AND estimated_nano_usd IS NOT NULL
            AND rate_revision_id IS NOT NULL
            AND formula_revision IS NOT NULL
            AND reason_code IS NULL)
        OR
        (state IN ('unmatched', 'invalid')
            AND estimated_nano_usd IS NULL
            AND rate_revision_id IS NULL
            AND catalog_snapshot_id IS NULL
            AND reason_code IS NOT NULL)
    )
);

CREATE INDEX idx_cost_estimate_revision_run
    ON cost_estimate_revision(run_id, created_at ASC, id ASC);
CREATE INDEX idx_cost_estimate_revision_event
    ON cost_estimate_revision(usage_event_id, revision DESC, id DESC);
CREATE INDEX idx_cost_estimate_revision_project
    ON cost_estimate_revision(project_id, created_at ASC, id ASC);

CREATE TRIGGER cost_estimate_revision_immutable_update
BEFORE UPDATE ON cost_estimate_revision
BEGIN
    SELECT RAISE(ABORT, 'cost estimate revisions are immutable');
END;

CREATE TRIGGER cost_estimation_preview_identity_immutable_update
BEFORE UPDATE OF id, owner_user_id, project_id, catalog_snapshot_id,
    catalog_freshness, usage_set_digest, window_from, window_to, filters_json,
    idempotency_key,
    created_at
ON cost_estimation_preview
BEGIN
    SELECT RAISE(ABORT, 'cost estimation preview identity is immutable');
END;

CREATE TRIGGER cost_estimation_run_identity_immutable_update
BEFORE UPDATE OF id, owner_user_id, project_id, preview_id,
    catalog_snapshot_id, usage_set_digest, idempotency_key, supersedes_run_id,
    created_at
ON cost_estimation_run
BEGIN
    SELECT RAISE(ABORT, 'cost estimation run identity is immutable');
END;

CREATE TRIGGER cost_estimation_run_preview_guard_insert
BEFORE INSERT ON cost_estimation_run
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM cost_estimation_preview p
            WHERE p.id = NEW.preview_id
              AND p.owner_user_id = NEW.owner_user_id
              AND p.project_id = NEW.project_id
              AND p.catalog_snapshot_id = NEW.catalog_snapshot_id
              AND p.usage_set_digest = NEW.usage_set_digest
        ) THEN RAISE(ABORT, 'cost estimation run does not match preview')
    END;
END;

CREATE TRIGGER cost_estimate_revision_scope_guard_insert
BEFORE INSERT ON cost_estimate_revision
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM cost_estimation_run r
            JOIN usage_event e ON e.id = NEW.usage_event_id
            WHERE r.id = NEW.run_id
              AND r.owner_user_id = NEW.owner_user_id
              AND r.project_id = NEW.project_id
              AND e.owner_user_id = NEW.owner_user_id
              AND e.project_id = NEW.project_id
              AND e.project_id IS NOT NULL
              AND (
                  (NEW.state = 'applied'
                      AND NEW.catalog_snapshot_id = r.catalog_snapshot_id)
                  OR
                  (NEW.state IN ('unmatched', 'invalid')
                      AND NEW.catalog_snapshot_id IS NULL)
              )
        ) THEN RAISE(ABORT, 'cost estimate revision scope does not match run/event')
    END;
END;

CREATE TRIGGER cost_estimation_preview_immutable_delete
BEFORE DELETE ON cost_estimation_preview
WHEN EXISTS (
    SELECT 1 FROM project
    WHERE id = OLD.project_id
)
AND NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    WHERE g.project_id = OLD.project_id
)
BEGIN
    SELECT RAISE(ABORT, 'cost estimation previews require guarded teardown');
END;

CREATE TRIGGER cost_estimation_run_immutable_delete
BEFORE DELETE ON cost_estimation_run
WHEN EXISTS (
    SELECT 1 FROM project
    WHERE id = OLD.project_id
)
AND NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    WHERE g.project_id = OLD.project_id
)
BEGIN
    SELECT RAISE(ABORT, 'cost estimation runs require guarded teardown');
END;

CREATE TRIGGER cost_estimate_revision_immutable_delete
BEFORE DELETE ON cost_estimate_revision
WHEN EXISTS (
    SELECT 1 FROM project
    WHERE id = OLD.project_id
)
AND NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    WHERE g.project_id = OLD.project_id
)
BEGIN
    SELECT RAISE(ABORT, 'cost estimate revisions require guarded teardown');
END;

-- V022 retained one lossy aggregate per execution.  Preserve that exact
-- grain: each source row becomes one explicitly unpriced legacy candidate,
-- one settled invocation, and one usage event.  The event ID itself carries
-- the V022 primary key forward; the source table/key and raw SQLite literals
-- below make the original representation auditable even when SQLite stored a
-- value with a type different from the declared V022 affinity.
INSERT INTO pricing_selection (
    id, owner_user_id, project_id, domain_kind, surface, source_id,
    execution_id, task_id, candidate_key, attempt_ordinal, subject_id,
    subject_revision_id, subject_revision_digest, binding_id,
    rate_revision_id, catalog_snapshot_id, catalog_freshness, runtime_model,
    admitted_provider_id, admitted_model_id, source_kind, provenance_kind,
    selection_status, selection_reason, selection_digest, selected_at, created_at
)
SELECT
    'legacy-execution-selection:' || CAST(eu.id AS TEXT),
    CASE
        WHEN EXISTS (SELECT 1 FROM user u WHERE u.id = p.owner_id)
            THEN p.owner_id
        ELSE NULL
    END,
    p.id,
    'execution',
    'task_execution',
    CAST(e.id AS TEXT),
    CAST(e.id AS TEXT),
    CAST(t.id AS TEXT),
    'legacy-execution-candidate:' || quote(eu.id),
    0,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    CASE
        WHEN typeof(eu.model) = 'text' AND length(trim(eu.model)) > 0
            THEN eu.model
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.provider) = 'text' AND length(trim(eu.provider)) > 0
            THEN eu.provider
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.model) = 'text' AND length(trim(eu.model)) > 0
            THEN eu.model
        ELSE NULL
    END,
    NULL,
    'legacy_execution_aggregate',
    'unpriced',
    NULL,
    'legacy-execution-selection-digest:' || quote(eu.id),
    CAST(eu.created_at AS TEXT),
    CAST(eu.created_at AS TEXT)
FROM execution_usage eu
JOIN execution e ON e.id = eu.execution_id
JOIN task t ON t.id = e.task_id
JOIN project p ON p.id = t.project_id;

INSERT INTO usage_invocation (
    id, owner_user_id, project_id, domain_kind, surface, source_id,
    execution_id, task_id, domain_idempotency_key, candidate_key,
    attempt_ordinal, pricing_selection_id, admitted_provider_id,
    admitted_model_id, admitted_runtime_model, pricing_subject_id,
    pricing_subject_revision_id, subject_revision_digest, agent_id, profile_id,
    agent_name_snapshot, project_name_snapshot, executor_type, backend_kind,
    provenance_kind, lifecycle, telemetry_state, terminal_reason, version,
    admitted_at, started_at, settled_at, created_at, updated_at
)
SELECT
    'legacy-execution-invocation:' || CAST(eu.id AS TEXT),
    CASE
        WHEN EXISTS (SELECT 1 FROM user u WHERE u.id = p.owner_id)
            THEN p.owner_id
        ELSE NULL
    END,
    p.id,
    'execution',
    'task_execution',
    CAST(e.id AS TEXT),
    CAST(e.id AS TEXT),
    CAST(t.id AS TEXT),
    'legacy-execution-invocation-key:' || quote(eu.id),
    'legacy-execution-candidate:' || quote(eu.id),
    0,
    'legacy-execution-selection:' || CAST(eu.id AS TEXT),
    CASE
        WHEN typeof(eu.provider) = 'text' AND length(trim(eu.provider)) > 0
            THEN eu.provider
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.model) = 'text' AND length(trim(eu.model)) > 0
            THEN eu.model
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.model) = 'text' AND length(trim(eu.model)) > 0
            THEN eu.model
        ELSE NULL
    END,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    'legacy_execution_aggregate',
    'settled',
    CASE
        WHEN typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
         AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
         AND typeof(eu.cache_read_tokens) = 'integer' AND eu.cache_read_tokens >= 0
         AND typeof(eu.cache_write_tokens) = 'integer' AND eu.cache_write_tokens >= 0
            THEN 'metered'
        ELSE 'unmetered'
    END,
    CASE
        WHEN typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
         AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
         AND typeof(eu.cache_read_tokens) = 'integer' AND eu.cache_read_tokens >= 0
         AND typeof(eu.cache_write_tokens) = 'integer' AND eu.cache_write_tokens >= 0
            THEN NULL
        ELSE 'legacy row retained with invalid or non-integer telemetry'
    END,
    1,
    CAST(eu.created_at AS TEXT),
    CAST(eu.created_at AS TEXT),
    CAST(eu.created_at AS TEXT),
    CAST(eu.created_at AS TEXT),
    CAST(eu.created_at AS TEXT)
FROM execution_usage eu
JOIN execution e ON e.id = eu.execution_id
JOIN task t ON t.id = e.task_id
JOIN project p ON p.id = t.project_id;

UPDATE pricing_selection
SET invocation_id = 'legacy-execution-invocation:' || CAST(
    (SELECT eu.id
     FROM execution_usage eu
     WHERE 'legacy-execution-selection:' || CAST(eu.id AS TEXT) = pricing_selection.id
    ) AS TEXT
)
WHERE provenance_kind = 'legacy_execution_aggregate';

INSERT INTO usage_event (
    id, invocation_id, owner_user_id, project_id, surface, source_id,
    execution_id, task_id, event_idempotency_key, source_report_id,
    report_sequence, report_mode, provenance_kind, legacy_source_table,
    legacy_source_id, legacy_provider_raw, legacy_provider_sqlite_type,
    legacy_provider_sql_literal, legacy_model_raw, legacy_model_sqlite_type,
    legacy_model_sql_literal,
    legacy_counter_values_json, legacy_cost_usd_raw, legacy_created_at_raw,
    legacy_project_owner_raw, legacy_invalid_usage, provider_id, model_id,
    runtime_model, candidate_key,
    attempt_ordinal, agent_id, profile_id, agent_name_snapshot,
    project_name_snapshot, executor_type, pricing_subject_revision_id,
    subject_revision_digest, telemetry_state, input_tokens, output_tokens,
    cache_read_tokens, cache_write_tokens, context_tokens, selected_tier,
    provider_reported_nano_usd, legacy_reported_cost_usd, estimated_nano_usd,
    cost_kind, rate_revision_id, catalog_snapshot_id, formula_revision,
    retrospective, coverage_reason_code, occurred_at, created_at
)
SELECT
    CAST(eu.id AS TEXT),
    'legacy-execution-invocation:' || CAST(eu.id AS TEXT),
    CASE
        WHEN EXISTS (SELECT 1 FROM user u WHERE u.id = p.owner_id)
            THEN p.owner_id
        ELSE NULL
    END,
    p.id,
    'task_execution',
    CAST(e.id AS TEXT),
    CAST(e.id AS TEXT),
    CAST(t.id AS TEXT),
    'legacy-execution-usage-event:' || quote(eu.id),
    CAST(eu.id AS TEXT),
    0,
    'legacy_aggregate',
    'legacy_execution_aggregate',
    'execution_usage',
    CAST(eu.id AS TEXT),
    CASE WHEN typeof(eu.provider) = 'text' THEN eu.provider ELSE NULL END,
    typeof(eu.provider),
    quote(eu.provider),
    CASE WHEN typeof(eu.model) = 'text' THEN eu.model ELSE NULL END,
    typeof(eu.model),
    quote(eu.model),
    json_object(
        'input_tokens', json_object(
            'sqlite_type', typeof(eu.input_tokens),
            'sql_literal', quote(eu.input_tokens)
        ),
        'output_tokens', json_object(
            'sqlite_type', typeof(eu.output_tokens),
            'sql_literal', quote(eu.output_tokens)
        ),
        'cache_read_tokens', json_object(
            'sqlite_type', typeof(eu.cache_read_tokens),
            'sql_literal', quote(eu.cache_read_tokens)
        ),
        'cache_write_tokens', json_object(
            'sqlite_type', typeof(eu.cache_write_tokens),
            'sql_literal', quote(eu.cache_write_tokens)
        )
    ),
    quote(eu.cost_usd),
    quote(eu.created_at),
    quote(p.owner_id),
    CASE
        WHEN typeof(eu.input_tokens) != 'integer' OR eu.input_tokens < 0
          OR typeof(eu.output_tokens) != 'integer' OR eu.output_tokens < 0
          OR typeof(eu.cache_read_tokens) != 'integer' OR eu.cache_read_tokens < 0
          OR typeof(eu.cache_write_tokens) != 'integer' OR eu.cache_write_tokens < 0
          OR typeof(eu.provider) != 'text'
          OR typeof(eu.model) != 'text'
          OR length(trim(CAST(eu.created_at AS TEXT))) = 0
          OR (
              eu.cost_usd IS NOT NULL
              AND (typeof(eu.cost_usd) NOT IN ('integer', 'real')
                   OR eu.cost_usd < 0
                   OR eu.cost_usd > 1.7976931348623157e308
                   OR eu.cost_usd != eu.cost_usd)
          )
            THEN 1
        ELSE 0
    END,
    CASE
        WHEN typeof(eu.provider) = 'text' AND length(trim(eu.provider)) > 0
            THEN eu.provider
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.model) = 'text' AND length(trim(eu.model)) > 0
            THEN eu.model
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.model) = 'text' AND length(trim(eu.model)) > 0
            THEN eu.model
        ELSE NULL
    END,
    'legacy-execution-candidate:' || quote(eu.id),
    0,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    CASE
        WHEN typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
         AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
         AND typeof(eu.cache_read_tokens) = 'integer' AND eu.cache_read_tokens >= 0
         AND typeof(eu.cache_write_tokens) = 'integer' AND eu.cache_write_tokens >= 0
            THEN 'metered'
        ELSE 'unmetered'
    END,
    CASE
        WHEN typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
         AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
         AND typeof(eu.cache_read_tokens) = 'integer' AND eu.cache_read_tokens >= 0
         AND typeof(eu.cache_write_tokens) = 'integer' AND eu.cache_write_tokens >= 0
            THEN eu.input_tokens
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
         AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
         AND typeof(eu.cache_read_tokens) = 'integer' AND eu.cache_read_tokens >= 0
         AND typeof(eu.cache_write_tokens) = 'integer' AND eu.cache_write_tokens >= 0
            THEN eu.output_tokens
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
         AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
         AND typeof(eu.cache_read_tokens) = 'integer' AND eu.cache_read_tokens >= 0
         AND typeof(eu.cache_write_tokens) = 'integer' AND eu.cache_write_tokens >= 0
            THEN eu.cache_read_tokens
        ELSE NULL
    END,
    CASE
        WHEN typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
         AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
         AND typeof(eu.cache_read_tokens) = 'integer' AND eu.cache_read_tokens >= 0
         AND typeof(eu.cache_write_tokens) = 'integer' AND eu.cache_write_tokens >= 0
            THEN eu.cache_write_tokens
        ELSE NULL
    END,
    NULL,
    NULL,
    NULL,
    CASE
        WHEN eu.cost_usd IS NOT NULL
         AND typeof(eu.cost_usd) IN ('integer', 'real')
         AND eu.cost_usd >= 0
         AND eu.cost_usd <= 1.7976931348623157e308
         AND eu.cost_usd = eu.cost_usd
            THEN eu.cost_usd
        ELSE NULL
    END,
    NULL,
    CASE
        WHEN eu.cost_usd IS NOT NULL
         AND typeof(eu.cost_usd) IN ('integer', 'real')
         AND eu.cost_usd >= 0
         AND eu.cost_usd <= 1.7976931348623157e308
         AND eu.cost_usd = eu.cost_usd
            THEN 'provider_reported'
        ELSE 'none'
    END,
    NULL,
    NULL,
    NULL,
    0,
    CASE
        WHEN eu.cost_usd IS NOT NULL
         AND typeof(eu.cost_usd) IN ('integer', 'real')
         AND eu.cost_usd >= 0
         AND eu.cost_usd <= 1.7976931348623157e308
         AND eu.cost_usd = eu.cost_usd
            THEN NULL
        WHEN typeof(eu.input_tokens) != 'integer' OR eu.input_tokens < 0
          OR typeof(eu.output_tokens) != 'integer' OR eu.output_tokens < 0
          OR typeof(eu.cache_read_tokens) != 'integer' OR eu.cache_read_tokens < 0
          OR typeof(eu.cache_write_tokens) != 'integer' OR eu.cache_write_tokens < 0
          OR typeof(eu.provider) != 'text'
          OR typeof(eu.model) != 'text'
          OR length(trim(CAST(eu.created_at AS TEXT))) = 0
          OR (
              eu.cost_usd IS NOT NULL
              AND (typeof(eu.cost_usd) NOT IN ('integer', 'real')
                   OR eu.cost_usd < 0
                   OR eu.cost_usd > 1.7976931348623157e308
                   OR eu.cost_usd != eu.cost_usd)
          )
            THEN 'invalid_legacy_usage'
        WHEN typeof(eu.provider) != 'text'
          OR length(trim(eu.provider)) = 0
            THEN 'missing_provider'
        WHEN typeof(eu.model) != 'text'
          OR length(trim(eu.model)) = 0
            THEN 'missing_model'
        ELSE 'missing_rate'
    END,
    CAST(eu.created_at AS TEXT),
    CAST(eu.created_at AS TEXT)
FROM execution_usage eu
JOIN execution e ON e.id = eu.execution_id
JOIN task t ON t.id = e.task_id
JOIN project p ON p.id = t.project_id;

-- A migration validation assertion is implemented with a temporary CHECK
-- table because SQLite's RAISE() function is legal only inside a trigger.
-- It verifies one-to-one mapping, compares raw source representations including
-- NULLs, and checks that every reconstructed typed counter/cost matches the
-- source semantics without changing the retained V022 table.
CREATE TEMP TABLE v135_legacy_validation (
    ok INTEGER NOT NULL CHECK (typeof(ok) = 'integer' AND ok = 1)
);

INSERT INTO v135_legacy_validation (ok)
SELECT CASE WHEN
    (SELECT COUNT(*) FROM execution_usage)
        = (SELECT COUNT(*) FROM pricing_selection
           WHERE provenance_kind = 'legacy_execution_aggregate')
    AND (SELECT COUNT(*) FROM execution_usage)
        = (SELECT COUNT(*) FROM usage_invocation
           WHERE provenance_kind = 'legacy_execution_aggregate')
    AND (SELECT COUNT(*) FROM execution_usage)
        = (SELECT COUNT(*) FROM usage_event
           WHERE provenance_kind = 'legacy_execution_aggregate'
             AND report_mode = 'legacy_aggregate')
    AND NOT EXISTS (
        SELECT 1
        FROM execution_usage eu
        LEFT JOIN usage_event ue ON ue.id = CAST(eu.id AS TEXT)
        WHERE ue.id IS NULL
           OR ue.invocation_id != 'legacy-execution-invocation:' || CAST(eu.id AS TEXT)
           OR ue.legacy_source_table != 'execution_usage'
           OR ue.legacy_source_id IS NOT CAST(eu.id AS TEXT)
           OR ue.source_report_id IS NOT CAST(eu.id AS TEXT)
           OR ue.execution_id IS NOT CAST(eu.execution_id AS TEXT)
           OR ue.occurred_at IS NOT CAST(eu.created_at AS TEXT)
           OR ue.legacy_cost_usd_raw IS NOT quote(eu.cost_usd)
           OR ue.legacy_created_at_raw IS NOT quote(eu.created_at)
           OR ue.legacy_project_owner_raw IS NOT quote(
                (SELECT p.owner_id
                 FROM project p
                 JOIN task t ON t.project_id = p.id
                 JOIN execution e2 ON e2.task_id = t.id
                 WHERE e2.id = eu.execution_id)
           )
           OR ue.legacy_provider_sqlite_type IS NOT typeof(eu.provider)
           OR ue.legacy_provider_sql_literal IS NOT quote(eu.provider)
           OR ue.legacy_model_sqlite_type IS NOT typeof(eu.model)
           OR ue.legacy_model_sql_literal IS NOT quote(eu.model)
           OR ue.legacy_counter_values_json IS NOT json_object(
                'input_tokens', json_object(
                    'sqlite_type', typeof(eu.input_tokens),
                    'sql_literal', quote(eu.input_tokens)
                ),
                'output_tokens', json_object(
                    'sqlite_type', typeof(eu.output_tokens),
                    'sql_literal', quote(eu.output_tokens)
                ),
                'cache_read_tokens', json_object(
                    'sqlite_type', typeof(eu.cache_read_tokens),
                    'sql_literal', quote(eu.cache_read_tokens)
                ),
                'cache_write_tokens', json_object(
                    'sqlite_type', typeof(eu.cache_write_tokens),
                    'sql_literal', quote(eu.cache_write_tokens)
                )
           )
           OR (
               typeof(eu.provider) = 'text'
               AND ue.legacy_provider_raw IS NOT eu.provider
           )
           OR (
               typeof(eu.provider) != 'text'
               AND ue.legacy_provider_raw IS NOT NULL
           )
           OR (
               typeof(eu.model) = 'text'
               AND ue.legacy_model_raw IS NOT eu.model
           )
           OR (
               typeof(eu.model) != 'text'
               AND ue.legacy_model_raw IS NOT NULL
           )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM execution_usage eu
        LEFT JOIN pricing_selection ps
          ON ps.id = 'legacy-execution-selection:' || CAST(eu.id AS TEXT)
        LEFT JOIN usage_invocation ui
          ON ui.id = 'legacy-execution-invocation:' || CAST(eu.id AS TEXT)
        WHERE ps.id IS NULL
           OR ui.id IS NULL
           OR ps.invocation_id IS NOT ui.id
           OR ui.pricing_selection_id IS NOT ps.id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM execution_usage eu
        LEFT JOIN usage_event ue ON ue.id = CAST(eu.id AS TEXT)
        WHERE ue.id IS NULL
           OR (
               typeof(eu.input_tokens) = 'integer' AND eu.input_tokens >= 0
               AND typeof(eu.output_tokens) = 'integer' AND eu.output_tokens >= 0
               AND typeof(eu.cache_read_tokens) = 'integer'
                   AND eu.cache_read_tokens >= 0
               AND typeof(eu.cache_write_tokens) = 'integer'
                   AND eu.cache_write_tokens >= 0
               AND (
                   ue.input_tokens IS NOT eu.input_tokens
                   OR ue.output_tokens IS NOT eu.output_tokens
                   OR ue.cache_read_tokens IS NOT eu.cache_read_tokens
                   OR ue.cache_write_tokens IS NOT eu.cache_write_tokens
               )
           )
           OR (
               (
                   typeof(eu.input_tokens) != 'integer' OR eu.input_tokens < 0
                   OR typeof(eu.output_tokens) != 'integer' OR eu.output_tokens < 0
                   OR typeof(eu.cache_read_tokens) != 'integer'
                       OR eu.cache_read_tokens < 0
                   OR typeof(eu.cache_write_tokens) != 'integer'
                       OR eu.cache_write_tokens < 0
               )
               AND (
                   ue.input_tokens IS NOT NULL
                   OR ue.output_tokens IS NOT NULL
                   OR ue.cache_read_tokens IS NOT NULL
                   OR ue.cache_write_tokens IS NOT NULL
               )
           )
           OR (
               eu.cost_usd IS NOT NULL
               AND typeof(eu.cost_usd) IN ('integer', 'real')
               AND eu.cost_usd >= 0
               AND eu.cost_usd <= 1.7976931348623157e308
               AND eu.cost_usd = eu.cost_usd
               AND ue.legacy_reported_cost_usd IS NOT eu.cost_usd
           )
           OR (
               (
                   eu.cost_usd IS NULL
                   OR typeof(eu.cost_usd) NOT IN ('integer', 'real')
                   OR eu.cost_usd < 0
                   OR eu.cost_usd > 1.7976931348623157e308
                   OR eu.cost_usd != eu.cost_usd
               )
               AND ue.legacy_reported_cost_usd IS NOT NULL
           )
    )
THEN 1 ELSE 0 END;

DROP TABLE v135_legacy_validation;
-- The typed copy and validation above complete the additive-authority cutover.
-- This drop is transactional with the rest of V135: a failed migration rolls
-- it back together with the copied ledger rows.
DROP TABLE execution_usage;

-- V071/V129's canonical Agent Chat ledger is the only historical chat source.
-- Room and Conversation tables are deliberately not joined here: V071 already
-- copied those rows into agent_chat_message, and consulting both authorities
-- would double-count one response.  The first temporary relation freezes the
-- only durable Genesis classification proofs available to this migration:
-- the response's immutable turn-job operating-skill revision, or membership
-- of the response/its triggering message in a Genesis source-message packet.
CREATE TEMP TABLE v135_legacy_chat_classification AS
SELECT
    m.id AS source_id,
    m.chat_id,
    m.source_id AS message_source_id,
    m.source_message_id,
    c.kind AS chat_kind,
    c.account_id AS chat_account_id,
    c.project_id AS chat_project_id,
    m.model,
    m.profile_id,
    m.author_id,
    m.created_at AS occurred_at,
    m.token_usage_json,
    CASE
        WHEN c.kind = 'project' THEN 'project_chat'
        WHEN c.kind = 'account_main'
         AND EXISTS (
             SELECT 1
             FROM product_genesis_session g
             WHERE g.main_chat_id = c.id
               AND (
                   EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job tj
                       JOIN operating_skill_revision sr
                         ON sr.id = tj.operating_skill_revision_id
                       JOIN operating_skill os
                         ON os.id = sr.operating_skill_id
                        AND os.skill_key = sr.skill_key
                       WHERE tj.chat_id = c.id
                         AND (tj.response_message_id = m.id OR m.source_id = tj.id)
                         AND sr.skill_key = 'forge.main.project-discovery/v2'
                         AND os.skill_key = 'forge.main.project-discovery/v2'
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(g.source_message_ids_json) source_message
                       WHERE source_message.value = m.id
                          OR source_message.value = m.source_message_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job tj
                       JOIN json_each(g.source_message_ids_json) source_message
                         ON source_message.value = tj.triggering_message_id
                       WHERE tj.chat_id = c.id
                         AND (tj.response_message_id = m.id OR m.source_id = tj.id)
                   )
               )
         )
            THEN 'genesis_chat'
        ELSE 'main_chat'
    END AS surface,
    CASE
        WHEN c.kind = 'account_main'
         AND EXISTS (
             SELECT 1
             FROM product_genesis_session g
             WHERE g.main_chat_id = c.id
               AND (
                   EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job tj
                       JOIN operating_skill_revision sr
                         ON sr.id = tj.operating_skill_revision_id
                       JOIN operating_skill os
                         ON os.id = sr.operating_skill_id
                        AND os.skill_key = sr.skill_key
                       WHERE tj.chat_id = c.id
                         AND (tj.response_message_id = m.id OR m.source_id = tj.id)
                         AND sr.skill_key = 'forge.main.project-discovery/v2'
                         AND os.skill_key = 'forge.main.project-discovery/v2'
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(g.source_message_ids_json) source_message
                       WHERE source_message.value = m.id
                          OR source_message.value = m.source_message_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job tj
                       JOIN json_each(g.source_message_ids_json) source_message
                         ON source_message.value = tj.triggering_message_id
                       WHERE tj.chat_id = c.id
                         AND (tj.response_message_id = m.id OR m.source_id = tj.id)
                   )
               )
         )
            THEN (
                SELECT g.id
                FROM product_genesis_session g
                WHERE g.main_chat_id = c.id
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM agent_chat_turn_job tj
                          JOIN operating_skill_revision sr
                            ON sr.id = tj.operating_skill_revision_id
                          JOIN operating_skill os
                            ON os.id = sr.operating_skill_id
                           AND os.skill_key = sr.skill_key
                          WHERE tj.chat_id = c.id
                            AND (tj.response_message_id = m.id OR m.source_id = tj.id)
                            AND sr.skill_key = 'forge.main.project-discovery/v2'
                            AND os.skill_key = 'forge.main.project-discovery/v2'
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM json_each(g.source_message_ids_json) source_message
                          WHERE source_message.value = m.id
                             OR source_message.value = m.source_message_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM agent_chat_turn_job tj
                          JOIN json_each(g.source_message_ids_json) source_message
                            ON source_message.value = tj.triggering_message_id
                          WHERE tj.chat_id = c.id
                            AND (tj.response_message_id = m.id OR m.source_id = tj.id)
                      )
                  )
                ORDER BY g.created_at ASC, g.id ASC
                LIMIT 1
            )
        ELSE NULL
    END AS genesis_session_id
FROM agent_chat_message m
JOIN agent_chat c ON c.id = m.chat_id
WHERE m.author_type = 'agent'
  AND m.status IN ('complete', 'failed', 'cancelled')
  AND m.source_type != 'handoff';

-- Project attribution for a Genesis response is intentionally stricter than
-- Genesis classification.  It needs the exact immutable handoff, its
-- Project admission receipt/digest, a delivered delivery receipt with the
-- preallocated target IDs, and the response's occurrence before that receipt.
-- A classified Genesis response without all of that proof remains account
-- scoped (genesis_chat with a NULL Project ID).
CREATE TEMP TABLE v135_legacy_chat_source AS
SELECT
    classified.*,
    CASE
        WHEN classified.chat_kind = 'project' THEN classified.chat_project_id
        WHEN classified.surface = 'genesis_chat'
         AND classified.genesis_session_id IS NOT NULL
         AND EXISTS (
             SELECT 1
             FROM product_genesis_session g
             JOIN project p ON p.id = g.project_id
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = classified.chat_id
              AND h.status = 'delivered'
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE g.id = classified.genesis_session_id
               AND g.lifecycle = 'handed_off'
               AND g.project_id IS NOT NULL
               AND g.handoff_id IS NOT NULL
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
               AND classified.occurred_at <= delivery.created_at
               AND (
                   (
                       h.source_turn_job_id IS NOT NULL
                       AND classified.message_source_id IS h.source_turn_job_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job source_turn
                       WHERE source_turn.id = h.source_turn_job_id
                         AND source_turn.chat_id = classified.chat_id
                         AND source_turn.response_message_id = classified.source_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(g.source_message_ids_json) source_message
                       WHERE source_message.value = classified.source_id
                          OR source_message.value = classified.source_message_id
                   )
               )
         )
            THEN (
                SELECT g.project_id
                FROM product_genesis_session g
                JOIN project p ON p.id = g.project_id
                JOIN project_admission_receipt receipt
                  ON receipt.project_id = g.project_id
                 AND receipt.source_kind = 'genesis_handoff'
                 AND receipt.handoff_id = g.handoff_id
                JOIN agent_handoff h
                  ON h.id = g.handoff_id
                 AND h.source_chat_id = classified.chat_id
                 AND h.status = 'delivered'
                JOIN agent_chat target_chat
                  ON target_chat.id = h.target_chat_id
                 AND target_chat.kind = 'project'
                 AND target_chat.project_id = g.project_id
                JOIN agent_handoff_delivery delivery
                  ON delivery.handoff_id = h.id
                 AND delivery.delivery_sequence = 1
                 AND delivery.status = 'delivered'
                 AND delivery.target_message_id IS h.target_message_id
                 AND delivery.target_turn_job_id IS h.target_turn_job_id
                WHERE g.id = classified.genesis_session_id
                  AND g.lifecycle = 'handed_off'
                  AND g.project_id IS NOT NULL
                  AND g.handoff_id IS NOT NULL
                  AND json_valid(h.source_revisions_json)
                  AND json_extract(
                          h.source_revisions_json,
                          '$.request.source_revisions_digest'
                      ) = receipt.payload_digest
                  AND classified.occurred_at <= delivery.created_at
                  AND (
                      (
                          h.source_turn_job_id IS NOT NULL
                          AND classified.message_source_id IS h.source_turn_job_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM agent_chat_turn_job source_turn
                          WHERE source_turn.id = h.source_turn_job_id
                            AND source_turn.chat_id = classified.chat_id
                            AND source_turn.response_message_id = classified.source_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM json_each(g.source_message_ids_json) source_message
                          WHERE source_message.value = classified.source_id
                             OR source_message.value = classified.source_message_id
                      )
                  )
                LIMIT 1
            )
        ELSE NULL
    END AS project_id,
    CASE
        WHEN classified.chat_kind = 'project' THEN classified.chat_id
        ELSE classified.chat_id
    END AS source_chat_id
FROM v135_legacy_chat_classification classified;

-- Resolve owner and immutable Profile labels only from the row's own frozen
-- Project/Chat scope and profile ID.  agent_current, selected bindings, and
-- mutable identity names are deliberately absent from this import.
CREATE TEMP TABLE v135_legacy_chat_ledger_source AS
SELECT
    source.*,
    CASE
        WHEN source.project_id IS NOT NULL
            THEN CASE
                WHEN EXISTS (
                    SELECT 1 FROM project p
                    JOIN user u ON u.id = p.owner_id
                    WHERE p.id = source.project_id
                ) THEN (SELECT p.owner_id FROM project p WHERE p.id = source.project_id)
                ELSE NULL
            END
        WHEN EXISTS (
            SELECT 1 FROM user u WHERE u.id = source.chat_account_id
        ) THEN source.chat_account_id
        ELSE NULL
    END AS owner_user_id,
    CASE
        WHEN source.project_id IS NOT NULL
            THEN quote((SELECT p.owner_id FROM project p WHERE p.id = source.project_id))
        ELSE quote(source.chat_account_id)
    END AS legacy_project_owner_raw,
    CASE
        WHEN typeof(source.model) = 'text' AND length(trim(source.model)) > 0
            THEN source.model
        ELSE NULL
    END AS admitted_model_id,
    CASE
        WHEN typeof(source.model) = 'text' AND length(trim(source.model)) > 0
            THEN source.model
        ELSE NULL
    END AS admitted_runtime_model,
    CASE
        WHEN typeof(source.model) = 'text'
         AND length(trim(source.model)) > 0
         AND EXISTS (
             SELECT 1
             FROM agent_profile profile
             WHERE profile.id = source.profile_id
               AND profile.identity_id IS source.author_id
               AND profile.model IS source.model
               AND typeof(profile.provider) = 'text'
               AND length(trim(profile.provider)) > 0
         )
            THEN (
                SELECT profile.provider
                FROM agent_profile profile
                WHERE profile.id = source.profile_id
                  AND profile.identity_id IS source.author_id
                  AND profile.model IS source.model
                  AND typeof(profile.provider) = 'text'
                  AND length(trim(profile.provider)) > 0
                LIMIT 1
            )
        ELSE NULL
    END AS admitted_provider_id,
    CASE
        WHEN typeof(source.model) = 'text'
         AND length(trim(source.model)) > 0
         AND EXISTS (
             SELECT 1
             FROM agent_profile profile
             WHERE profile.id = source.profile_id
               AND profile.identity_id IS source.author_id
               AND profile.model IS source.model
         )
            THEN (
                SELECT profile.executor_type
                FROM agent_profile profile
                WHERE profile.id = source.profile_id
                  AND profile.identity_id IS source.author_id
                  AND profile.model IS source.model
                LIMIT 1
            )
        ELSE NULL
    END AS executor_type,
    CASE
        WHEN typeof(source.model) = 'text'
         AND length(trim(source.model)) > 0
         AND EXISTS (
             SELECT 1
             FROM agent_profile profile
             WHERE profile.id = source.profile_id
               AND profile.identity_id IS source.author_id
               AND profile.model IS source.model
         )
            THEN (
                SELECT profile.backend_kind
                FROM agent_profile profile
                WHERE profile.id = source.profile_id
                  AND profile.identity_id IS source.author_id
                  AND profile.model IS source.model
                LIMIT 1
            )
        ELSE NULL
    END AS backend_kind,
    CASE
        WHEN source.token_usage_json IS NOT NULL
         AND json_valid(source.token_usage_json)
         AND json_type(source.token_usage_json, '$.input_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.input_tokens') >= 0
         AND json_type(source.token_usage_json, '$.output_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.output_tokens') >= 0
         AND json_type(source.token_usage_json, '$.cache_read_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.cache_read_tokens') >= 0
         AND json_type(source.token_usage_json, '$.cache_write_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.cache_write_tokens') >= 0
            THEN 1
        ELSE 0
    END AS telemetry_valid,
    CASE
        WHEN source.token_usage_json IS NOT NULL
         AND json_valid(source.token_usage_json)
         AND json_type(source.token_usage_json, '$.input_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.input_tokens') >= 0
         AND json_type(source.token_usage_json, '$.output_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.output_tokens') >= 0
         AND json_type(source.token_usage_json, '$.cache_read_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.cache_read_tokens') >= 0
         AND json_type(source.token_usage_json, '$.cache_write_tokens') = 'integer'
         AND json_extract(source.token_usage_json, '$.cache_write_tokens') >= 0
            THEN 'metered'
        ELSE 'unmetered'
    END AS telemetry_state,
    CASE
        WHEN source.token_usage_json IS NULL THEN '{}'
        WHEN NOT json_valid(source.token_usage_json) THEN json_object(
            'source_table', 'agent_chat_message',
            'json_valid', 0,
            'input_tokens', json_object('present', 0, 'type', 'unparseable', 'valid', 0),
            'output_tokens', json_object('present', 0, 'type', 'unparseable', 'valid', 0),
            'cache_read_tokens', json_object('present', 0, 'type', 'unparseable', 'valid', 0),
            'cache_write_tokens', json_object('present', 0, 'type', 'unparseable', 'valid', 0)
        )
        ELSE json_object(
            'source_table', 'agent_chat_message',
            'json_valid', 1,
            'input_tokens', json_object(
                'present', CASE WHEN json_type(source.token_usage_json, '$.input_tokens') IS NULL THEN 0 ELSE 1 END,
                'type', COALESCE(json_type(source.token_usage_json, '$.input_tokens'), 'missing'),
                'valid', CASE WHEN json_type(source.token_usage_json, '$.input_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.input_tokens') >= 0 THEN 1 ELSE 0 END,
                'value', CASE WHEN json_type(source.token_usage_json, '$.input_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.input_tokens') >= 0
                              THEN json_extract(source.token_usage_json, '$.input_tokens') ELSE NULL END
            ),
            'output_tokens', json_object(
                'present', CASE WHEN json_type(source.token_usage_json, '$.output_tokens') IS NULL THEN 0 ELSE 1 END,
                'type', COALESCE(json_type(source.token_usage_json, '$.output_tokens'), 'missing'),
                'valid', CASE WHEN json_type(source.token_usage_json, '$.output_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.output_tokens') >= 0 THEN 1 ELSE 0 END,
                'value', CASE WHEN json_type(source.token_usage_json, '$.output_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.output_tokens') >= 0
                              THEN json_extract(source.token_usage_json, '$.output_tokens') ELSE NULL END
            ),
            'cache_read_tokens', json_object(
                'present', CASE WHEN json_type(source.token_usage_json, '$.cache_read_tokens') IS NULL THEN 0 ELSE 1 END,
                'type', COALESCE(json_type(source.token_usage_json, '$.cache_read_tokens'), 'missing'),
                'valid', CASE WHEN json_type(source.token_usage_json, '$.cache_read_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.cache_read_tokens') >= 0 THEN 1 ELSE 0 END,
                'value', CASE WHEN json_type(source.token_usage_json, '$.cache_read_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.cache_read_tokens') >= 0
                              THEN json_extract(source.token_usage_json, '$.cache_read_tokens') ELSE NULL END
            ),
            'cache_write_tokens', json_object(
                'present', CASE WHEN json_type(source.token_usage_json, '$.cache_write_tokens') IS NULL THEN 0 ELSE 1 END,
                'type', COALESCE(json_type(source.token_usage_json, '$.cache_write_tokens'), 'missing'),
                'valid', CASE WHEN json_type(source.token_usage_json, '$.cache_write_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.cache_write_tokens') >= 0 THEN 1 ELSE 0 END,
                'value', CASE WHEN json_type(source.token_usage_json, '$.cache_write_tokens') = 'integer'
                                   AND json_extract(source.token_usage_json, '$.cache_write_tokens') >= 0
                              THEN json_extract(source.token_usage_json, '$.cache_write_tokens') ELSE NULL END
            )
        )
    END AS legacy_counter_values_json
FROM v135_legacy_chat_source source;

-- Every canonical agent response is one historical provider-attempt
-- candidate, even when its token field is NULL.  NULL telemetry therefore
-- gets an invocation but deliberately no usage_event; a non-NULL malformed
-- payload gets an invocation and an invalid/unmetered event below.
INSERT INTO pricing_selection (
    id, owner_user_id, project_id, domain_kind, surface, source_id,
    execution_id, task_id, candidate_key, attempt_ordinal, subject_id,
    subject_revision_id, subject_revision_digest, binding_id,
    rate_revision_id, catalog_snapshot_id, catalog_freshness, runtime_model,
    admitted_provider_id, admitted_model_id, source_kind, provenance_kind,
    selection_status, selection_reason, selection_digest, selected_at, created_at
)
SELECT
    'legacy-chat-selection:' || CAST(source.source_id AS TEXT),
    source.owner_user_id,
    source.project_id,
    'chat',
    source.surface,
    CAST(source.source_id AS TEXT),
    NULL,
    NULL,
    'legacy-chat-candidate:' || CAST(source.source_id AS TEXT),
    0,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    source.admitted_runtime_model,
    source.admitted_provider_id,
    source.admitted_model_id,
    NULL,
    'legacy_chat',
    'unpriced',
    NULL,
    'legacy-chat-selection-digest:' || CAST(source.source_id AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT)
FROM v135_legacy_chat_ledger_source source
WHERE NOT EXISTS (
    SELECT 1 FROM pricing_selection existing
    WHERE existing.id = 'legacy-chat-selection:' || CAST(source.source_id AS TEXT)
);

INSERT INTO usage_invocation (
    id, owner_user_id, project_id, domain_kind, surface, source_id,
    execution_id, task_id, domain_idempotency_key, candidate_key,
    attempt_ordinal, pricing_selection_id, admitted_provider_id,
    admitted_model_id, admitted_runtime_model, pricing_subject_id,
    pricing_subject_revision_id, subject_revision_digest, agent_id, profile_id,
    agent_name_snapshot, project_name_snapshot, executor_type, backend_kind,
    provenance_kind, lifecycle, telemetry_state, terminal_reason, version,
    admitted_at, started_at, settled_at, created_at, updated_at
)
SELECT
    'legacy-chat-invocation:' || CAST(source.source_id AS TEXT),
    source.owner_user_id,
    source.project_id,
    'chat',
    source.surface,
    CAST(source.source_id AS TEXT),
    NULL,
    NULL,
    'legacy-chat-invocation-key:' || CAST(source.source_id AS TEXT),
    'legacy-chat-candidate:' || CAST(source.source_id AS TEXT),
    0,
    'legacy-chat-selection:' || CAST(source.source_id AS TEXT),
    source.admitted_provider_id,
    source.admitted_model_id,
    source.admitted_runtime_model,
    NULL,
    NULL,
    NULL,
    source.author_id,
    source.profile_id,
    NULL,
    NULL,
    source.executor_type,
    source.backend_kind,
    'legacy_chat',
    'settled',
    CASE
        WHEN source.token_usage_json IS NOT NULL AND source.telemetry_valid = 1
            THEN 'metered'
        ELSE 'unmetered'
    END,
    CASE
        WHEN source.token_usage_json IS NULL
            THEN 'legacy chat response has no token telemetry'
        WHEN source.telemetry_valid = 0
            THEN 'legacy chat row retained with invalid telemetry'
        ELSE NULL
    END,
    1,
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT)
FROM v135_legacy_chat_ledger_source source
WHERE NOT EXISTS (
    SELECT 1 FROM usage_invocation existing
    WHERE existing.id = 'legacy-chat-invocation:' || CAST(source.source_id AS TEXT)
);

UPDATE pricing_selection
SET invocation_id = 'legacy-chat-invocation:' || CAST(
    (SELECT source.source_id
     FROM v135_legacy_chat_ledger_source source
     WHERE pricing_selection.id =
           'legacy-chat-selection:' || CAST(source.source_id AS TEXT)) AS TEXT
)
WHERE provenance_kind = 'legacy_chat'
  AND invocation_id IS NULL;

INSERT INTO usage_event (
    id, invocation_id, owner_user_id, project_id, surface, source_id,
    execution_id, task_id, event_idempotency_key, source_report_id,
    report_sequence, report_mode, provenance_kind, legacy_source_table,
    legacy_source_id, legacy_provider_raw, legacy_provider_sqlite_type,
    legacy_provider_sql_literal, legacy_model_raw, legacy_model_sqlite_type,
    legacy_model_sql_literal, legacy_counter_values_json, legacy_cost_usd_raw,
    legacy_created_at_raw, legacy_project_owner_raw, legacy_invalid_usage,
    provider_id, model_id, runtime_model, candidate_key, attempt_ordinal,
    agent_id, profile_id, agent_name_snapshot, project_name_snapshot,
    executor_type, pricing_subject_revision_id, subject_revision_digest,
    telemetry_state, input_tokens, output_tokens, cache_read_tokens,
    cache_write_tokens, context_tokens, selected_tier,
    provider_reported_nano_usd, legacy_reported_cost_usd, estimated_nano_usd,
    cost_kind, rate_revision_id, catalog_snapshot_id, formula_revision,
    retrospective, coverage_reason_code, occurred_at, created_at
)
SELECT
    'legacy-chat-event:' || CAST(source.source_id AS TEXT),
    'legacy-chat-invocation:' || CAST(source.source_id AS TEXT),
    source.owner_user_id,
    source.project_id,
    source.surface,
    CAST(source.source_id AS TEXT),
    NULL,
    NULL,
    'legacy-chat-event-key:' || CAST(source.source_id AS TEXT),
    'legacy-chat-report:' || CAST(source.source_id AS TEXT),
    0,
    'legacy_aggregate',
    'legacy_chat',
    'agent_chat_message',
    CAST(source.source_id AS TEXT),
    NULL,
    'absent',
    'NULL',
    CASE WHEN typeof(source.model) = 'text' THEN source.model ELSE NULL END,
    typeof(source.model),
    quote(source.model),
    source.legacy_counter_values_json,
    'NULL',
    quote(source.occurred_at),
    source.legacy_project_owner_raw,
    CASE
        WHEN source.token_usage_json IS NOT NULL
         AND source.telemetry_valid = 0 THEN 1
        ELSE 0
    END,
    source.admitted_provider_id,
    source.admitted_model_id,
    source.admitted_runtime_model,
    'legacy-chat-candidate:' || CAST(source.source_id AS TEXT),
    0,
    source.author_id,
    source.profile_id,
    NULL,
    NULL,
    source.executor_type,
    NULL,
    NULL,
    source.telemetry_state,
    CASE WHEN source.telemetry_valid = 1
         THEN json_extract(source.token_usage_json, '$.input_tokens') ELSE NULL END,
    CASE WHEN source.telemetry_valid = 1
         THEN json_extract(source.token_usage_json, '$.output_tokens') ELSE NULL END,
    CASE WHEN source.telemetry_valid = 1
         THEN json_extract(source.token_usage_json, '$.cache_read_tokens') ELSE NULL END,
    CASE WHEN source.telemetry_valid = 1
         THEN json_extract(source.token_usage_json, '$.cache_write_tokens') ELSE NULL END,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    'none',
    NULL,
    NULL,
    NULL,
    0,
    CASE
        WHEN source.telemetry_valid = 0 THEN 'invalid_legacy_usage'
        WHEN source.admitted_provider_id IS NULL THEN 'missing_provider'
        WHEN source.admitted_model_id IS NULL THEN 'missing_model'
        ELSE 'missing_rate'
    END,
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT)
FROM v135_legacy_chat_ledger_source source
WHERE source.token_usage_json IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM usage_event existing
      WHERE existing.id = 'legacy-chat-event:' || CAST(source.source_id AS TEXT)
  );

-- V130 inquiries have no provider/model columns of their own.  A matching
-- turn-job/profile pair is immutable source evidence when present; otherwise
-- those attribution fields remain NULL while the inquiry identity and raw
-- profile ID (if any) remain visible.  Only rows with a terminal status and a
-- non-empty finished_at are imported.  A running/unfinished row is never
-- converted into a fabricated settled attempt.
CREATE TEMP TABLE v135_legacy_inquiry_source AS
SELECT
    inquiry.id AS source_id,
    inquiry.chat_id,
    inquiry.owner_user_id AS source_owner_user_id,
    inquiry.identity_id AS agent_id,
    CAST(inquiry.finished_at AS TEXT) AS occurred_at,
    turn.profile_id,
    CASE
        WHEN typeof(profile.model) = 'text' AND length(trim(profile.model)) > 0
            THEN profile.model
        ELSE NULL
    END AS admitted_model_id,
    CASE
        WHEN typeof(profile.model) = 'text' AND length(trim(profile.model)) > 0
            THEN profile.model
        ELSE NULL
    END AS admitted_runtime_model,
    CASE
        WHEN typeof(profile.model) = 'text'
         AND length(trim(profile.model)) > 0
         AND typeof(profile.provider) = 'text'
         AND length(trim(profile.provider)) > 0
            THEN profile.provider
        ELSE NULL
    END AS admitted_provider_id,
    CASE
        WHEN typeof(profile.model) = 'text'
         AND length(trim(profile.model)) > 0
            THEN profile.executor_type
        ELSE NULL
    END AS executor_type,
    CASE
        WHEN typeof(profile.model) = 'text'
         AND length(trim(profile.model)) > 0
            THEN profile.backend_kind
        ELSE NULL
    END AS backend_kind,
    CASE
        WHEN typeof(inquiry.input_tokens) = 'integer' AND inquiry.input_tokens >= 0
         AND typeof(inquiry.output_tokens) = 'integer' AND inquiry.output_tokens >= 0
         AND typeof(inquiry.cache_read_tokens) = 'integer'
             AND inquiry.cache_read_tokens >= 0
         AND typeof(inquiry.cache_write_tokens) = 'integer'
             AND inquiry.cache_write_tokens >= 0
            THEN 1
        ELSE 0
    END AS telemetry_valid,
    CASE
        WHEN typeof(inquiry.input_tokens) = 'integer' AND inquiry.input_tokens >= 0
         AND typeof(inquiry.output_tokens) = 'integer' AND inquiry.output_tokens >= 0
         AND typeof(inquiry.cache_read_tokens) = 'integer'
             AND inquiry.cache_read_tokens >= 0
         AND typeof(inquiry.cache_write_tokens) = 'integer'
             AND inquiry.cache_write_tokens >= 0
         AND (
             inquiry.input_tokens > 0
             OR inquiry.output_tokens > 0
             OR inquiry.cache_read_tokens > 0
             OR inquiry.cache_write_tokens > 0
         )
            THEN 1
        ELSE 0
    END AS positive_usage,
    CASE
        WHEN typeof(inquiry.input_tokens) = 'integer' AND inquiry.input_tokens >= 0
         AND typeof(inquiry.output_tokens) = 'integer' AND inquiry.output_tokens >= 0
         AND typeof(inquiry.cache_read_tokens) = 'integer'
             AND inquiry.cache_read_tokens >= 0
         AND typeof(inquiry.cache_write_tokens) = 'integer'
             AND inquiry.cache_write_tokens >= 0
         AND (
             inquiry.input_tokens > 0
             OR inquiry.output_tokens > 0
             OR inquiry.cache_read_tokens > 0
             OR inquiry.cache_write_tokens > 0
         )
            THEN 'metered'
        ELSE 'unmetered'
    END AS telemetry_state,
    CASE
        WHEN typeof(inquiry.input_tokens) = 'integer' AND inquiry.input_tokens >= 0
            THEN 1 ELSE 0 END AS input_valid,
    CASE
        WHEN typeof(inquiry.output_tokens) = 'integer' AND inquiry.output_tokens >= 0
            THEN 1 ELSE 0 END AS output_valid,
    CASE
        WHEN typeof(inquiry.cache_read_tokens) = 'integer'
         AND inquiry.cache_read_tokens >= 0
            THEN 1 ELSE 0 END AS cache_read_valid,
    CASE
        WHEN typeof(inquiry.cache_write_tokens) = 'integer'
         AND inquiry.cache_write_tokens >= 0
            THEN 1 ELSE 0 END AS cache_write_valid,
    inquiry.input_tokens AS input_tokens,
    inquiry.output_tokens AS output_tokens,
    inquiry.cache_read_tokens AS cache_read_tokens,
    inquiry.cache_write_tokens AS cache_write_tokens,
    CASE
        WHEN typeof(inquiry.input_tokens) = 'integer' AND inquiry.input_tokens >= 0
         AND typeof(inquiry.output_tokens) = 'integer' AND inquiry.output_tokens >= 0
         AND typeof(inquiry.cache_read_tokens) = 'integer'
             AND inquiry.cache_read_tokens >= 0
         AND typeof(inquiry.cache_write_tokens) = 'integer'
             AND inquiry.cache_write_tokens >= 0
            THEN json_object(
                'source_table', 'agent_inquiry',
                'reported_marker', 0,
                'input_tokens', json_object(
                    'sqlite_type', typeof(inquiry.input_tokens),
                    'valid', 1, 'value', inquiry.input_tokens
                ),
                'output_tokens', json_object(
                    'sqlite_type', typeof(inquiry.output_tokens),
                    'valid', 1, 'value', inquiry.output_tokens
                ),
                'cache_read_tokens', json_object(
                    'sqlite_type', typeof(inquiry.cache_read_tokens),
                    'valid', 1, 'value', inquiry.cache_read_tokens
                ),
                'cache_write_tokens', json_object(
                    'sqlite_type', typeof(inquiry.cache_write_tokens),
                    'valid', 1, 'value', inquiry.cache_write_tokens
                )
            )
        ELSE json_object(
            'source_table', 'agent_inquiry',
            'reported_marker', 0,
            'input_tokens', json_object(
                'sqlite_type', typeof(inquiry.input_tokens),
                'valid', CASE
                    WHEN typeof(inquiry.input_tokens) = 'integer'
                     AND inquiry.input_tokens >= 0 THEN 1 ELSE 0 END
            ),
            'output_tokens', json_object(
                'sqlite_type', typeof(inquiry.output_tokens),
                'valid', CASE
                    WHEN typeof(inquiry.output_tokens) = 'integer'
                     AND inquiry.output_tokens >= 0 THEN 1 ELSE 0 END
            ),
            'cache_read_tokens', json_object(
                'sqlite_type', typeof(inquiry.cache_read_tokens),
                'valid', CASE
                    WHEN typeof(inquiry.cache_read_tokens) = 'integer'
                     AND inquiry.cache_read_tokens >= 0 THEN 1 ELSE 0 END
            ),
            'cache_write_tokens', json_object(
                'sqlite_type', typeof(inquiry.cache_write_tokens),
                'valid', CASE
                    WHEN typeof(inquiry.cache_write_tokens) = 'integer'
                     AND inquiry.cache_write_tokens >= 0 THEN 1 ELSE 0 END
            )
        )
    END AS legacy_counter_values_json
FROM agent_inquiry inquiry
JOIN agent_chat chat ON chat.id = inquiry.chat_id
LEFT JOIN agent_chat_turn_job turn
  ON turn.id = inquiry.turn_job_id
 AND turn.chat_id = inquiry.chat_id
 AND turn.responder_identity_id = inquiry.identity_id
LEFT JOIN agent_profile profile
  ON profile.id = turn.profile_id
 AND profile.identity_id = inquiry.identity_id
WHERE inquiry.status IN ('succeeded', 'failed', 'cancelled')
  AND inquiry.finished_at IS NOT NULL
  AND length(trim(CAST(inquiry.finished_at AS TEXT))) > 0;

INSERT INTO pricing_selection (
    id, owner_user_id, project_id, domain_kind, surface, source_id,
    execution_id, task_id, candidate_key, attempt_ordinal, subject_id,
    subject_revision_id, subject_revision_digest, binding_id,
    rate_revision_id, catalog_snapshot_id, catalog_freshness, runtime_model,
    admitted_provider_id, admitted_model_id, source_kind, provenance_kind,
    selection_status, selection_reason, selection_digest, selected_at, created_at
)
SELECT
    'legacy-inquiry-selection:' || CAST(source.source_id AS TEXT),
    CASE
        WHEN EXISTS (
            SELECT 1 FROM user u WHERE u.id = source.source_owner_user_id
        ) THEN source.source_owner_user_id
        ELSE NULL
    END,
    NULL,
    'inquiry',
    'main_inquiry',
    CAST(source.source_id AS TEXT),
    NULL,
    NULL,
    'legacy-inquiry-candidate:' || CAST(source.source_id AS TEXT),
    0,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    source.admitted_runtime_model,
    source.admitted_provider_id,
    source.admitted_model_id,
    NULL,
    'legacy_inquiry',
    'unpriced',
    NULL,
    'legacy-inquiry-selection-digest:' || CAST(source.source_id AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT)
FROM v135_legacy_inquiry_source source
WHERE NOT EXISTS (
    SELECT 1 FROM pricing_selection existing
    WHERE existing.id = 'legacy-inquiry-selection:' || CAST(source.source_id AS TEXT)
);

INSERT INTO usage_invocation (
    id, owner_user_id, project_id, domain_kind, surface, source_id,
    execution_id, task_id, domain_idempotency_key, candidate_key,
    attempt_ordinal, pricing_selection_id, admitted_provider_id,
    admitted_model_id, admitted_runtime_model, pricing_subject_id,
    pricing_subject_revision_id, subject_revision_digest, agent_id, profile_id,
    agent_name_snapshot, project_name_snapshot, executor_type, backend_kind,
    provenance_kind, lifecycle, telemetry_state, terminal_reason, version,
    admitted_at, started_at, settled_at, created_at, updated_at
)
SELECT
    'legacy-inquiry-invocation:' || CAST(source.source_id AS TEXT),
    CASE
        WHEN EXISTS (
            SELECT 1 FROM user u WHERE u.id = source.source_owner_user_id
        ) THEN source.source_owner_user_id
        ELSE NULL
    END,
    NULL,
    'inquiry',
    'main_inquiry',
    CAST(source.source_id AS TEXT),
    NULL,
    NULL,
    'legacy-inquiry-invocation-key:' || CAST(source.source_id AS TEXT),
    'legacy-inquiry-candidate:' || CAST(source.source_id AS TEXT),
    0,
    'legacy-inquiry-selection:' || CAST(source.source_id AS TEXT),
    source.admitted_provider_id,
    source.admitted_model_id,
    source.admitted_runtime_model,
    NULL,
    NULL,
    NULL,
    source.agent_id,
    source.profile_id,
    NULL,
    NULL,
    source.executor_type,
    source.backend_kind,
    'legacy_inquiry',
    'settled',
    source.telemetry_state,
    CASE
        WHEN source.telemetry_valid = 0
            THEN 'legacy inquiry row retained with invalid telemetry'
        WHEN source.positive_usage = 0
            THEN 'legacy inquiry counters are unknown without a reported marker'
        ELSE NULL
    END,
    1,
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT)
FROM v135_legacy_inquiry_source source
WHERE NOT EXISTS (
    SELECT 1 FROM usage_invocation existing
    WHERE existing.id = 'legacy-inquiry-invocation:' || CAST(source.source_id AS TEXT)
);

UPDATE pricing_selection
SET invocation_id = 'legacy-inquiry-invocation:' || CAST(
    (SELECT source.source_id
     FROM v135_legacy_inquiry_source source
     WHERE pricing_selection.id =
           'legacy-inquiry-selection:' || CAST(source.source_id AS TEXT)) AS TEXT
)
WHERE provenance_kind = 'legacy_inquiry'
  AND invocation_id IS NULL;

-- Positive, complete inquiry counters are metered.  Four valid zero values
-- remain an explicit unmetered/unknown event because V130 has no independent
-- reported-marker column.  Invalid or negative values likewise become a
-- bounded invalid/unmetered event, while the source row remains untouched.
INSERT INTO usage_event (
    id, invocation_id, owner_user_id, project_id, surface, source_id,
    execution_id, task_id, event_idempotency_key, source_report_id,
    report_sequence, report_mode, provenance_kind, legacy_source_table,
    legacy_source_id, legacy_provider_raw, legacy_provider_sqlite_type,
    legacy_provider_sql_literal, legacy_model_raw, legacy_model_sqlite_type,
    legacy_model_sql_literal, legacy_counter_values_json, legacy_cost_usd_raw,
    legacy_created_at_raw, legacy_project_owner_raw, legacy_invalid_usage,
    provider_id, model_id, runtime_model, candidate_key, attempt_ordinal,
    agent_id, profile_id, agent_name_snapshot, project_name_snapshot,
    executor_type, pricing_subject_revision_id, subject_revision_digest,
    telemetry_state, input_tokens, output_tokens, cache_read_tokens,
    cache_write_tokens, context_tokens, selected_tier,
    provider_reported_nano_usd, legacy_reported_cost_usd, estimated_nano_usd,
    cost_kind, rate_revision_id, catalog_snapshot_id, formula_revision,
    retrospective, coverage_reason_code, occurred_at, created_at
)
SELECT
    'legacy-inquiry-event:' || CAST(source.source_id AS TEXT),
    'legacy-inquiry-invocation:' || CAST(source.source_id AS TEXT),
    CASE
        WHEN EXISTS (
            SELECT 1 FROM user u WHERE u.id = source.source_owner_user_id
        ) THEN source.source_owner_user_id
        ELSE NULL
    END,
    NULL,
    'main_inquiry',
    CAST(source.source_id AS TEXT),
    NULL,
    NULL,
    'legacy-inquiry-event-key:' || CAST(source.source_id AS TEXT),
    'legacy-inquiry-report:' || CAST(source.source_id AS TEXT),
    0,
    'legacy_aggregate',
    'legacy_inquiry',
    'agent_inquiry',
    CAST(source.source_id AS TEXT),
    NULL,
    'absent',
    'NULL',
    NULL,
    'absent',
    'NULL',
    source.legacy_counter_values_json,
    'NULL',
    quote(source.occurred_at),
    quote(source.source_owner_user_id),
    CASE WHEN source.telemetry_valid = 0 THEN 1 ELSE 0 END,
    source.admitted_provider_id,
    source.admitted_model_id,
    source.admitted_runtime_model,
    'legacy-inquiry-candidate:' || CAST(source.source_id AS TEXT),
    0,
    source.agent_id,
    source.profile_id,
    NULL,
    NULL,
    source.executor_type,
    NULL,
    NULL,
    source.telemetry_state,
    CASE WHEN source.positive_usage = 1 THEN source.input_tokens ELSE NULL END,
    CASE WHEN source.positive_usage = 1 THEN source.output_tokens ELSE NULL END,
    CASE WHEN source.positive_usage = 1 THEN source.cache_read_tokens ELSE NULL END,
    CASE WHEN source.positive_usage = 1 THEN source.cache_write_tokens ELSE NULL END,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    'none',
    NULL,
    NULL,
    NULL,
    0,
    CASE
        WHEN source.telemetry_valid = 0 THEN 'invalid_legacy_usage'
        WHEN source.positive_usage = 0 THEN 'unmetered'
        WHEN source.admitted_provider_id IS NULL THEN 'missing_provider'
        WHEN source.admitted_model_id IS NULL THEN 'missing_model'
        ELSE 'missing_rate'
    END,
    CAST(source.occurred_at AS TEXT),
    CAST(source.occurred_at AS TEXT)
FROM v135_legacy_inquiry_source source
WHERE NOT EXISTS (
    SELECT 1 FROM usage_event existing
    WHERE existing.id = 'legacy-inquiry-event:' || CAST(source.source_id AS TEXT)
);

-- Migration-local coverage assertions make the import auditable without
-- rewriting either canonical source table.  Chat rows with NULL telemetry are
-- intentionally absent from usage_event; every other imported attempt has one
-- bounded event, and ineligible inquiry rows never create a settled attempt.
CREATE TEMP TABLE v135_legacy_chat_inquiry_validation (
    ok INTEGER NOT NULL CHECK (typeof(ok) = 'integer' AND ok = 1)
);

INSERT INTO v135_legacy_chat_inquiry_validation (ok)
SELECT CASE WHEN
    (SELECT COUNT(*) FROM v135_legacy_chat_ledger_source)
        = (SELECT COUNT(*) FROM pricing_selection
           WHERE provenance_kind = 'legacy_chat')
    AND (SELECT COUNT(*) FROM v135_legacy_chat_ledger_source)
        = (SELECT COUNT(*) FROM usage_invocation
           WHERE provenance_kind = 'legacy_chat')
    AND (SELECT COUNT(*) FROM v135_legacy_inquiry_source)
        = (SELECT COUNT(*) FROM pricing_selection
           WHERE provenance_kind = 'legacy_inquiry')
    AND (SELECT COUNT(*) FROM v135_legacy_inquiry_source)
        = (SELECT COUNT(*) FROM usage_invocation
           WHERE provenance_kind = 'legacy_inquiry')
    AND (SELECT COUNT(*) FROM v135_legacy_chat_ledger_source
         WHERE token_usage_json IS NOT NULL)
        = (SELECT COUNT(*) FROM usage_event
           WHERE provenance_kind = 'legacy_chat')
    AND (SELECT COUNT(*) FROM v135_legacy_inquiry_source)
        = (SELECT COUNT(*) FROM usage_event
           WHERE provenance_kind = 'legacy_inquiry')
    AND NOT EXISTS (
        SELECT 1
        FROM v135_legacy_chat_ledger_source source
        LEFT JOIN pricing_selection selection
          ON selection.id = 'legacy-chat-selection:' || CAST(source.source_id AS TEXT)
        LEFT JOIN usage_invocation invocation
          ON invocation.id = 'legacy-chat-invocation:' || CAST(source.source_id AS TEXT)
        WHERE selection.id IS NULL
           OR invocation.id IS NULL
           OR selection.invocation_id IS NOT invocation.id
           OR invocation.pricing_selection_id IS NOT selection.id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM v135_legacy_inquiry_source source
        LEFT JOIN pricing_selection selection
          ON selection.id = 'legacy-inquiry-selection:' || CAST(source.source_id AS TEXT)
        LEFT JOIN usage_invocation invocation
          ON invocation.id = 'legacy-inquiry-invocation:' || CAST(source.source_id AS TEXT)
        WHERE selection.id IS NULL
           OR invocation.id IS NULL
           OR selection.invocation_id IS NOT invocation.id
           OR invocation.pricing_selection_id IS NOT selection.id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM v135_legacy_chat_ledger_source source
        WHERE source.token_usage_json IS NULL
          AND EXISTS (
              SELECT 1 FROM usage_event event
              WHERE event.id = 'legacy-chat-event:' || CAST(source.source_id AS TEXT)
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM v135_legacy_chat_ledger_source source
        LEFT JOIN usage_event event
          ON event.id = 'legacy-chat-event:' || CAST(source.source_id AS TEXT)
        WHERE source.token_usage_json IS NOT NULL
          AND (
              event.id IS NULL
              OR (
                  source.telemetry_valid = 1
                  AND (
                      event.input_tokens IS NOT
                          CASE WHEN source.telemetry_valid = 1
                               THEN json_extract(source.token_usage_json, '$.input_tokens')
                               ELSE NULL END
                      OR event.output_tokens IS NOT
                          CASE WHEN source.telemetry_valid = 1
                               THEN json_extract(source.token_usage_json, '$.output_tokens')
                               ELSE NULL END
                      OR event.cache_read_tokens IS NOT
                          CASE WHEN source.telemetry_valid = 1
                               THEN json_extract(source.token_usage_json, '$.cache_read_tokens')
                               ELSE NULL END
                      OR event.cache_write_tokens IS NOT
                          CASE WHEN source.telemetry_valid = 1
                               THEN json_extract(source.token_usage_json, '$.cache_write_tokens')
                               ELSE NULL END
                  )
              )
              OR (
                  source.telemetry_valid = 0
                  AND (
                      event.input_tokens IS NOT NULL
                      OR event.output_tokens IS NOT NULL
                      OR event.cache_read_tokens IS NOT NULL
                      OR event.cache_write_tokens IS NOT NULL
                  )
              )
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM v135_legacy_inquiry_source source
        LEFT JOIN usage_event event
          ON event.id = 'legacy-inquiry-event:' || CAST(source.source_id AS TEXT)
        WHERE event.id IS NULL
           OR (
               source.positive_usage = 1
               AND (
                   event.input_tokens IS NOT source.input_tokens
                   OR event.output_tokens IS NOT source.output_tokens
                   OR event.cache_read_tokens IS NOT source.cache_read_tokens
                   OR event.cache_write_tokens IS NOT source.cache_write_tokens
               )
           )
           OR (
               source.positive_usage = 0
               AND (
                   event.input_tokens IS NOT NULL
                   OR event.output_tokens IS NOT NULL
                   OR event.cache_read_tokens IS NOT NULL
                   OR event.cache_write_tokens IS NOT NULL
               )
           )
    )
THEN 1 ELSE 0 END;

DROP TABLE v135_legacy_chat_inquiry_validation;
DROP TABLE v135_legacy_inquiry_source;
DROP TABLE v135_legacy_chat_ledger_source;
DROP TABLE v135_legacy_chat_source;
DROP TABLE v135_legacy_chat_classification;
