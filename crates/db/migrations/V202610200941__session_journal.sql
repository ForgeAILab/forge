-- Additive native-journal storage only. Legacy rows/payloads are never rewritten.
-- Runtime ids are opaque and are NOT unique in the host's agent_session table.
CREATE INDEX idx_agent_session_runtime_session_id ON agent_session(runtime_session_id);

-- Unsigned runtime counters use fixed-width big-endian eight-byte BLOBs, so
-- SQL fence equality preserves the complete u64 range (including control writes).
CREATE TABLE agent_session_journal_session (
    storage_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('protected', 'ordinary')),
    runtime_session_id TEXT NOT NULL,
    forge_session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    generation BLOB NOT NULL CHECK (typeof(generation) = 'blob' AND length(generation) = 8),
    revision BLOB NOT NULL CHECK (typeof(revision) = 'blob' AND length(revision) = 8),
    writer_epoch BLOB NOT NULL CHECK (typeof(writer_epoch) = 'blob' AND length(writer_epoch) = 8),
    collection_dirty INTEGER NOT NULL CHECK (collection_dirty IN (0, 1)),
    published INTEGER NOT NULL CHECK (published IN (0, 1)),
    has_checkpoint INTEGER NOT NULL CHECK (has_checkpoint IN (0, 1)),
    retired INTEGER NOT NULL CHECK (retired IN (0, 1)),
    key_revision INTEGER NOT NULL CHECK (key_revision > 0),
    format INTEGER NOT NULL CHECK (format > 0),
    nonce BLOB NOT NULL CHECK (typeof(nonce) = 'blob' AND length(nonce) = 24),
    payload BLOB NOT NULL CHECK (typeof(payload) = 'blob' AND length(payload) >= 16),
    PRIMARY KEY (storage_id, role, runtime_session_id),
    UNIQUE (storage_id, role, runtime_session_id, forge_session_id),
    CHECK (has_checkpoint = 0 OR (published = 1 AND retired = 0))
) WITHOUT ROWID;
CREATE INDEX idx_session_journal_session_forge ON agent_session_journal_session(forge_session_id);
CREATE INDEX idx_session_journal_dirty
    ON agent_session_journal_session(storage_id, role, runtime_session_id)
    WHERE collection_dirty = 1;

CREATE TABLE agent_session_journal_pin (
    storage_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('protected', 'ordinary')),
    runtime_session_id TEXT NOT NULL,
    forge_session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    pin_key BLOB NOT NULL CHECK (typeof(pin_key) = 'blob' AND length(pin_key) = 32),
    key_revision INTEGER NOT NULL CHECK (key_revision > 0),
    format INTEGER NOT NULL CHECK (format > 0),
    nonce BLOB NOT NULL CHECK (typeof(nonce) = 'blob' AND length(nonce) = 24),
    payload BLOB NOT NULL CHECK (typeof(payload) = 'blob' AND length(payload) >= 16),
    PRIMARY KEY (storage_id, role, runtime_session_id, pin_key),
    FOREIGN KEY (storage_id, role, runtime_session_id, forge_session_id)
        REFERENCES agent_session_journal_session
            (storage_id, role, runtime_session_id, forge_session_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX idx_session_journal_pin_forge ON agent_session_journal_pin(forge_session_id);

CREATE TABLE agent_session_journal_catalog (
    storage_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('protected', 'ordinary')),
    runtime_session_id TEXT NOT NULL,
    forge_session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    head_key BLOB NOT NULL CHECK (typeof(head_key) = 'blob' AND length(head_key) = 32),
    key_revision INTEGER NOT NULL CHECK (key_revision > 0),
    format INTEGER NOT NULL CHECK (format > 0),
    nonce BLOB NOT NULL CHECK (typeof(nonce) = 'blob' AND length(nonce) = 24),
    payload BLOB NOT NULL CHECK (typeof(payload) = 'blob' AND length(payload) >= 16),
    PRIMARY KEY (storage_id, role, runtime_session_id, head_key),
    FOREIGN KEY (storage_id, role, runtime_session_id, forge_session_id)
        REFERENCES agent_session_journal_session
            (storage_id, role, runtime_session_id, forge_session_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX idx_session_journal_catalog_forge ON agent_session_journal_catalog(forge_session_id);

-- Owner labels deliberately have NO foreign key to agent_session or the
-- journal session. Cascades unlink memberships; bounded cleanup drains packs.
-- A pack belongs to one runtime session; each of its domains holds its own
-- membership bitmap, so identical bodies are stored once per session.
CREATE TABLE agent_session_journal_pack (
    pack_id INTEGER PRIMARY KEY AUTOINCREMENT,
    runtime_session_id TEXT NOT NULL,
    forge_session_id TEXT NOT NULL,
    generation BLOB NOT NULL CHECK (typeof(generation) = 'blob' AND length(generation) = 8),
    object_count INTEGER NOT NULL CHECK (object_count > 0),
    retained_canonical_bytes INTEGER NOT NULL CHECK (retained_canonical_bytes > 0 AND retained_canonical_bytes <= 1048576),
    key_revision INTEGER NOT NULL CHECK (key_revision > 0),
    format INTEGER NOT NULL CHECK (format > 0),
    index_nonce BLOB NOT NULL CHECK (typeof(index_nonce) = 'blob' AND length(index_nonce) = 24),
    index_payload BLOB NOT NULL CHECK (typeof(index_payload) = 'blob' AND length(index_payload) >= 16),
    body_nonce BLOB NOT NULL CHECK (typeof(body_nonce) = 'blob' AND length(body_nonce) = 24),
    body_payload BLOB NOT NULL CHECK (typeof(body_payload) = 'blob' AND length(body_payload) >= 16),
    UNIQUE (pack_id, runtime_session_id, forge_session_id)
);
CREATE INDEX idx_session_journal_pack_scope
    ON agent_session_journal_pack(runtime_session_id, pack_id);
CREATE INDEX idx_session_journal_pack_forge ON agent_session_journal_pack(forge_session_id, pack_id);

CREATE TABLE agent_session_journal_membership (
    storage_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('protected', 'ordinary')),
    runtime_session_id TEXT NOT NULL,
    forge_session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    pack_id INTEGER NOT NULL,
    bitmap BLOB NOT NULL CHECK (typeof(bitmap) = 'blob' AND length(bitmap) > 0 AND bitmap <> zeroblob(length(bitmap))),
    PRIMARY KEY (storage_id, role, runtime_session_id, pack_id),
    FOREIGN KEY (storage_id, role, runtime_session_id, forge_session_id)
        REFERENCES agent_session_journal_session
            (storage_id, role, runtime_session_id, forge_session_id) ON DELETE CASCADE,
    FOREIGN KEY (pack_id, runtime_session_id, forge_session_id)
        REFERENCES agent_session_journal_pack
            (pack_id, runtime_session_id, forge_session_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX idx_session_journal_membership_forge ON agent_session_journal_membership(forge_session_id);
CREATE INDEX idx_session_journal_membership_pack ON agent_session_journal_membership(pack_id);
CREATE INDEX idx_session_journal_membership_session
    ON agent_session_journal_membership(runtime_session_id, pack_id);
-- Bitmap length/slot count consistency is checked by storage on every read/write.

CREATE TABLE agent_session_journal_operation_pack (
    storage_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('protected', 'ordinary')),
    runtime_session_id TEXT NOT NULL,
    forge_session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    tier INTEGER NOT NULL CHECK (tier >= 0),
    key_revision INTEGER NOT NULL CHECK (key_revision > 0),
    format INTEGER NOT NULL CHECK (format > 0),
    nonce BLOB NOT NULL CHECK (typeof(nonce) = 'blob' AND length(nonce) = 24),
    payload BLOB NOT NULL CHECK (typeof(payload) = 'blob' AND length(payload) >= 16),
    PRIMARY KEY (storage_id, role, runtime_session_id, tier),
    FOREIGN KEY (storage_id, role, runtime_session_id, forge_session_id)
        REFERENCES agent_session_journal_session
            (storage_id, role, runtime_session_id, forge_session_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX idx_session_journal_operation_pack_forge ON agent_session_journal_operation_pack(forge_session_id);
