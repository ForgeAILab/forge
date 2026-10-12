-- An owner stores only opaque, salted revisions, never environment values.
-- A new server process starts a new environment generation and attests once.
CREATE TABLE check_environment_attestation (
    owner_id TEXT NOT NULL CHECK(length(owner_id) > 0),
    environment_generation TEXT NOT NULL CHECK(length(environment_generation) > 0),
    salt_revision TEXT NOT NULL CHECK(length(salt_revision) > 0),
    inherited_revision TEXT NOT NULL CHECK(length(inherited_revision) > 0),
    shell_revision TEXT NOT NULL CHECK(length(shell_revision) > 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY(owner_id, environment_generation, salt_revision)
);
