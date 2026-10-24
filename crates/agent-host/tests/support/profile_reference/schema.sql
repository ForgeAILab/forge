CREATE TABLE IF NOT EXISTS session (
 storage TEXT NOT NULL, role INTEGER NOT NULL, session TEXT NOT NULL,
 revision INTEGER NOT NULL, epoch INTEGER NOT NULL, dirty INTEGER NOT NULL,
 generation BLOB NOT NULL CHECK(length(generation)=8),
 payload BLOB NOT NULL, nonce BLOB NOT NULL,
 PRIMARY KEY(storage,role,session)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS pack (
 pack_id INTEGER PRIMARY KEY, owner TEXT NOT NULL, payload BLOB NOT NULL, nonce BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS pack_owner ON pack(owner,pack_id);
CREATE TABLE IF NOT EXISTS membership (
 storage TEXT NOT NULL, role INTEGER NOT NULL, session TEXT NOT NULL,
 pack_id INTEGER NOT NULL REFERENCES pack(pack_id) ON DELETE CASCADE,
 bits BLOB NOT NULL, PRIMARY KEY(storage,role,session,pack_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS membership_pack ON membership(pack_id);
CREATE TABLE IF NOT EXISTS pin (
 storage TEXT NOT NULL, role INTEGER NOT NULL, session TEXT NOT NULL,
 pin TEXT NOT NULL, payload BLOB NOT NULL, nonce BLOB NOT NULL,
 PRIMARY KEY(storage,role,session,pin)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS catalog (
 storage TEXT NOT NULL, role INTEGER NOT NULL, session TEXT NOT NULL,
 head BLOB NOT NULL, payload BLOB NOT NULL, nonce BLOB NOT NULL,
 PRIMARY KEY(storage,role,session,head)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS operation_pack (
 storage TEXT NOT NULL, role INTEGER NOT NULL, session TEXT NOT NULL,
 tier INTEGER NOT NULL, payload BLOB NOT NULL, nonce BLOB NOT NULL,
 PRIMARY KEY(storage,role,session,tier)
) WITHOUT ROWID;
