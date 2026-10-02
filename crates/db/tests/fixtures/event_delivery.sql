CREATE TABLE event_projection_receipt (
    consumer_name     TEXT NOT NULL,
    event_id          TEXT NOT NULL REFERENCES domain_event(id) ON DELETE CASCADE,
    dedupe_key        TEXT NOT NULL,
    processed_at      TEXT NOT NULL,
    PRIMARY KEY (consumer_name, event_id),
    UNIQUE (consumer_name, dedupe_key)
);

CREATE TABLE event_processing_lease (
    consumer_name     TEXT NOT NULL,
    event_sequence    INTEGER NOT NULL REFERENCES domain_event(sequence) ON DELETE CASCADE,
    lease_owner       TEXT NOT NULL,
    leased_until      TEXT NOT NULL,
    attempts          INTEGER NOT NULL DEFAULT 1,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (consumer_name, event_sequence)
);


CREATE TABLE attention_consumer_health (
    consumer_name      TEXT PRIMARY KEY,
    last_sequence      INTEGER NOT NULL DEFAULT 0,
    last_started_at    TEXT,
    last_success_at    TEXT,
    last_error_at      TEXT,
    last_error_code    TEXT,
    last_error_message TEXT,
    lease_owner        TEXT,
    lease_until        TEXT,
    processed_events   INTEGER NOT NULL DEFAULT 0,
    version            INTEGER NOT NULL DEFAULT 1,
    updated_at         TEXT NOT NULL
);

