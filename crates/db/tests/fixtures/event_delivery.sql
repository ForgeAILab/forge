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

