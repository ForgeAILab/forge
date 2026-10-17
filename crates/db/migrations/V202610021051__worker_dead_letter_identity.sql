-- Stable identity for later replay/dismiss operations; existing item keys survive.
ALTER TABLE worker_dead_letter ADD COLUMN id TEXT;
UPDATE worker_dead_letter SET id = lower(hex(randomblob(16)));
CREATE UNIQUE INDEX idx_worker_dead_letter_id ON worker_dead_letter(id);
ALTER TABLE worker_item_failure ADD COLUMN transient_attempts INTEGER NOT NULL DEFAULT 0;
UPDATE worker_item_failure SET transient_attempts = attempts, attempts = 0 WHERE error_kind = 'transient';
CREATE TRIGGER worker_dead_letter_requires_identity BEFORE INSERT ON worker_dead_letter
WHEN NEW.id IS NULL BEGIN SELECT RAISE(ABORT, 'dead-letter identity required'); END;
