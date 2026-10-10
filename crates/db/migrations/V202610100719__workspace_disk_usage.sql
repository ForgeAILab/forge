-- Measured disk usage of a Task root (3.4 stage D).
-- Additive only: existing rows keep every value and read both columns as NULL
-- until the garbage-collection sweep measures them. The sweep writes the
-- number only for a walk it finished, so NULL means "not measured", never 0.
ALTER TABLE workspace ADD COLUMN disk_bytes INTEGER CHECK(disk_bytes IS NULL OR disk_bytes >= 0);
ALTER TABLE workspace ADD COLUMN disk_measured_at TEXT;
