-- Disk facts of a daemon's workspace root (3.4 stage E).
-- Additive only: existing rows keep every value and read the column as NULL
-- until the daemon's next report carries a reading. NULL means "no reading",
-- and a machine without a reading is never refused for disk pressure.
ALTER TABLE daemon ADD COLUMN disk_json TEXT;
