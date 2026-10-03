-- New detection starts at installation, with no historical handoffs counted.
INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
SELECT 'conflict-hotspots', COALESCE(MAX(sequence), 0), 1,
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM domain_event WHERE 1
ON CONFLICT(consumer_name) DO NOTHING;

INSERT INTO event_consumer_cutover (consumer_name, cutover_sequence, reason, created_at)
SELECT consumer_name, last_sequence, 'conflict-hotspots-install-cutover', updated_at
FROM event_consumer_cursor WHERE consumer_name = 'conflict-hotspots'
ON CONFLICT(consumer_name) DO NOTHING;

-- Attention clears resolved_at on reopening. Retain that counting boundary
-- across refreshes so earlier episodes cannot contribute to another detection.
CREATE TABLE conflict_hotspot_boundary (
    project_id TEXT NOT NULL,
    path TEXT NOT NULL,
    resolved_after TEXT NOT NULL,
    PRIMARY KEY (project_id, path)
);
