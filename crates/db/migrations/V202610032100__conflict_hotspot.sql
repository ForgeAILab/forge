-- New detection starts at installation, with no historical handoffs counted.
INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
SELECT 'conflict-hotspots', COALESCE(MAX(sequence), 0), 1,
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM domain_event WHERE 1
ON CONFLICT(consumer_name) DO NOTHING;

INSERT INTO event_consumer_cutover (consumer_name, cutover_sequence, reason, created_at)
-- transition_log writers use Utc::now().to_rfc3339(): UTC with +00:00.
-- SQLite's millisecond precision is the same RFC3339 form at that precision.
SELECT consumer_name, last_sequence, 'conflict-hotspots-install-cutover',
    strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
FROM event_consumer_cursor WHERE consumer_name = 'conflict-hotspots'
ON CONFLICT(consumer_name) DO NOTHING;

-- Keep the first detection stable until the user resolves its Attention item.
-- Episodes, resolution boundaries, detections and cursor commit together.
CREATE TABLE conflict_hotspot_boundary (
    project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    resolved_after TEXT,
    open_since TEXT,
    PRIMARY KEY (project_id, path)
);
