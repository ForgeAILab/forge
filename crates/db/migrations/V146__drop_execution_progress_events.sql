-- One-time historical repair: these child foreign keys otherwise scan their
-- tables for every deleted event. None has an equivalent final-schema index.
CREATE INDEX IF NOT EXISTS idx_v146_tmp_projection_receipt_event
    ON event_projection_receipt(event_id);
CREATE INDEX IF NOT EXISTS idx_v146_tmp_processing_lease_event
    ON event_processing_lease(event_sequence);
CREATE INDEX IF NOT EXISTS idx_v146_tmp_attention_source_event
    ON attention_projection(source_event_id);
CREATE INDEX IF NOT EXISTS idx_v146_tmp_memory_legacy_source_event
    ON memory_item_legacy(source_event_id);
CREATE INDEX IF NOT EXISTS idx_v146_tmp_command_receipt_event
    ON command_receipt(event_id);
CREATE INDEX IF NOT EXISTS idx_v146_tmp_wake_source_sequence
    ON agent_wake_disposition(source_event_sequence);
CREATE INDEX IF NOT EXISTS idx_v146_tmp_wake_current_source_event
    ON agent_wake_disposition_current(source_event_id);

-- Semantic progress no longer appends an `execution.progressed` domain event
-- for every agent log line; it only advances `execution.last_progress_at`,
-- and appends an event solely when progress ends a warned stall. The
-- historical per-line events carry no information the execution row and its
-- JSONL log do not, yet each one also holds a receipt row per consumer.
-- Delete them (receipts and processing leases cascade), keeping any event
-- another durable row still points at.

DELETE FROM domain_event
WHERE event_type = 'execution.progressed'
  AND id NOT IN (SELECT source_event_id FROM attention_projection)
  AND id NOT IN (SELECT event_id FROM command_receipt)
  AND id NOT IN (SELECT source_event_id FROM agent_wake_disposition)
  AND id NOT IN (SELECT source_event_id FROM agent_wake_disposition_current);

DROP INDEX idx_v146_tmp_projection_receipt_event;
DROP INDEX idx_v146_tmp_processing_lease_event;
DROP INDEX idx_v146_tmp_attention_source_event;
DROP INDEX idx_v146_tmp_memory_legacy_source_event;
DROP INDEX idx_v146_tmp_command_receipt_event;
DROP INDEX idx_v146_tmp_wake_source_sequence;
DROP INDEX idx_v146_tmp_wake_current_source_event;
