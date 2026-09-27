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
