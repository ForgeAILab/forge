-- Delivery leases and projection receipts are retired metadata, not user data.
-- Attention consumer health held only operational counters, timestamps, bounded
-- errors and lease diagnostics. Runtime health replaced it; it holds no user data.
-- Runtime checkpoints and every domain projection remain authoritative.
DROP TABLE event_processing_lease;
DROP TABLE event_projection_receipt;
DROP TABLE attention_consumer_health;
DELETE FROM event_consumer_cursor WHERE consumer_name = 'sse-broadcast';
