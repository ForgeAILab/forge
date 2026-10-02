-- Delivery leases and projection receipts are retired metadata, not user data.
-- Runtime checkpoints and every domain projection remain authoritative.
DROP TABLE event_processing_lease;
DROP TABLE event_projection_receipt;
DELETE FROM event_consumer_cursor WHERE consumer_name = 'sse-broadcast';
