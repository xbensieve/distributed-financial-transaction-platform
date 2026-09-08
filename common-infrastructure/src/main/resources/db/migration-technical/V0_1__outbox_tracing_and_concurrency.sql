-- V0_1: Add W3C distributed tracing and multi-instance concurrency support to outbox_events
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS traceparent VARCHAR(128);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS tracestate VARCHAR(256);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claimed_by VARCHAR(128);

-- Create composite indexes for high-throughput polling and fast recovery of stale claimed events
CREATE INDEX IF NOT EXISTS idx_outbox_events_polling ON outbox_events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_events_claimed_recovery ON outbox_events(status, updated_at) WHERE status = 'CLAIMED';
