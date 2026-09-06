CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT
);

CREATE INDEX idx_outbox_events_status ON outbox_events(status);
CREATE INDEX idx_outbox_events_created_at ON outbox_events(created_at) WHERE status = 'PENDING';

CREATE TABLE inbox_messages (
    event_id UUID PRIMARY KEY,
    consumer_group VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PROCESSED',
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
