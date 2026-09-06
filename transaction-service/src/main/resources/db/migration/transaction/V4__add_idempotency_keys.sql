CREATE TABLE api_idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    request_hash VARCHAR(64),
    response_status INTEGER,
    response_body JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
