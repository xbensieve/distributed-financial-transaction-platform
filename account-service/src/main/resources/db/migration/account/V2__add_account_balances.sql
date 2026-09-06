ALTER TABLE accounts
ADD COLUMN settled_balance NUMERIC(19, 4) NOT NULL DEFAULT 0,
ADD COLUMN held_funds NUMERIC(19, 4) NOT NULL DEFAULT 0,
ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

CREATE TABLE account_operations (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL,
    transaction_id VARCHAR(255) NOT NULL,
    operation_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (transaction_id, operation_type, account_id)
);
