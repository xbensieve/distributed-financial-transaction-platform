-- Phase 11: Batch Settlement and Cross-Service Financial Reconciliation Schema

CREATE TABLE settlement_batches (
    id UUID PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL,
    total_items INT NOT NULL DEFAULT 0,
    success_count INT NOT NULL DEFAULT 0,
    failure_count INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_by VARCHAR(64) NOT NULL DEFAULT 'system',
    version BIGINT NOT NULL DEFAULT 0,
    metadata JSONB
);

CREATE INDEX idx_settlement_batches_status ON settlement_batches(status);
CREATE INDEX idx_settlement_batches_created_at ON settlement_batches(created_at);

CREATE TABLE settlement_batch_items (
    id UUID PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL REFERENCES settlement_batches(batch_id),
    transaction_id VARCHAR(255) NOT NULL,
    source_account_id UUID NOT NULL,
    destination_account_id UUID NOT NULL,
    amount NUMERIC(19, 4) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uq_batch_item_batch_tx UNIQUE (batch_id, transaction_id)
);

CREATE INDEX idx_batch_items_batch_status ON settlement_batch_items(batch_id, status);
CREATE INDEX idx_batch_items_transaction_id ON settlement_batch_items(transaction_id);
CREATE INDEX idx_batch_items_status_updated ON settlement_batch_items(status, updated_at);

CREATE TABLE reconciliation_anomalies (
    id UUID PRIMARY KEY,
    reconciliation_id UUID NOT NULL,
    business_transaction_id VARCHAR(255) NOT NULL,
    account_id UUID,
    ledger_transaction_id VARCHAR(255),
    anomaly_type VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'DETECTED',
    observed_state JSONB NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    resolution TEXT,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(64),
    CONSTRAINT uq_anomaly_tx_type UNIQUE (business_transaction_id, anomaly_type)
);

CREATE INDEX idx_reconciliation_anomalies_status ON reconciliation_anomalies(status);
CREATE INDEX idx_reconciliation_anomalies_type ON reconciliation_anomalies(anomaly_type);
CREATE INDEX idx_reconciliation_anomalies_severity ON reconciliation_anomalies(severity);
CREATE INDEX idx_reconciliation_anomalies_detected_at ON reconciliation_anomalies(detected_at);
