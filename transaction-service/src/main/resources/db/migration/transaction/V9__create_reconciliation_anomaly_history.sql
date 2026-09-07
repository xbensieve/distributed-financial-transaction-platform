-- Phase 11 Remediation: Immutable Reconciliation Anomaly Audit History (P11-REM-04)

CREATE TABLE reconciliation_anomaly_history (
    id UUID PRIMARY KEY,
    anomaly_id UUID NOT NULL REFERENCES reconciliation_anomalies(id) ON DELETE CASCADE,
    business_transaction_id VARCHAR(255) NOT NULL,
    anomaly_type VARCHAR(64) NOT NULL,
    from_status VARCHAR(32),
    to_status VARCHAR(32) NOT NULL,
    actor VARCHAR(64) NOT NULL,
    resolution TEXT,
    notes TEXT,
    observed_state JSONB,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_anomaly_history_anomaly_id ON reconciliation_anomaly_history(anomaly_id);
CREATE INDEX idx_anomaly_history_business_tx ON reconciliation_anomaly_history(business_transaction_id);
CREATE INDEX idx_anomaly_history_occurred_at ON reconciliation_anomaly_history(occurred_at);
