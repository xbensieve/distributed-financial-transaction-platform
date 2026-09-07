-- Phase 12 Resilience: Protect Reconciliation Anomaly Audit History from deletion and mutation

-- 1. Drop existing FK with ON DELETE CASCADE and replace with ON DELETE RESTRICT
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = 'reconciliation_anomaly_history'
          AND constraint_type = 'FOREIGN KEY'
    ) LOOP
        EXECUTE 'ALTER TABLE reconciliation_anomaly_history DROP CONSTRAINT ' || quote_ident(r.constraint_name);
    END LOOP;
END $$;

ALTER TABLE reconciliation_anomaly_history
    ADD CONSTRAINT fk_anomaly_history_anomaly_id
    FOREIGN KEY (anomaly_id) REFERENCES reconciliation_anomalies(id) ON DELETE RESTRICT;

-- 2. Enforce DB-level immutability on reconciliation_anomaly_history (no UPDATE or DELETE allowed)
CREATE OR REPLACE FUNCTION prevent_reconciliation_anomaly_history_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'reconciliation_anomaly_history is strictly append-only and immutable. UPDATE and DELETE are prohibited.';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_protect_reconciliation_anomaly_history ON reconciliation_anomaly_history;

CREATE TRIGGER trg_protect_reconciliation_anomaly_history
BEFORE UPDATE OR DELETE ON reconciliation_anomaly_history
FOR EACH ROW
EXECUTE FUNCTION prevent_reconciliation_anomaly_history_mutation();
