CREATE TABLE ledger_transactions (
    id UUID PRIMARY KEY,
    ledger_transaction_id VARCHAR(255) NOT NULL UNIQUE,
    business_transaction_id VARCHAR(255) NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE postings (
    id UUID PRIMARY KEY,
    ledger_transaction_id UUID NOT NULL REFERENCES ledger_transactions(id),
    account_id UUID NOT NULL,
    amount DECIMAL(19, 4) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    posting_type VARCHAR(20) NOT NULL CHECK (posting_type IN ('DEBIT', 'CREDIT')),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE account_references (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- 1. Double-Entry Invariant Trigger (Deferred)
CREATE FUNCTION check_ledger_balance() RETURNS TRIGGER AS $$
DECLARE
    balance DECIMAL(19, 4);
BEGIN
    -- Calculate sum of debits - sum of credits for this ledger transaction
    SELECT COALESCE(SUM(CASE WHEN posting_type = 'DEBIT' THEN amount ELSE -amount END), 0)
    INTO balance
    FROM postings
    WHERE ledger_transaction_id = NEW.ledger_transaction_id;

    IF balance != 0 THEN
        RAISE EXCEPTION 'Double-entry invariant violated for ledger_transaction_id: %', NEW.ledger_transaction_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger runs at the end of the transaction
CREATE CONSTRAINT TRIGGER ensure_ledger_balance
AFTER INSERT OR UPDATE OR DELETE ON postings
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION check_ledger_balance();


-- 2. Immutability Hardening (Prevent Deletion)
CREATE FUNCTION prevent_deletion() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Deletion of accounting history is strictly prohibited.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_posting_deletion
BEFORE DELETE ON postings
FOR EACH ROW EXECUTE FUNCTION prevent_deletion();

CREATE TRIGGER prevent_ledger_tx_deletion
BEFORE DELETE ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION prevent_deletion();

-- 3. Immutability Hardening (Prevent Updates)
CREATE FUNCTION prevent_update() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Modification of accounting history is strictly prohibited.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_posting_update
BEFORE UPDATE ON postings
FOR EACH ROW EXECUTE FUNCTION prevent_update();

CREATE TRIGGER prevent_ledger_tx_update
BEFORE UPDATE ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION prevent_update();
