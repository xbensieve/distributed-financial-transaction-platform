ALTER TABLE transactions ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT 'system-unassigned';
CREATE INDEX idx_transactions_owner_id ON transactions(owner_id);

ALTER TABLE account_references ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT 'system-unassigned';
CREATE INDEX idx_account_references_owner_id ON account_references(owner_id);
