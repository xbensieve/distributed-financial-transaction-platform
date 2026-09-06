ALTER TABLE accounts ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT 'system-unassigned';
CREATE INDEX idx_accounts_owner_id ON accounts(owner_id);
