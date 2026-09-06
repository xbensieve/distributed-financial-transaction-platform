ALTER TABLE transactions
DROP COLUMN account_id;

ALTER TABLE transactions
ADD COLUMN source_account_id UUID NOT NULL;

ALTER TABLE transactions
ADD COLUMN destination_account_id UUID NOT NULL;
