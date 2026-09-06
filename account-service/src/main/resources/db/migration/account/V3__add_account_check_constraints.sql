-- Database invariants (§16): Enforce financial safety at the DB level.
-- These constraints guarantee that no code path can violate balance integrity.

ALTER TABLE accounts
ADD CONSTRAINT chk_held_funds_non_negative CHECK (held_funds >= 0);

ALTER TABLE accounts
ADD CONSTRAINT chk_settled_balance_non_negative CHECK (settled_balance >= 0);
