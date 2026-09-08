-- Phase 13: Distributed Coordination & Background Task Locking (P13 / ADR-013)
-- ShedLock table backed by PostgreSQL for singleton background tasks
-- Clock-skew resilient via database clock (usingDbTime())

CREATE TABLE IF NOT EXISTS shedlock (
    name VARCHAR(64) NOT NULL,
    lock_until TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_at TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_by VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);

CREATE INDEX IF NOT EXISTS idx_shedlock_lock_until ON shedlock(lock_until);
