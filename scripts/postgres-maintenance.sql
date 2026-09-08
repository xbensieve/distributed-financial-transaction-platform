-- =====================================================================================
-- DFTP PostgreSQL Production Maintenance & Storage Optimization Script
-- Author: Principal Distributed Systems Engineer & DBA
-- Database Targets: transaction_db, account_db, ledger_db
-- Compatibility: PostgreSQL 15 / 16+
-- =====================================================================================

-- =====================================================================================
-- SECTION 1: STORAGE PARAMETERS & AUTOVACUUM HARDENING FOR HIGH-CHURN TABLES
-- =====================================================================================
-- Outbox and Inbox tables experience rapid INSERT -> UPDATE -> DELETE/ARCHIVE churn.
-- Default autovacuum_vacuum_scale_factor (0.2 = 20% dead tuples) is too sluggish,
-- causing severe table bloat and degraded index scan performance under high TPS.

DO $$
BEGIN
    -- 1.1 Outbox Events Storage Optimization
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'outbox_events') THEN
        ALTER TABLE outbox_events SET (
            autovacuum_vacuum_scale_factor = 0.05,        -- Trigger vacuum when dead tuples reach 5%
            autovacuum_vacuum_threshold = 500,             -- Minimum dead tuples before vacuuming
            autovacuum_vacuum_cost_limit = 1000,           -- Higher I/O throughput allowance
            autovacuum_vacuum_cost_delay = 2,              -- Minimal delay (2ms) for fast cleanup
            autovacuum_analyze_scale_factor = 0.02         -- Keep planner statistics razor-sharp
        );
        RAISE NOTICE 'Applied tuned autovacuum parameters to outbox_events';
    END IF;

    -- 1.2 Inbox Events Storage Optimization (Deduplication Log)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'inbox_events') THEN
        ALTER TABLE inbox_events SET (
            autovacuum_vacuum_scale_factor = 0.05,
            autovacuum_vacuum_threshold = 500,
            autovacuum_vacuum_cost_limit = 1000,
            autovacuum_vacuum_cost_delay = 2,
            autovacuum_analyze_scale_factor = 0.02
        );
        RAISE NOTICE 'Applied tuned autovacuum parameters to inbox_events';
    END IF;

    -- 1.3 ShedLock Table Optimization (Frequent singleton updates)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'shedlock') THEN
        ALTER TABLE shedlock SET (
            autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_vacuum_threshold = 100,
            autovacuum_vacuum_cost_limit = 1000
        );
        RAISE NOTICE 'Applied tuned autovacuum parameters to shedlock';
    END IF;
END $$;

-- =====================================================================================
-- SECTION 2: ARCHIVAL & PURGE STORED PROCEDURES (BOUNDED BATCH PURGING)
-- =====================================================================================
-- Purging massive transaction or outbox tables in a single unqualified DELETE causes
-- exclusive lock holding, lock contention with OutboxRelay, and WAL burst spikes.
-- The routine below performs iterative bounded batch deletes with safe transaction commits.

CREATE OR REPLACE PROCEDURE purge_archived_outbox_events(
    p_retention_days INT DEFAULT 7,
    p_batch_size INT DEFAULT 5000,
    p_max_batches INT DEFAULT 20
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff_time TIMESTAMP WITH TIME ZONE;
    v_rows_deleted INT := 0;
    v_total_deleted INT := 0;
    v_batch_count INT := 0;
BEGIN
    v_cutoff_time := NOW() - (p_retention_days || ' days')::INTERVAL;
    RAISE NOTICE 'Starting outbox purge for records older than %', v_cutoff_time;

    LOOP
        EXIT WHEN v_batch_count >= p_max_batches;

        DELETE FROM outbox_events
        WHERE id IN (
            SELECT id FROM outbox_events
            WHERE status = 'PUBLISHED'
              AND created_at < v_cutoff_time
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        );

        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;
        v_total_deleted := v_total_deleted + v_rows_deleted;
        v_batch_count := v_batch_count + 1;

        COMMIT; -- Release row locks and allow other transactions to progress

        IF v_rows_deleted < p_batch_size THEN
            EXIT; -- Cleaned all candidate rows
        END IF;

        PERFORM pg_sleep(0.05); -- 50ms cooperative yield to prevent I/O starvation
    END LOOP;

    RAISE NOTICE 'Completed outbox purge: deleted % total records across % batches.', v_total_deleted, v_batch_count;
END;
$$;

-- 2.2 Inbox Deduplication Records Cleanup Procedure
CREATE OR REPLACE PROCEDURE purge_aged_inbox_events(
    p_retention_days INT DEFAULT 14,
    p_batch_size INT DEFAULT 5000,
    p_max_batches INT DEFAULT 20
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff_time TIMESTAMP WITH TIME ZONE;
    v_rows_deleted INT := 0;
    v_total_deleted INT := 0;
    v_batch_count INT := 0;
BEGIN
    v_cutoff_time := NOW() - (p_retention_days || ' days')::INTERVAL;
    RAISE NOTICE 'Starting inbox purge for processed events older than %', v_cutoff_time;

    LOOP
        EXIT WHEN v_batch_count >= p_max_batches;

        DELETE FROM inbox_events
        WHERE id IN (
            SELECT id FROM inbox_events
            WHERE status = 'PROCESSED'
              AND processed_at < v_cutoff_time
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        );

        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;
        v_total_deleted := v_total_deleted + v_rows_deleted;
        v_batch_count := v_batch_count + 1;

        COMMIT;

        IF v_rows_deleted < p_batch_size THEN
            EXIT;
        END IF;

        PERFORM pg_sleep(0.05);
    END LOOP;

    RAISE NOTICE 'Completed inbox purge: deleted % total records across % batches.', v_total_deleted, v_batch_count;
END;
$$;

-- =====================================================================================
-- SECTION 3: RESIDUAL DISTRIBUTED LOCK CLEANUP
-- =====================================================================================
-- Clean up defunct or orphaned ShedLock entries for deleted / renamed tasks.
CREATE OR REPLACE PROCEDURE cleanup_residual_shedlock_entries(
    p_grace_days INT DEFAULT 30
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_deleted INT;
BEGIN
    DELETE FROM shedlock
    WHERE lock_until < NOW() - (p_grace_days || ' days')::INTERVAL;

    GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;
    RAISE NOTICE 'Cleaned % stale shedlock entries older than % days.', v_rows_deleted, p_grace_days;
END;
$$;

-- =====================================================================================
-- SECTION 4: CONCURRENT INDEX REINDEX & BLOAT DEFENSE
-- =====================================================================================
-- High-frequency partial indices on outbox_events require periodic rebuilds
-- to reclaim index bloat without blocking live reads or writes.

-- Note: REINDEX CONCURRENTLY cannot run inside a multi-command DO block or transaction.
-- Run the following statements individually in scheduled maintenance jobs:
--
-- REINDEX INDEX CONCURRENTLY idx_outbox_events_status_created;
-- REINDEX INDEX CONCURRENTLY idx_settlement_batch_items_batch_status;

-- =====================================================================================
-- SECTION 5: HEALTH & TABLE BLOAT MONITORING DIAGNOSTICS
-- =====================================================================================
-- Diagnostic query to assess dead tuples and bloat on core financial and messaging tables.
CREATE OR REPLACE VIEW v_dftp_table_health AS
SELECT 
    schemaname,
    relname AS table_name,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    ROUND(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2) AS dead_tuple_pct,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_table_size
FROM pg_stat_user_tables
WHERE relname IN ('outbox_events', 'inbox_events', 'shedlock', 'transactions', 'settlement_batches', 'settlement_batch_items', 'ledger_entries')
ORDER BY n_dead_tup DESC;

COMMENT ON VIEW v_dftp_table_health IS 'DFTP Table Health & Dead Tuple Diagnostic View';
