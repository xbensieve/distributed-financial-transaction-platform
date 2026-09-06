package com.dftp.common.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class DftpMetricsTest {

    private MeterRegistry meterRegistry;
    private DftpMetrics metrics;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        metrics = new DftpMetrics(meterRegistry);
    }

    @Test
    @DisplayName("METRIC-01: Saga and transaction metrics record counts and durations cleanly")
    void testTransactionMetrics() {
        metrics.recordTransactionStarted("TRANSFER");
        metrics.recordTransactionCompleted("TRANSFER");
        metrics.recordTransactionFailed("TRANSFER", "INSUFFICIENT_FUNDS");
        metrics.recordTransactionCompensated("TRANSFER", "TIMEOUT");
        metrics.recordSagaDuration("TRANSFER", "SUCCESS", Duration.ofMillis(450));

        Counter started = meterRegistry.find("transactions.started").tag("type", "transfer").counter();
        assertThat(started).isNotNull();
        assertThat(started.count()).isEqualTo(1.0);

        Counter completed = meterRegistry.find("transactions.completed").tag("type", "transfer").tag("status", "SUCCESS").counter();
        assertThat(completed).isNotNull();
        assertThat(completed.count()).isEqualTo(1.0);

        Counter failed = meterRegistry.find("transactions.failed").tag("type", "transfer").tag("reason", "insufficient_funds").counter();
        assertThat(failed).isNotNull();
        assertThat(failed.count()).isEqualTo(1.0);

        Counter compensated = meterRegistry.find("transactions.compensated").tag("type", "transfer").tag("reason", "timeout").counter();
        assertThat(compensated).isNotNull();
        assertThat(compensated.count()).isEqualTo(1.0);

        assertThat(meterRegistry.find("saga.duration").tag("type", "transfer").timer()).isNotNull();
    }

    @Test
    @DisplayName("METRIC-02: Account and Ledger operational metrics record successfully")
    void testAccountAndLedgerMetrics() {
        metrics.recordAccountHoldSuccess();
        metrics.recordAccountHoldRejected("ACCOUNT_FROZEN");
        metrics.recordAccountSettlementSuccess();
        metrics.recordAccountCompensationSuccess();
        metrics.recordLedgerPostSuccess();
        metrics.recordLedgerPostRejected("UNBALANCED_ENTRIES");

        assertThat(meterRegistry.find("account.hold.success").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("account.hold.rejected").tag("reason", "account_frozen").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("account.settlement.success").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("account.compensation.success").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("ledger.post.success").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("ledger.post.rejected").tag("reason", "unbalanced_entries").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("METRIC-03: Outbox, Inbox, Kafka, and DLT replay metrics record counts and gauges")
    void testReliabilityAndDltMetrics() {
        metrics.recordInboxDuplicate("transaction-service", "AccountHoldSucceeded");
        metrics.setOutboxPendingGauge("transaction-service", 42);
        metrics.recordOutboxClaimed("transaction-service", 10);
        metrics.recordOutboxPublished("transaction-events");
        metrics.recordOutboxRetry("transaction-events");
        metrics.recordOutboxStaleClaim("transaction-service");
        metrics.recordKafkaConsumerError("ledger-events", "ledger-service-group");
        metrics.recordKafkaRetry("ledger-events");
        metrics.recordKafkaDltMessage("transaction-events.DLT");
        metrics.recordDltReplayCount("transaction-events", 5);
        metrics.recordDltReplayRejected("transaction-events", "COOLDOWN_ACTIVE");
        metrics.recordApiIdempotencyHit("/transactions");
        metrics.recordApiIdempotencyCollision("/transactions");
        metrics.recordReconciliationAnomaly("FUNDS_HELD_LEDGER_ABSENT");

        assertThat(meterRegistry.find("inbox.duplicates").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("outbox.pending").gauge().value()).isEqualTo(42.0);
        assertThat(meterRegistry.find("outbox.claimed").counter().count()).isEqualTo(10.0);
        assertThat(meterRegistry.find("outbox.published").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("kafka.dlt.count").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("dlt.replay.count").counter().count()).isEqualTo(5.0);
        assertThat(meterRegistry.find("reconciliation.anomaly").tag("anomaly_type", "funds_held_ledger_absent").counter().count()).isEqualTo(1.0);
    }
}
