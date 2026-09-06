package com.dftp.common.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Authoritative Metrics Catalog for DFTP Financial Workflows.
 * Standardizes Micrometer / Prometheus metrics with strictly low-cardinality labels.
 * High-cardinality attributes (e.g. transactionId, eventId, customerId) are EXCLUDED
 * and must be tracked via OpenTelemetry traces and structured MDC logs.
 */
@Component
public class DftpMetrics {

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    public DftpMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // --- Saga / Transaction Metrics ---

    public void recordTransactionStarted(String type) {
        getOrCreateCounter("transactions.started", Tags.of("type", sanitize(type))).increment();
    }

    public void recordTransactionCompleted(String type) {
        getOrCreateCounter("transactions.completed", Tags.of("type", sanitize(type), "status", "SUCCESS")).increment();
    }

    public void recordTransactionFailed(String type, String reason) {
        getOrCreateCounter("transactions.failed", Tags.of("type", sanitize(type), "reason", sanitize(reason))).increment();
    }

    public void recordTransactionCompensated(String type, String reason) {
        getOrCreateCounter("transactions.compensated", Tags.of("type", sanitize(type), "reason", sanitize(reason))).increment();
    }

    public void recordSagaDuration(String type, String outcome, Duration duration) {
        getOrCreateTimer("saga.duration", Tags.of("type", sanitize(type), "outcome", sanitize(outcome)))
                .record(duration);
    }

    // --- Account Metrics ---

    public void recordAccountHoldSuccess() {
        getOrCreateCounter("account.hold.success", Tags.empty()).increment();
    }

    public void recordAccountHoldRejected(String reason) {
        getOrCreateCounter("account.hold.rejected", Tags.of("reason", sanitize(reason))).increment();
    }

    public void recordAccountSettlementSuccess() {
        getOrCreateCounter("account.settlement.success", Tags.empty()).increment();
    }

    public void recordAccountCompensationSuccess() {
        getOrCreateCounter("account.compensation.success", Tags.empty()).increment();
    }

    // --- Ledger Metrics ---

    public void recordLedgerPostSuccess() {
        getOrCreateCounter("ledger.post.success", Tags.empty()).increment();
    }

    public void recordLedgerPostRejected(String reason) {
        getOrCreateCounter("ledger.post.rejected", Tags.of("reason", sanitize(reason))).increment();
    }

    // --- Inbox / Outbox Reliability Metrics ---

    public void recordInboxDuplicate(String service, String eventType) {
        getOrCreateCounter("inbox.duplicates", Tags.of("service", sanitize(service), "eventType", sanitize(eventType))).increment();
    }

    public void setOutboxPendingGauge(String service, long count) {
        getOrCreateGauge("outbox.pending", Tags.of("service", sanitize(service))).set(count);
    }

    public void recordOutboxClaimed(String service, int count) {
        getOrCreateCounter("outbox.claimed", Tags.of("service", sanitize(service))).increment(count);
    }

    public void recordOutboxPublished(String topic) {
        getOrCreateCounter("outbox.published", Tags.of("topic", sanitize(topic))).increment();
    }

    public void recordOutboxRetry(String topic) {
        getOrCreateCounter("outbox.retries", Tags.of("topic", sanitize(topic))).increment();
    }

    public void recordOutboxStaleClaim(String service) {
        getOrCreateCounter("outbox.stale_claims", Tags.of("service", sanitize(service))).increment();
    }

    // --- Kafka Reliability & DLT Metrics ---

    public void recordKafkaConsumerError(String topic, String consumerGroup) {
        getOrCreateCounter("kafka.consumer.errors", Tags.of("topic", sanitize(topic), "group", sanitize(consumerGroup))).increment();
    }

    public void recordKafkaRetry(String topic) {
        getOrCreateCounter("kafka.retry.count", Tags.of("topic", sanitize(topic))).increment();
    }

    public void recordKafkaDltMessage(String topic) {
        getOrCreateCounter("kafka.dlt.count", Tags.of("topic", sanitize(topic))).increment();
    }

    public void recordDltReplayCount(String topic, int count) {
        getOrCreateCounter("dlt.replay.count", Tags.of("topic", sanitize(topic))).increment(count);
    }

    public void recordDltReplayRejected(String topic, String reason) {
        getOrCreateCounter("dlt.replay.rejected", Tags.of("topic", sanitize(topic), "reason", sanitize(reason))).increment();
    }

    // --- API Idempotency Metrics ---

    public void recordApiIdempotencyHit(String endpoint) {
        getOrCreateCounter("api.idempotency.hit", Tags.of("endpoint", sanitize(endpoint))).increment();
    }

    public void recordApiIdempotencyCollision(String endpoint) {
        getOrCreateCounter("api.idempotency.collision", Tags.of("endpoint", sanitize(endpoint))).increment();
    }

    // --- Reconciliation Telemetry ---

    public void recordReconciliationAnomaly(String anomalyType) {
        getOrCreateCounter("reconciliation.anomaly", Tags.of("anomaly_type", sanitize(anomalyType))).increment();
    }

    // --- Rate Limiting Metrics ---

    public void recordRateLimitExceeded(String tier) {
        getOrCreateCounter("rate.limit.exceeded", Tags.of("tier", sanitize(tier))).increment();
    }

    // --- Helpers ---

    private Counter getOrCreateCounter(String name, Iterable<Tag> tags) {
        String key = name + tags.toString();
        return counters.computeIfAbsent(key, k -> Counter.builder(name)
                .tags(tags)
                .description("DFTP Metric: " + name)
                .register(registry));
    }

    private Timer getOrCreateTimer(String name, Iterable<Tag> tags) {
        String key = name + tags.toString();
        return timers.computeIfAbsent(key, k -> Timer.builder(name)
                .tags(tags)
                .description("DFTP Timer: " + name)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry));
    }

    private AtomicLong getOrCreateGauge(String name, Iterable<Tag> tags) {
        String key = name + tags.toString();
        return gauges.computeIfAbsent(key, k -> {
            AtomicLong value = new AtomicLong(0);
            registry.gauge(name, tags, value);
            return value;
        });
    }

    private String sanitize(String input) {
        return (input == null || input.isBlank()) ? "unknown" : input.trim().toLowerCase().replaceAll("[^a-z0-9_.-]", "_");
    }
}
