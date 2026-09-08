package com.dftp.transaction.backpressure;

import lombok.Getter;

/**
 * Thrown when an inbound business transaction is rejected at admission control
 * due to Outbox queue saturation during a broker outage.
 *
 * Guarantees that the rejection occurs before any database mutation.
 */
@Getter
public class OutboxAdmissionThrottledException extends RuntimeException {

    private final long pendingCount;
    private final int retryAfterSeconds;

    public OutboxAdmissionThrottledException(long pendingCount, int retryAfterSeconds) {
        super(String.format("Transaction admission throttled: pending outbox events (%d) reached capacity limit. Retry after %d seconds.",
                pendingCount, retryAfterSeconds));
        this.pendingCount = pendingCount;
        this.retryAfterSeconds = retryAfterSeconds;
    }
}
