package com.dftp.transaction.backpressure;

import com.dftp.common.observability.DftpMetrics;
import com.dftp.common.outbox.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages Outbox queue depth monitoring and inbound admission control backpressure.
 *
 * Implements strict four-state operational classification:
 * 1. HEALTHY: pending < warningThreshold
 * 2. WARNING: warningThreshold <= pending < criticalThreshold
 * 3. CRITICAL: criticalThreshold <= pending < throttleThreshold
 * 4. THROTTLED: pending >= throttleThreshold (admission throttled until count <= recoveryThreshold)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxBackpressureService {

    private final OutboxEventRepository outboxEventRepository;
    private final OutboxBackpressureProperties properties;

    @Autowired(required = false)
    private DftpMetrics dftpMetrics = new DftpMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

    private final AtomicReference<OutboxBackpressureState> currentState =
            new AtomicReference<>(OutboxBackpressureState.HEALTHY);

    private volatile long cachedPendingCount = 0;

    @Scheduled(fixedDelayString = "${dftp.outbox.backpressure.check-interval:1000}")
    public void scheduledStateCheck() {
        if (!properties.isEnabled()) {
            return;
        }
        refreshState();
    }

    public synchronized OutboxBackpressureState refreshState() {
        if (!properties.isEnabled()) {
            currentState.set(OutboxBackpressureState.HEALTHY);
            return OutboxBackpressureState.HEALTHY;
        }

        long pending = outboxEventRepository.countByStatus("PENDING");
        this.cachedPendingCount = pending;

        if (dftpMetrics != null) {
            dftpMetrics.setOutboxPendingGauge("transaction-service", pending);
        }

        OutboxBackpressureState previous = currentState.get();
        OutboxBackpressureState next;

        if (previous == OutboxBackpressureState.THROTTLED) {
            // Hysteresis: Stay throttled until backlog drains below recovery threshold
            if (pending <= properties.getRecoveryThreshold()) {
                if (pending >= properties.getWarningThreshold()) {
                    next = OutboxBackpressureState.WARNING;
                } else {
                    next = OutboxBackpressureState.HEALTHY;
                }
                log.info("OUTBOX BACKPRESSURE RECOVERED: Pending count {} dropped to/below recovery threshold {}. State: {} -> {}",
                        pending, properties.getRecoveryThreshold(), previous, next);
            } else {
                next = OutboxBackpressureState.THROTTLED;
            }
        } else {
            if (pending >= properties.getThrottleThreshold()) {
                next = OutboxBackpressureState.THROTTLED;
                log.warn("OUTBOX BACKPRESSURE ACTIVATED: Pending count {} reached throttle threshold {}. Inbound admission suspended.",
                        pending, properties.getThrottleThreshold());
            } else if (pending >= properties.getCriticalThreshold()) {
                next = OutboxBackpressureState.CRITICAL;
                log.warn("OUTBOX BACKPRESSURE CRITICAL: Pending count {} reached critical threshold {}.",
                        pending, properties.getCriticalThreshold());
            } else if (pending >= properties.getWarningThreshold()) {
                next = OutboxBackpressureState.WARNING;
                log.info("OUTBOX BACKPRESSURE WARNING: Pending count {} reached warning threshold {}.",
                        pending, properties.getWarningThreshold());
            } else {
                next = OutboxBackpressureState.HEALTHY;
            }
        }

        if (previous != next) {
            log.info("Outbox Backpressure State changed: {} -> {} (pending: {})", previous, next, pending);
            currentState.set(next);
        }

        if (dftpMetrics != null) {
            dftpMetrics.setBackpressureState(next.getCode());
        }

        return next;
    }

    public boolean isAdmissionAllowed() {
        if (!properties.isEnabled()) {
            return true;
        }
        return currentState.get() != OutboxBackpressureState.THROTTLED;
    }

    public void checkAdmission() {
        if (!properties.isEnabled()) {
            return;
        }

        // Fast path check
        OutboxBackpressureState state = currentState.get();
        if (state == OutboxBackpressureState.THROTTLED) {
            if (dftpMetrics != null) {
                dftpMetrics.recordBackpressureRejection();
            }
            throw new OutboxAdmissionThrottledException(cachedPendingCount, properties.getRetryAfterSeconds());
        }
    }

    public OutboxBackpressureState getCurrentState() {
        return currentState.get();
    }

    public long getCachedPendingCount() {
        return cachedPendingCount;
    }

    /**
     * Testing / Administrative override to force a state check immediately.
     */
    public OutboxBackpressureState forceRefresh() {
        return refreshState();
    }
}
