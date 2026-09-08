package com.dftp.transaction.backpressure;

/**
 * Operational states for Outbox storage monitoring and inbound admission control.
 */
public enum OutboxBackpressureState {
    HEALTHY(0),
    WARNING(1),
    CRITICAL(2),
    THROTTLED(3);

    private final int code;

    OutboxBackpressureState(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
