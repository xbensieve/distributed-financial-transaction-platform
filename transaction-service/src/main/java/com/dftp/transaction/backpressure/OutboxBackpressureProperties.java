package com.dftp.transaction.backpressure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "dftp.outbox.backpressure")
public class OutboxBackpressureProperties {

    /**
     * Whether admission control based on Outbox backlog is enabled.
     */
    private boolean enabled = true;

    /**
     * Warning threshold for pending outbox events (default: 20,000).
     */
    private long warningThreshold = 20000;

    /**
     * Critical threshold for pending outbox events (default: 50,000).
     * Alerts operators when reached.
     */
    private long criticalThreshold = 50000;

    /**
     * Throttling threshold at which inbound transactions are rejected (default: 50,000).
     */
    private long throttleThreshold = 50000;

    /**
     * Recovery threshold for deactivating throttling (hysteresis).
     * Throttling remains active until pending outbox count drops below this value.
     */
    private long recoveryThreshold = 20000;

    /**
     * Recommended Retry-After header duration in seconds when rejecting requests.
     */
    private int retryAfterSeconds = 30;
}
