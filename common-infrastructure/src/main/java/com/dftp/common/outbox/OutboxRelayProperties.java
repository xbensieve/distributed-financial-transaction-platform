package com.dftp.common.outbox;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "dftp.outbox.relay")
public class OutboxRelayProperties {
    /**
     * Maximum number of events to claim per polling cycle.
     */
    private int batchSize = 100;
}
