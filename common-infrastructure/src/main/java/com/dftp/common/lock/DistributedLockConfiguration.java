package com.dftp.common.lock;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Distributed Scheduled Locking Configuration (P13 / ADR-013).
 * Provides PostgreSQL-backed distributed locking for singleton background tasks:
 * - ReconciliationScannerService
 * - StalledSagaRecoveryService
 * - DataRetentionPurgeService
 *
 * Utilizes database clock (`usingDbTime()`) to ensure complete resilience
 * against container pod clock skew, and guarantees lock release on ungraceful pod termination
 * via atomic TTL expiration (`lockAtMostFor`).
 */
@Configuration
@ConditionalOnClass({LockProvider.class, JdbcTemplate.class})
@ConditionalOnProperty(name = "dftp.shedlock.enabled", havingValue = "true", matchIfMissing = false)
@EnableSchedulerLock(defaultLockAtMostFor = "PT10M")
public class DistributedLockConfiguration {

    @Bean
    public LockProvider lockProvider(DataSource dataSource) {
        return new JdbcTemplateLockProvider(
                JdbcTemplateLockProvider.Configuration.builder()
                        .withJdbcTemplate(new JdbcTemplate(dataSource))
                        .usingDbTime() // Uses PostgreSQL NOW() to prevent multi-pod clock skew drift
                        .build()
        );
    }
}
