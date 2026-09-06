package com.dftp.transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.dftp.transaction", "com.dftp.common"})
@EntityScan(basePackages = {"com.dftp.transaction.domain", "com.dftp.transaction.security", "com.dftp.common.outbox", "com.dftp.common.inbox"})
@EnableJpaRepositories(basePackages = {"com.dftp.transaction.domain", "com.dftp.transaction.security", "com.dftp.common.outbox", "com.dftp.common.inbox"})
@EnableScheduling
public class TransactionServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(TransactionServiceApplication.class, args);
    }
}
