package com.dftp.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"com.dftp.ledger", "com.dftp.common"})
@EntityScan(basePackages = {"com.dftp.ledger", "com.dftp.common"})
@EnableJpaRepositories(basePackages = {"com.dftp.ledger", "com.dftp.common"})
public class LedgerServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LedgerServiceApplication.class, args);
    }
}
