package com.dftp.account;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"com.dftp.account", "com.dftp.common"})
@EntityScan(basePackages = {"com.dftp.account.domain", "com.dftp.common.outbox", "com.dftp.common.inbox"})
@EnableJpaRepositories(basePackages = {"com.dftp.account.domain", "com.dftp.common.outbox", "com.dftp.common.inbox"})
public class AccountServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountServiceApplication.class, args);
    }
}
