package com.dftp.ledger;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class LedgerServiceApplicationTests extends AbstractLedgerIntegrationTest {

    @Test
    void contextLoadsAndFlywayExecutes() {
        // If the context loads successfully, it proves:
        // 1. Spring Boot is configured correctly
        // 2. PostgreSQL Testcontainer started
        // 3. Kafka Testcontainer started
        // 4. Flyway executed the V1__init_outbox_inbox.sql migration
    }
}
