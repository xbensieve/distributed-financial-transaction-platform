package com.dftp.account;

import com.dftp.account.api.dto.AccountResponse;
import com.dftp.account.api.dto.CreateAccountRequest;
import com.dftp.account.domain.AccountRepository;
import com.dftp.common.inbox.InboxMessageRepository;
import com.dftp.common.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AccountIntegrationTest extends AbstractAccountIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private InboxMessageRepository inboxMessageRepository;

    @Autowired
    private com.dftp.common.security.JwtTokenService jwtTokenService;

    @BeforeEach
    void setUp() {
        inboxMessageRepository.deleteAll();
        outboxEventRepository.deleteAll();
        accountRepository.deleteAll();

        String token = jwtTokenService.generateToken("account-test-user", java.util.List.of("ROLE_USER"));
        restTemplate.getRestTemplate().getInterceptors().clear();
        restTemplate.getRestTemplate().getInterceptors().add((req, body, exec) -> {
            req.getHeaders().set(org.springframework.http.HttpHeaders.AUTHORIZATION, "Bearer " + token);
            return exec.execute(req, body);
        });
    }

    @Test
    void shouldCreateAccountAndProcessEventIdempotently() {
        String transactionId = UUID.randomUUID().toString();
        CreateAccountRequest request = new CreateAccountRequest(transactionId);

        // 1. HTTP Request -> Account Creation & Outbox Event
        ResponseEntity<AccountResponse> response1 = restTemplate.postForEntity("/accounts", request, AccountResponse.class);
        
        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response1.getBody()).isNotNull();
        UUID accountId = response1.getBody().getAccountId();
        
        // Verify DB State
        assertThat(accountRepository.findById(accountId)).isPresent();
        assertThat(outboxEventRepository.findAll().stream()
                .filter(e -> "AccountCreated".equals(e.getEventType()) && accountId.toString().equals(e.getAggregateId()))
                .count()).isEqualTo(1);
        
        // 2. HTTP Idempotency -> Second request with same transactionId should return same account, not create new event
        ResponseEntity<AccountResponse> response2 = restTemplate.postForEntity("/accounts", request, AccountResponse.class);
        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response2.getBody().getAccountId()).isEqualTo(accountId);
        assertThat(outboxEventRepository.findAll().stream()
                .filter(e -> "AccountCreated".equals(e.getEventType()) && accountId.toString().equals(e.getAggregateId()))
                .count()).isEqualTo(1); // No new outbox event

        UUID createdEventId = outboxEventRepository.findAll().stream()
                .filter(e -> "AccountCreated".equals(e.getEventType()) && accountId.toString().equals(e.getAggregateId()))
                .findFirst().get().getId();

        // 3. Outbox Relay -> Kafka -> Consumer -> Inbox Deduplication
        // Wait for event to be processed
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(inboxMessageRepository.findById(createdEventId)).isPresent();
        });
        
        // Verify outbox status was updated to PUBLISHED
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(outboxEventRepository.findById(createdEventId).get().getStatus()).isEqualTo("PUBLISHED");
        });
    }
}
