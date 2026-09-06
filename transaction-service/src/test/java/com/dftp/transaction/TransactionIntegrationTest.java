package com.dftp.transaction;

import com.dftp.common.event.EventEnvelope;
import com.dftp.transaction.api.dto.CreateTransactionRequest;
import com.dftp.transaction.api.dto.TransactionResponse;
import com.dftp.transaction.domain.AccountReference;
import com.dftp.transaction.domain.AccountReferenceRepository;
import com.dftp.transaction.domain.event.AccountCreated;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TransactionIntegrationTest extends AbstractTransactionIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountReferenceRepository accountReferenceRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private com.dftp.common.security.JwtTokenService jwtTokenService;

    @LocalServerPort
    private int port;

    private KafkaConsumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        accountReferenceRepository.deleteAll();

        String token = jwtTokenService.generateToken("tx-test-user", java.util.List.of("ROLE_USER"));
        restTemplate.getRestTemplate().getInterceptors().clear();
        restTemplate.getRestTemplate().getInterceptors().add((req, body, exec) -> {
            req.getHeaders().set(org.springframework.http.HttpHeaders.AUTHORIZATION, "Bearer " + token);
            return exec.execute(req, body);
        });

        // Set up a raw Kafka consumer against the shared Testcontainers Kafka
        testConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-transaction-group-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ));
        testConsumer.subscribe(Collections.singletonList("transaction-events"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    void testEndToEndEventualConsistencyAndTransactionCreation() throws Exception {
        UUID accountId = UUID.randomUUID();
        
        // 1. Simulate Account Creation Event
        AccountCreated accountCreated = AccountCreated.builder()
                .accountId(accountId)
                .ownerId("tx-test-user")
                .status("ACTIVE")
                .createdAt(Instant.now())
                .build();
                
        EventEnvelope<AccountCreated> envelope = EventEnvelope.<AccountCreated>builder()
                .eventId(UUID.randomUUID())
                .eventType("AccountCreated")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .correlationId(UUID.randomUUID().toString())
                .producerService("account-service")
                .payload(accountCreated)
                .build();
                
        kafkaTemplate.send("account-events", accountId.toString(), objectMapper.writeValueAsString(envelope));
        
        // Wait for eventual consistency projection to update
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> 
            assertThat(accountReferenceRepository.findById(accountId)).isPresent()
        );

        UUID destAccountId = UUID.randomUUID();
        AccountCreated destAccountCreated = AccountCreated.builder()
                .accountId(destAccountId)
                .status("ACTIVE")
                .createdAt(Instant.now())
                .build();
                
        EventEnvelope<AccountCreated> destEnvelope = EventEnvelope.<AccountCreated>builder()
                .eventId(UUID.randomUUID())
                .eventType("AccountCreated")
                .eventVersion("1.0")
                .occurredAt(Instant.now())
                .correlationId(UUID.randomUUID().toString())
                .producerService("account-service")
                .payload(destAccountCreated)
                .build();
                
        kafkaTemplate.send("account-events", destAccountId.toString(), objectMapper.writeValueAsString(destEnvelope));
        
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> 
            assertThat(accountReferenceRepository.findById(destAccountId)).isPresent()
        );

        // 2. Create Transaction
        String transactionId = "TXN-" + UUID.randomUUID();
        CreateTransactionRequest request = CreateTransactionRequest.builder()
                .transactionId(transactionId)
                .sourceAccountId(accountId)
                .destinationAccountId(destAccountId)
                .amount(new BigDecimal("150.00"))
                .currency("USD")
                .build();

        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        org.springframework.http.HttpEntity<CreateTransactionRequest> entity = new org.springframework.http.HttpEntity<>(request, headers);

        ResponseEntity<TransactionResponse> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/transactions", entity, TransactionResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getTransactionId()).isEqualTo(transactionId);
        assertThat(response.getBody().getStatus()).isEqualTo("PENDING");

        // 3. Verify Idempotency (Duplicate Request)
        ResponseEntity<TransactionResponse> duplicateResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/transactions", entity, TransactionResponse.class);

        assertThat(duplicateResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(duplicateResponse.getBody().getId()).isEqualTo(response.getBody().getId());

        // 4. Verify Outbox Event Published to Kafka
        java.util.List<ConsumerRecord<String, String>> allRecords = new java.util.ArrayList<>();
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofMillis(200));
            records.forEach(allRecords::add);
            
            boolean found = false;
            for (ConsumerRecord<String, String> record : allRecords) {
                EventEnvelope<Map<String, Object>> txEnvelope = objectMapper.readValue(
                        record.value(), new TypeReference<>() {}
                );
                if ("FundsHoldRequested".equals(txEnvelope.getEventType()) && 
                    transactionId.equals(txEnvelope.getTransactionId())) {
                    found = true;
                    break;
                }
            }
            assertThat(found).as("Expected FundsHoldRequested event not found for transaction " + transactionId).isTrue();
        });
    }

    @Test
    void testTransactionFailsIfAccountUnknown() {
        CreateTransactionRequest request = CreateTransactionRequest.builder()
                .transactionId("TXN-" + UUID.randomUUID())
                .sourceAccountId(UUID.randomUUID()) // Unknown account
                .destinationAccountId(UUID.randomUUID())
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .build();

        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        org.springframework.http.HttpEntity<CreateTransactionRequest> entity = new org.springframework.http.HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/transactions", entity, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
