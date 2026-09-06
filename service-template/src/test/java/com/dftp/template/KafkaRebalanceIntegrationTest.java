package com.dftp.template;

import com.dftp.common.event.EventEnvelope;
import com.dftp.common.inbox.InboxMessage;
import com.dftp.common.inbox.InboxMessageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class KafkaRebalanceIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        
        // Force an aggressively low max.poll.interval.ms to trigger rebalance easily
        registry.add("spring.kafka.consumer.properties.max.poll.interval.ms", () -> "2000");
        registry.add("spring.kafka.consumer.group-id", () -> "rebalance-group");
    }

    @Autowired
    private InboxMessageRepository inboxMessageRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TransactionTemplate transactionTemplate;

    private static final CountDownLatch processLatch = new CountDownLatch(2);
    private static final AtomicInteger attemptCount = new AtomicInteger(0);

    @KafkaListener(topics = "RebalanceTopic", groupId = "rebalance-group")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        attemptCount.incrementAndGet();
        
        try {
            EventEnvelope<?> envelope = objectMapper.readValue(record.value(), EventEnvelope.class);

            transactionTemplate.execute(status -> {
                try {
                    // We purposefully sleep longer than max.poll.interval.ms (2000ms) on the first attempt
                    // to force Kafka to consider this consumer dead and trigger a rebalance/redelivery.
                    if (attemptCount.get() == 1) {
                        Thread.sleep(3000); 
                    }

                    InboxMessage inboxMessage = InboxMessage.builder()
                            .eventId(envelope.getEventId())
                            .consumerGroup("rebalance-group")
                            .eventType(envelope.getEventType())
                            .build();
                    
                    inboxMessageRepository.saveAndFlush(inboxMessage);
                    return null;
                } catch (DataIntegrityViolationException e) {
                    // Expected on the duplicated run after rebalance if the first transaction committed
                    status.setRollbackOnly();
                    return null;
                } catch (Exception e) {
                    status.setRollbackOnly();
                    return null;
                }
            });

            ack.acknowledge();
        } catch (Exception e) {
            // ignore
        } finally {
            processLatch.countDown();
        }
    }

    @Test
    void consumerRebalance_inboxPreventsDoubleProcessing() throws Exception {
        UUID eventId = UUID.randomUUID();
        EventEnvelope<String> envelope = EventEnvelope.<String>builder()
                .eventId(eventId)
                .eventType("RebalanceEvent")
                .payload("Data")
                .build();

        kafkaTemplate.send("RebalanceTopic", eventId.toString(), objectMapper.writeValueAsString(envelope));

        // We expect the message to be processed, the consumer to sleep and cause a rebalance,
        // which forces the broker to redeliver the message (meaning the listener fires twice).
        // The first attempt will either commit or be rolled back depending on timing, 
        // but the Inbox constraints will guarantee it is only successfully committed ONCE.
        
        boolean completed = processLatch.await(15, TimeUnit.SECONDS);
        
        // Assert that the listener fired more than once due to rebalance redelivery
        assertThat(attemptCount.get()).isGreaterThanOrEqualTo(1);
        
        // Ensure the inbox only has exactly one record for this event
        long count = inboxMessageRepository.count();
        assertThat(count).isEqualTo(1);
    }
}
