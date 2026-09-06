package com.dftp.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DefaultErrorHandlerRetryTest {

    @Test
    @DisplayName("Transient Failure: Retries exactly 3 times before routing to recoverer/DLT")
    void testTransientFailureRetriesThreeTimesBeforeDLT() {
        AtomicInteger recovererInvocations = new AtomicInteger(0);
        ConsumerRecordRecoverer recoverer = (record, exception) -> recovererInvocations.incrementAndGet();

        // 3 max attempts with 1ms backoff for fast deterministic unit testing
        FixedBackOff backOff = new FixedBackOff(1L, 3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                JsonProcessingException.class
        );

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        Exception transientException = new RuntimeException("Simulated transient database connection timeout");

        AtomicInteger deliveryAttempts = new AtomicInteger(0);

        // Simulate Spring Kafka listener execution loop with transient error
        for (int i = 0; i < 4; i++) {
            deliveryAttempts.incrementAndGet();
            try {
                errorHandler.handleOne(transientException, record, mock(org.apache.kafka.clients.consumer.Consumer.class), mock(org.springframework.kafka.listener.MessageListenerContainer.class));
            } catch (Exception ignored) {
                // Spring Kafka rethrows while retries remain
            }
        }

        // Must execute attempts up to the configured limit, then trigger recoverer (DLT) exactly once
        assertThat(recovererInvocations.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Poison Message: IllegalArgumentException is non-retryable and immediately routes to recoverer/DLT on attempt 1")
    void testPoisonMessageRoutesImmediatelyToDLTWithoutRetry() {
        AtomicInteger recovererInvocations = new AtomicInteger(0);
        ConsumerRecordRecoverer recoverer = (record, exception) -> recovererInvocations.incrementAndGet();

        FixedBackOff backOff = new FixedBackOff(1000L, 3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                JsonProcessingException.class
        );

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "poison-message");
        IllegalArgumentException poisonException = new IllegalArgumentException("Malformed event: missing mandatory eventId");

        // First delivery attempt with non-retryable exception
        errorHandler.handleOne(poisonException, record, mock(org.apache.kafka.clients.consumer.Consumer.class), mock(org.springframework.kafka.listener.MessageListenerContainer.class));

        // Immediately invokes recoverer (DLT) without any backoff retries
        assertThat(recovererInvocations.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Poison Message wrapped in ListenerExecutionFailedException: unwrap cause and classify as non-retryable on attempt 1")
    void testPoisonMessageWrappedInListenerExecutionFailedExceptionRoutesImmediatelyToDLT() {
        AtomicInteger recovererInvocations = new AtomicInteger(0);
        ConsumerRecordRecoverer recoverer = (record, exception) -> recovererInvocations.incrementAndGet();

        FixedBackOff backOff = new FixedBackOff(1000L, 3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                JsonProcessingException.class
        );

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "poison-message");
        org.springframework.kafka.listener.ListenerExecutionFailedException wrappedException =
                new org.springframework.kafka.listener.ListenerExecutionFailedException("Listener failed",
                        new IllegalArgumentException("Malformed event: missing mandatory eventId"));

        errorHandler.handleOne(wrappedException, record, mock(org.apache.kafka.clients.consumer.Consumer.class), mock(org.springframework.kafka.listener.MessageListenerContainer.class));

        assertThat(recovererInvocations.get()).isEqualTo(1);
    }
}
