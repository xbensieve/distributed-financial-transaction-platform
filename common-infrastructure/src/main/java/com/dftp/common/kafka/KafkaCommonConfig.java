package com.dftp.common.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaCommonConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxInterval(10000L);
        backOff.setMaxAttempts(3);
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // Non-retryable permanent exceptions: malformed payloads, validation failures, schema parse errors
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                com.fasterxml.jackson.core.JsonProcessingException.class
        );
        // Explicitly retryable transient exceptions (optimistic locking, DB contention)
        errorHandler.addRetryableExceptions(
                org.springframework.orm.ObjectOptimisticLockingFailureException.class,
                org.springframework.dao.CannotAcquireLockException.class,
                org.springframework.dao.TransientDataAccessException.class
        );
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // Manual immediate acknowledgment commits the offset immediately after message transaction processing
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }
}
