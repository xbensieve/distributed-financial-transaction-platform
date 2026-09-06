package com.dftp.transaction.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DltReplayService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private static final String REPLAY_COUNT_HEADER = "x-dlt-replay-count";
    private static final int MAX_REPLAY_COUNT = 3;

    public int replayDlt(String sourceTopic, String targetTopic) {
        return replayDlt(sourceTopic, targetTopic, 50);
    }

    public int replayDlt(String sourceTopic, String targetTopic, int maxRecords) {
        int boundedBatch = Math.min(Math.max(maxRecords, 1), 100);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "dlt-replay-consumer-group");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.poll.records", String.valueOf(boundedBatch));

        int replayed = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(sourceTopic));

            // Poll once to get the current batch
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            if (records.isEmpty()) {
                log.info("No records found in DLT topic {}", sourceTopic);
                return 0;
            }

            for (ConsumerRecord<String, String> record : records) {
                int replayCount = 0;
                Header existingHeader = record.headers().lastHeader(REPLAY_COUNT_HEADER);
                if (existingHeader != null) {
                    replayCount = Integer.parseInt(new String(existingHeader.value()));
                }

                if (replayCount >= MAX_REPLAY_COUNT) {
                    log.error("Message {} exceeded max replay count of {}. Parking message.", record.key(), MAX_REPLAY_COUNT);
                    // In a full implementation, we might send this to a 'parked' database table.
                    continue; // Skip replaying
                }

                int newReplayCount = replayCount + 1;
                
                org.springframework.messaging.support.MessageBuilder<String> messageBuilder = 
                        org.springframework.messaging.support.MessageBuilder
                        .withPayload(record.value())
                        .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC, targetTopic)
                        .setHeader(org.springframework.kafka.support.KafkaHeaders.KEY, record.key())
                        .setHeader(REPLAY_COUNT_HEADER, String.valueOf(newReplayCount).getBytes());
                
                // Copy original headers, omitting the old replay count
                for (Header header : record.headers()) {
                    if (!header.key().equals(REPLAY_COUNT_HEADER)) {
                        messageBuilder.setHeader(header.key(), header.value());
                    }
                }

                kafkaTemplate.send(messageBuilder.build());
                replayed++;
                log.info("Replayed message {} from {} to {}. Replay count: {}", record.key(), sourceTopic, targetTopic, newReplayCount);
            }

            consumer.commitSync();
            log.info("Successfully committed offsets for {} replayed messages", replayed);
        }

        return replayed;
    }
}
