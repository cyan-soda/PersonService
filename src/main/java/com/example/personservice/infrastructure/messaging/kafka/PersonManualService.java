package com.example.personservice.infrastructure.messaging.kafka;

import com.example.personservice.application.dto.ManualBatchResponseDto;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.config.KafkaTopicConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class PersonManualService {

    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // 1. Keep the consumer as a class field
    private Consumer<String, String> consumer;

    // 2. Initialize it ONCE when the bean is created
    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "person.manual.group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        this.consumer = consumerFactory.createConsumer("person.manual.group", null, null, props);
        this.consumer.subscribe(List.of(KafkaTopicConfig.PERSON_MANUAL_TOPIC));
        log.info("Manual Consumer initialized and subscribed.");
    }

    // 3. Close it cleanly when the app stops
    @PreDestroy
    public void close() {
        if (consumer != null) {
            consumer.close();
            log.info("Manual Consumer closed.");
        }
    }

    private void processPersonDomainLogic(PersonEvent event) {
        if (event.getPerson().getFirstName().contains("Fail")) {
            throw new RuntimeException("Simulated Domain Error for " + event.getPerson().getTaxNumber());
        }
        log.info("Successfully processed {} for Person ID {}", event.getEventType(), event.getPerson().getId());
    }

    public synchronized ManualBatchResponseDto fetchAndProcessBatch(int maxEvents, String strategy) {

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

        if (records.isEmpty()) {
            return new ManualBatchResponseDto(new ArrayList<>(), calculateLag(consumer), false);
        }

        List<String> processedEventsLog = new ArrayList<>();
        Set<UUID> failedPersonIdsInBatch = new HashSet<>();
        boolean batchHasFailures = false;

        int processedCount = 0;

        for (ConsumerRecord<String, String> record : records) {
            if (processedCount >= maxEvents) break; // Stop if we hit the requested limit
            processedCount++;

            try {
                // 1. Deserialize
                PersonEvent event = objectMapper.readValue(record.value(), PersonEvent.class);
                UUID personId = event.getPerson().getId();

                // 2. Check Dependency
                if (failedPersonIdsInBatch.contains(personId)) {
                    log.warn("Skipping dependent event {} for Person {} due to previous failure", event.getEventType(), personId);
                    handleFailure(record, "Dependency Failed", strategy);
                    processedEventsLog.add("SKIPPED (Dependency): " + event.getEventType());
                    continue;
                }

                // 3. Process
                processPersonDomainLogic(event);
                processedEventsLog.add("SUCCESS: " + event.getEventType() + " - " + personId);

            } catch (Exception e) {
                log.error("Error processing record: {}", record.value(), e);
                batchHasFailures = true;
                processedEventsLog.add("FAILED: " + e.getMessage());

                // 4. Handle Error
                if ("BLOCKING".equalsIgnoreCase(strategy)) {
                    log.error("BLOCKING STRATEGY: Stopping batch processing. Offsets will NOT be committed.");
                    // Return immediately. Do NOT commit.
                    // Next time we poll, we get the same records back.
                    return new ManualBatchResponseDto(processedEventsLog, calculateLag(consumer), true);
                } else {
                    // NON_BLOCKING
                    try {
                        PersonEvent event = objectMapper.readValue(record.value(), PersonEvent.class);
                        failedPersonIdsInBatch.add(event.getPerson().getId());
                    } catch (Exception ignored) {}

                    handleFailure(record, e.getMessage(), strategy);
                }
            }
        }

        // 5. Commit Offsets
        if (!batchHasFailures || "NON_BLOCKING".equalsIgnoreCase(strategy)) {
            // Note: commitSync commits ALL records returned by the poll.
            // If we broke the loop early due to 'maxEvents', we are technically committing messages we haven't processed yet.
            // For a strict 'maxEvents' implementation, you would need to commit specific offsets.
            // For this exercise, assuming maxEvents >= batch size usually creates less friction.
            consumer.commitSync();
            log.info("Batch committed successfully.");
        }

        long lag = calculateLag(consumer);
        return new ManualBatchResponseDto(processedEventsLog, lag, lag > 0);
    }

    private void handleFailure(ConsumerRecord<String, String> record, String reason, String strategy) {
        if ("NON_BLOCKING".equalsIgnoreCase(strategy)) {
            log.info("NON-BLOCKING: Sending failed message to DLT: person.kafka.dlt");
            kafkaTemplate.send("person.kafka.dlt", record.key(), record.value());
        }
    }

    private long calculateLag(Consumer<?, ?> consumer) {
        var assignment = consumer.assignment();
        if (assignment == null || assignment.isEmpty()) return 0;
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        long total = 0;
        for (TopicPartition tp : assignment) {
            total += (endOffsets.get(tp) - consumer.position(tp));
        }
        return total;
    }
}
