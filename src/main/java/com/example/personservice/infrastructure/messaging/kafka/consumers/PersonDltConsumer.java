package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonDltConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ConsumerFactory<String, PersonEvent> consumerFactory;
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicLong totalRecoveredMessages = new AtomicLong(0);

    private static final String DLT_TOPIC = "person.kafka.dlt";
    private static final String MAIN_TOPIC = "person.kafka.batch";
    private static final String RECOVERY_HEADER = "dlt-recovery-attempt";
    private static final String RECOVERY_TIMESTAMP_HEADER = "dlt-recovery-timestamp";
    private static final int MAX_RECOVERY_ATTEMPTS = 3;

    @Scheduled(fixedRate = 300000) // 5 minutes
    public void scheduledDltRecovery() {
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("[DLT-Recovery] Previous recovery process still running, skipping this execution");
            return;
        }

        try {
            log.info("[DLT-Recovery] Starting scheduled DLT recovery process at {}",
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

            processMessagesFromDlt();

        } catch (Exception e) {
            log.error("[DLT-Recovery] Error during DLT recovery process", e);
        } finally {
            isProcessing.set(false);
        }
    }

    private void processMessagesFromDlt() {
        Consumer<String, PersonEvent> consumer = null;

        try {
            // Create a consumer specifically for DLT recovery
            consumer = consumerFactory.createConsumer("person-dlt-recovery-" + System.currentTimeMillis(), "");
            consumer.subscribe(Collections.singletonList(DLT_TOPIC));

            log.info("[DLT-Recovery] Polling DLT topic for messages...");

            // Poll for messages with a timeout
            ConsumerRecords<String, PersonEvent> records = consumer.poll(Duration.ofSeconds(10));

            if (records.isEmpty()) {
                log.info("[DLT-Recovery] No messages found in DLT topic");
                return;
            }

            log.info("[DLT-Recovery] Found {} messages in DLT topic", records.count());

            int successCount = 0;
            int failureCount = 0;
            int skippedCount = 0;

            for (ConsumerRecord<String, PersonEvent> record : records) {
                try {
                    String taxNumber = record.key();
                    PersonEvent event = record.value();

                    // Check recovery attempts
                    int recoveryAttempts = getRecoveryAttempts(record);

                    if (recoveryAttempts >= MAX_RECOVERY_ATTEMPTS) {
                        log.warn("[DLT-Recovery] Max recovery attempts ({}) reached for taxNumber: {}. Skipping.",
                                MAX_RECOVERY_ATTEMPTS, taxNumber);
                        skippedCount++;
                        continue;
                    }

                    log.info("[DLT-Recovery] Reprocessing event for taxNumber: {}, eventType: {}, attempt: {}",
                            taxNumber, event.getEventType(), recoveryAttempts + 1);

                    // Create producer record with recovery headers
                    ProducerRecord<String, Object> producerRecord =
                            new ProducerRecord<>(MAIN_TOPIC, taxNumber, event);

                    // Add recovery tracking headers
                    producerRecord.headers().add(RECOVERY_HEADER,
                            String.valueOf(recoveryAttempts + 1).getBytes());
                    producerRecord.headers().add(RECOVERY_TIMESTAMP_HEADER,
                            LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).getBytes());

                    // Send the message back to the main topic
                    kafkaTemplate.send(producerRecord);

                    successCount++;
                    totalRecoveredMessages.incrementAndGet();

                    log.debug("[DLT-Recovery] Successfully sent message back to main topic for taxNumber: {}", taxNumber);

                } catch (Exception e) {
                    failureCount++;
                    log.error("[DLT-Recovery] Failed to reprocess message for taxNumber: {}, error: {}",
                            record.key(), e.getMessage(), e);
                }
            }

            // Commit the offsets manually
            consumer.commitSync();

            log.info("[DLT-Recovery] Completed DLT recovery. Success: {}, Failures: {}, Skipped: {}, Total recovered: {}",
                    successCount, failureCount, skippedCount, totalRecoveredMessages.get());

        } catch (Exception e) {
            log.error("[DLT-Recovery] Error during DLT message processing", e);
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    log.error("[DLT-Recovery] Error closing consumer", e);
                }
            }
        }
    }

    private int getRecoveryAttempts(ConsumerRecord<String, PersonEvent> record) {
        var header = record.headers().lastHeader(RECOVERY_HEADER);
        if (header != null) {
            try {
                return Integer.parseInt(new String(header.value()));
            } catch (NumberFormatException e) {
                log.warn("[DLT-Recovery] Could not parse recovery attempt header for key: {}", record.key());
            }
        }
        return 0;
    }
}
