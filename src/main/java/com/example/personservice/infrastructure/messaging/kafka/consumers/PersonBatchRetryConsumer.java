package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.RetryRouter;
import com.example.personservice.infrastructure.messaging.redis.RetryLatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonBatchRetryConsumer {

    private final PersonService service;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RetryLatch retryLatch;
    private final RetryRouter router;

    private static final int MAX_RETRIES = 3;

    @KafkaListener(
            topics = {"person.kafka.retry-1", "person.kafka.retry-2", "person.kafka.retry-3"},
            groupId = "person.retry.group",
            containerFactory = "personBatchContainerFactory"
    )
    public void consumeRetry(List<ConsumerRecord<String, PersonEvent>> records,
                             Acknowledgment acknowledgment) {

        log.info("[RetryWorker][Batch] Received batch of size: {}", records.size());

        for (ConsumerRecord<String, PersonEvent> record : records) {
            String taxNumber = record.key();
            PersonEvent event = record.value();

            // 1. Extract retry-count from the individual record's header
            int retryCount = extractRetryCount(record, taxNumber);

            log.info("[RetryWorker] Processing {} from topic {} (Attempt {})",
                    taxNumber, record.topic(), retryCount);

            try {
                // 2. Process each event individually
                processEvent(event);

                // On success, notify the original consumer via Redis
                log.info("[RetryWorker] Success for {}. Notifying Batch Consumer.", taxNumber);
                retryLatch.notifyResult(taxNumber, RetryLatch.RetryStatus.SUCCESS);

            } catch (Exception e) {
                log.error("[RetryWorker] Failed attempt {} for {}: {}", retryCount, taxNumber, e.getMessage());

                // 3. On failure, decide whether to retry again or send to DLT
                handleRetryFailure(event, taxNumber, retryCount);
            }
        }

        // 4. Acknowledge the entire batch after processing all records
        acknowledgment.acknowledge();
        log.info("[RetryWorker][Batch] Acknowledged batch of {} records.", records.size());
    }

    private int extractRetryCount(ConsumerRecord<String, PersonEvent> record, String taxNumber) {
        int retryCount = 1;
        Header retryHeader = record.headers().lastHeader("retry-count");

        if (retryHeader != null) {
            try {
                retryCount = Integer.parseInt(new String(retryHeader.value(), StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                log.warn("[RetryWorker] Could not parse 'retry-count' header for key {}. Using default value 1.", taxNumber);
            }
        } else {
            log.warn("[RetryWorker] No 'retry-count' header found for key {}. Using default value 1.", taxNumber);
        }

        return retryCount;
    }

    private void handleRetryFailure(PersonEvent event, String taxNumber, int retryCount) {
        if (retryCount >= MAX_RETRIES) {
            log.warn("[RetryWorker] Max retries ({}) reached for {}. Sending to DLT.", MAX_RETRIES, taxNumber);
            router.sendToDlt(event, taxNumber);
            retryLatch.notifyResult(taxNumber, RetryLatch.RetryStatus.DLT);
        } else {
            log.info("[RetryWorker] Routing {} to next retry topic for attempt #{}.", taxNumber, retryCount + 1);
            router.routeToNextTopic(event, taxNumber, retryCount);
        }
    }

    private void processEvent(PersonEvent event) {
        switch (event.getEventType()) {
            case CREATE -> service.createPersonFromEvent(event.getPerson());
            case UPDATE -> service.updatePersonFromEvent(event.getPerson());
            case DELETE -> service.deletePersonFromEvent(event.getPerson());
        }
    }
}
