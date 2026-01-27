package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.RetryRouter;
import com.example.personservice.infrastructure.messaging.redis.PersonEventBufferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonBatchRetryConsumer {

    private final PersonService personService;
    private final PersonEventBufferService bufferService;
    private final RetryRouter retryRouter;

    @KafkaListener(
            topics = {"person.kafka.retry-1", "person.kafka.retry-2", "person.kafka.retry-3"},
            groupId = "person.retry.group",
            containerFactory = "personBatchContainerFactory"
    )
    public void consumeRetry(List<ConsumerRecord<String, PersonEvent>> records, Acknowledgment acknowledgment) {
        log.info("[RetryWorker] Received batch of size: {}", records.size());

        for (ConsumerRecord<String, PersonEvent> record : records) {
            String taxNumber = record.key();
            PersonEvent event = record.value();
            int retryCount = extractRetryCount(record);

            log.info("[RetryWorker] Processing key {} from topic {} (Attempt {})", taxNumber, record.topic(), retryCount);

            try {
                processEvent(event);
                log.info("[RetryWorker] SUCCESS on retried event for key {}. Checking for buffered events.", taxNumber);

                processBufferedEvents(taxNumber);

            } catch (Exception e) {
                log.error("[RetryWorker] FAILED attempt {} for key {}: {}", retryCount, taxNumber, e.getMessage());
                retryRouter.routeToNextTopic(event, taxNumber, retryCount);
            }
        }
        acknowledgment.acknowledge();
        log.info("[RetryWorker] Acknowledged batch of {} records.", records.size());
    }

    private void processBufferedEvents(String taxNumber) {
        while (true) {
            PersonEvent bufferedEvent = bufferService.popNextBufferedEvent(taxNumber);
            if (bufferedEvent == null) {
                log.info("[RetryWorker] No more buffered events for key {}. Clearing retry state.", taxNumber);
                bufferService.clearRetryState(taxNumber);
                break;
            }

            log.info("[RetryWorker] Processing buffered event of type {} for key {}.", bufferedEvent.getEventType(), taxNumber);
            try {
                processEvent(bufferedEvent);
                log.info("[RetryWorker] Successfully processed buffered event for key {}.", taxNumber);
            } catch (Exception e) {
                log.error("[RetryWorker] FAILED to process a buffered event for key {}. Re-queueing to retry-1.", taxNumber, e);
                // This buffered event now becomes the primary failed event.
                // It's sent back to the start of the retry flow. The remaining buffered events stay in Redis.
                retryRouter.routeToNextTopic(bufferedEvent, taxNumber, 0); // Start its retry cycle
                break;
            }
        }
    }

    private void processEvent(PersonEvent event) {
        switch (event.getEventType()) {
            case CREATE -> personService.createPersonFromEvent(event.getPerson());
            case UPDATE -> personService.updatePersonFromEvent(event.getPerson());
            case DELETE -> personService.deletePersonFromEvent(event.getPerson());
            default -> throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
        }
    }

    private int extractRetryCount(ConsumerRecord<String, PersonEvent> record) {
        var header = record.headers().lastHeader("retry-count");
        if (header != null) {
            try {
                return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                log.warn("[RetryWorker] Could not parse 'retry-count' header. Defaulting to 1.");
            }
        }
        return 1; // Default if coming from main topic
    }
}
