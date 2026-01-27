package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.ErrorClassifier;
import com.example.personservice.infrastructure.messaging.redis.PersonEventBufferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonBatchConsumer {

    private final PersonService personService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ErrorClassifier errorClassifier;
    private final PersonEventBufferService bufferService;

    @KafkaListener(
            topics = "person.kafka.batch",
            containerFactory = "personBatchContainerFactory",
            groupId = "person.batch.group"
    )
    public void consumeBatch(List<ConsumerRecord<String, PersonEvent>> records, Acknowledgment ack) {
        log.info("[Batch] Received batch of size: {}", records.size());

        try {
            Map<String, List<PersonEvent>> groupedEvents = records.stream()
                    .collect(Collectors.groupingBy(
                            ConsumerRecord::key,
                            LinkedHashMap::new,
                            Collectors.mapping(ConsumerRecord::value, Collectors.toList())
                    ));

            for (Map.Entry<String, List<PersonEvent>> entry : groupedEvents.entrySet()) {
                String taxNumber = entry.getKey();
                List<PersonEvent> events = entry.getValue();

                try {
                    if (bufferService.isKeyInRetry(taxNumber)) {
                        log.info("[Batch] Key {} is in RETRY state. Buffering {} new event(s).", taxNumber, events.size());
                        bufferService.bufferEvents(taxNumber, events);
                        continue;
                    }

                    // Process events sequentially for the current key
                    for (PersonEvent event : events) {
                        try {
                            processEvent(event);
                            log.info("[Batch] Successfully processed event for key {}.", taxNumber);
                        } catch (Exception e) {
                            log.error("[Batch] Error processing event for key {}. Initiating retry flow.", taxNumber, e);
                            handleProcessingError(taxNumber, event, e);

                            int failedEventIndex = events.indexOf(event);
                            List<PersonEvent> remainingEvents = events.subList(failedEventIndex + 1, events.size());
                            if (!remainingEvents.isEmpty()) {
                                log.info("[Batch] Buffering {} subsequent event(s) for key {} due to failure.", remainingEvents.size(), taxNumber);
                                bufferService.bufferEvents(taxNumber, remainingEvents);
                            }

                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("[Batch] UNEXPECTED error while processing key {}. Skipping to next key.", taxNumber, e);
                }
            }
        } finally {
            ack.acknowledge();
            log.info("[Batch] Acknowledged batch of size: {}.", records.size());
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

    private void handleProcessingError(String taxNumber, PersonEvent event, Exception e) {
        ErrorClassifier.ErrorType type = errorClassifier.classifyError(e);

        if (type == ErrorClassifier.ErrorType.FATAL) {
            log.error("[Batch] FATAL error for key {}. Sending to DLT and marking key as DLT.", taxNumber);
            kafkaTemplate.send("person.kafka.dlt", taxNumber, event);
            bufferService.markAsDlt(taxNumber);
        } else {
            log.warn("[Batch] RETRYABLE error for key {}. Sending to Retry-1 and marking key for retry.", taxNumber);
            kafkaTemplate.send("person.kafka.retry-1", taxNumber, event);
            bufferService.markAsRetrying(taxNumber);
        }
    }
}
