package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.ErrorClassifier;
import com.example.personservice.infrastructure.messaging.redis.RetryLatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonBatchConsumer {

    private final PersonService personService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ErrorClassifier errorClassifier;
    private final RetryLatch retryLatch;

    // Thread pool for parallel processing of different keys within a batch
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    @KafkaListener(
            topics = "person.kafka.batch",
            containerFactory = "personBatchContainerFactory",
            groupId = "person.batch.group"
    )
    public void consumeBatch(
            List<ConsumerRecord<String, PersonEvent>> records,
            Acknowledgment ack
    ) {

        log.info("[Batch] Received batch of size: {}", records.size());

        // 1. Group records by key (TaxNumber) to preserve order PER KEY
        Map<String, List<ConsumerRecord<String, PersonEvent>>> groupedRecords =
                records.stream()
                        .collect(Collectors.groupingBy(
                                ConsumerRecord::key,
                                LinkedHashMap::new,
                                Collectors.toList()
                        ));

        // 2. Process each key in parallel
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, List<ConsumerRecord<String, PersonEvent>>> entry : groupedRecords.entrySet()) {
            String taxNumber = entry.getKey();
            List<ConsumerRecord<String, PersonEvent>> eventsForKey = entry.getValue();

            CompletableFuture<Void> future =
                    CompletableFuture.runAsync(
                            () -> processKeyEvents(taxNumber, eventsForKey),
                            executor
                    );

            futures.add(future);
        }

        // 3. Wait for ALL keys to finish before ACK
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("[Batch] All keys processed. Acknowledging batch.");
        ack.acknowledge();
    }

    private void processKeyEvents(
            String taxNumber,
            List<ConsumerRecord<String, PersonEvent>> events
    ) {
        retryLatch.clearSignal(taxNumber);

        boolean isRetrying = false;
        boolean isCriticalFailed = false;

        for (ConsumerRecord<String, PersonEvent> record : events) {
            PersonEvent event = record.value();

            if (isCriticalFailed) {
                log.warn(
                        "[Batch-Key:{}] Previous CREATE. Sending {} to DLT.",
                        taxNumber,
                        event.getEventType()
                );
                kafkaTemplate.send("person.kafka.dlt", taxNumber, event);
                continue;
            }

            // If already retrying, block this key until retry resolves
            if (isRetrying) {
                log.info(
                        "[Batch-Key:{}] Dependent event ({}). Waiting for previous retry...",
                        taxNumber,
                        event.getEventType()
                );

                try {
                    RetryLatch.RetryStatus status =
                            retryLatch.waitForResult(taxNumber, 60);

                    if (status == RetryLatch.RetryStatus.SUCCESS) {
                        log.info(
                                "[Batch-Key:{}] Dependency resolved. Resuming.",
                                taxNumber
                        );
                        isRetrying = false;
                    } else {
                        log.error(
                                "[Batch-Key:{}] Dependency failed/timed-out. Sending {} to DLT.",
                                taxNumber,
                                event.getEventType()
                        );
                        kafkaTemplate.send("person.kafka.dlt", taxNumber, event);
                        isRetrying = false;
                    }
                } catch (Exception e) {
                    log.error(
                            "[Batch-Key:{}] Redis error. Failing batch.",
                            taxNumber,
                            e
                    );
                    throw new RuntimeException(e);
                }
            }

            // Attempt processing
            try {
                processEvent(event);
            } catch (Exception e) {
                boolean isSentToRetry = handleBatchError(record, e);
                if (isSentToRetry) {
                    isRetrying = true;
                } else {
                    if (event.getEventType() == PersonEvent.EventType.CREATE) {
                        log.error("[BATCH-Key:{}] Fatal CREATE error. Breaking chain.", taxNumber);
                        isCriticalFailed = true;
                    }
                }
            }
        }
    }

    private boolean handleBatchError(
            ConsumerRecord<String, PersonEvent> record,
            Exception e
    ) {
        String taxNumber = record.key();
        PersonEvent event = record.value();

        ErrorClassifier.ErrorType type = errorClassifier.classifyError(e);

        if (type == ErrorClassifier.ErrorType.FATAL) {
            log.error("[Batch] Fatal error for {}. Sending to DLT.", taxNumber);
            kafkaTemplate.send("person.kafka.dlt", taxNumber, event);
            return false;
        } else {
            log.info("[Batch] Retryable error for {}. Sending to retry-1.", taxNumber);

            ProducerRecord<String, Object> retryRecord =
                    new ProducerRecord<>("person.kafka.retry-1", taxNumber, event);

            retryRecord.headers()
                    .add("retry-count", "1".getBytes());

            kafkaTemplate.send(retryRecord);
            return true;
        }
    }

    private void processEvent(PersonEvent event) {
        switch (event.getEventType()) {
            case CREATE -> personService.createPersonFromEvent(event.getPerson());
            case UPDATE -> personService.updatePersonFromEvent(event.getPerson());
            case DELETE -> personService.deletePersonFromEvent(event.getPerson());
        }
    }
}