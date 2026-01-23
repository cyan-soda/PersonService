package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.TaxService;
import com.example.personservice.infrastructure.exception.PersonNotFoundException;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import com.example.personservice.infrastructure.repository.PersonRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class TaxCalculationBatchConsumer {

    private final TaxService taxService;
    private final PersonRepository personRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TAX_BATCH_TOPIC = "tax.kafka.batch";
    private static final String TAX_RETRY_TOPIC_1 = "tax.kafka.batch.retry-1";
    private static final String TAX_DLT_TOPIC = "tax.kafka.batch.dlt";
    private static final String PROCESSED_KEY_PREFIX = "tax:processed:";
    private static final int PROCESSED_TTL_HOURS = 24;

    @KafkaListener(
            topics = TAX_BATCH_TOPIC,
            groupId = "tax.batch.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    public void consumeBatch(List<ConsumerRecord<String, TaxCalculationEvent>> records, Acknowledgment ack) {
        log.info("[Tax Batch] Received batch of size {}", records.size());

        try {
            // 1. PREPARE DATA
            // We filter duplicates BEFORE starting the DB transaction
            List<TaxCalculationEvent> eventsToProcess = new ArrayList<>();

            for (ConsumerRecord<String, TaxCalculationEvent> record : records) {
                TaxCalculationEvent event = record.value();
                UUID eventId = event.getEventId();
                String processedKey = "tax:processed:" + eventId;

                if (Boolean.TRUE.equals(redisTemplate.hasKey(processedKey))) {
                    log.info("[Tax Batch] Skipping duplicate event {}", eventId);
                    continue;
                }
                eventsToProcess.add(event);
            }

            if (!eventsToProcess.isEmpty()) {
                // 2. ATOMIC DB TRANSACTION
                // Calls the new method in TaxService.
                // If this fails, the Service rolls back ALL DB changes.
                taxService.processBatch(eventsToProcess);

                // 3. UPDATE REDIS (Only if DB success)
                for (TaxCalculationEvent event : eventsToProcess) {
                    String processedKey = "tax:processed:" + event.getEventId();
                    redisTemplate.opsForValue().set(processedKey, "processed", 24, TimeUnit.HOURS);
                }
            }

            // 4. ACKNOWLEDGE SUCCESS
            ack.acknowledge();
            log.info("[Tax Batch] Successfully processed batch");

        } catch (Exception e) {
            log.error("[Tax Batch] Batch failed. DB Transaction is already rolled back by Service. Moving to Retry-1. Error: {}", e.getMessage());

            // 5. SEND TO RETRY
            records.forEach(record -> {
                kafkaTemplate.send("tax.kafka.batch.retry-1", record.key(), record.value());
            });

            // 6. ACKNOWLEDGE MAIN TOPIC
            // This moves the offset forward so we don't loop on the same message.
            ack.acknowledge();
        }
    }

}
