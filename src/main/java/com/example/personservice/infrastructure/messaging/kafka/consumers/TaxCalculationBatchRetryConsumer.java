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
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaxCalculationBatchRetryConsumer {

    private final TaxService taxService;
    private final PersonRepository personRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TAX_RETRY_TOPIC_1 = "tax.kafka.batch.retry-1";
    private static final String TAX_RETRY_TOPIC_2 = "tax.kafka.batch.retry-2";
    private static final String TAX_DLT_TOPIC = "tax.kafka.batch.dlt";
    private static final String PROCESSED_KEY_PREFIX = "tax:processed:";
    private static final int PROCESSED_TTL_SECONDS = 60;

    // --- RETRY LEVEL 1 ---
    @KafkaListener(
            topics = TAX_RETRY_TOPIC_1,
            groupId = "tax.batch.retry.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeRetry1(List<ConsumerRecord<String, TaxCalculationEvent>> records, Acknowledgment ack) {
        log.info("[Tax Retry-1] Received batch of size {}", records.size());
        processBatchOrForward(records, ack, TAX_RETRY_TOPIC_2, "RETRY-2", 1);
    }

    // --- RETRY LEVEL 2 ---
    @KafkaListener(
            topics = TAX_RETRY_TOPIC_2,
            groupId = "tax.batch.retry.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeRetry2(List<ConsumerRecord<String, TaxCalculationEvent>> records, Acknowledgment ack) {
        log.info("[Tax Retry-2] Received batch of size {}", records.size());
        processBatchOrForward(records, ack, TAX_DLT_TOPIC, "DLT", 2);
    }

    // --- HELPER METHOD ---
    private void processBatchOrForward(List<ConsumerRecord<String, TaxCalculationEvent>> records,
                                       Acknowledgment ack,
                                       String nextTopic,
                                       String nextTopicName,
                                       int retryLevel) {
        try {
            log.info("[Tax Retry-{}] Attempting to process batch of {} records", retryLevel, records.size());

            // Process the batch with validation and idempotency
            for (ConsumerRecord<String, TaxCalculationEvent> record : records) {
                TaxCalculationEvent event = record.value();
                String taxNumber = event.getTaxId();
                String processedKey = PROCESSED_KEY_PREFIX + taxNumber + ":" + event.getAmount();

                log.info("[Tax Retry-{}] Processing taxNumber: {}, amount: {}",
                        retryLevel, taxNumber, event.getAmount());

                // Check if already processed (idempotency)
                if (isAlreadyProcessed(processedKey)) {
                    log.info("[Tax Retry-{}] Tax calculation for {} already processed. Skipping.", retryLevel, taxNumber);
                    continue;
                }

                // Validate event structure - if invalid, send directly to DLT
                try {
                    validateTaxEvent(event);
                } catch (IllegalArgumentException e) {
                    log.error("[Tax Retry-{}] Invalid event data for {}: {}. Sending to DLT.",
                            retryLevel, taxNumber, e.getMessage());
                    kafkaTemplate.send(TAX_DLT_TOPIC, record.key(), record.value());
                    continue; // Skip this record, continue with others
                }

                validatePersonExists(taxNumber);
                processTaxCalculation(event);
                markAsProcessed(processedKey);
            }

            ack.acknowledge();
            log.info("[Tax Retry-{}] Successfully processed batch of {} records", retryLevel, records.size());

        } catch (Exception e) {
            log.error("[Tax Retry-{}] Batch failed again. Error: {}", retryLevel, e.getMessage());

            if ("DLT".equals(nextTopicName)) {
                log.error("[Tax Retry-{}] Final retry failed. Sending {} records to DLT.",
                        retryLevel, records.size());

                records.forEach(record -> {
                    kafkaTemplate.send(TAX_DLT_TOPIC, record.key(), record.value());
                    log.debug("[Tax Retry-{}] Sent record {} to DLT", retryLevel, record.key());
                });
            } else {
                log.warn("[Tax Retry-{}] Forwarding {} records to {}.",
                        retryLevel, records.size(), nextTopicName);

                records.forEach(record -> {
                    kafkaTemplate.send(nextTopic, record.key(), record.value());
                    log.debug("[Tax Retry-{}] Forwarded record {} to {}", retryLevel, record.key(), nextTopicName);
                });
            }

            ack.acknowledge();
            log.info("[Tax Retry-{}] Acknowledged batch. Records moved to {}", retryLevel, nextTopicName);

        }
    }

    private boolean isAlreadyProcessed(String processedKey) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(processedKey));
    }

    private void markAsProcessed(String processedKey) {
        redisTemplate.opsForValue().set(processedKey, "processed", PROCESSED_TTL_SECONDS, TimeUnit.SECONDS);
    }

    private void validateTaxEvent(TaxCalculationEvent event) {
        if (event.getTaxId() == null || event.getTaxId().trim().isEmpty()) {
            throw new IllegalArgumentException("Tax ID cannot be null or empty");
        }
        if (event.getAmount() == null || event.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Tax amount must be greater than zero");
        }
    }

    private void validatePersonExists(String taxNumber) {
        if (!personRepository.existsByTaxNumber(taxNumber)) {
            log.warn("[Tax Retry] Person with taxNumber {} does not exist in database", taxNumber);
            throw new PersonNotFoundException("Person with tax number " + taxNumber + " not found");
        }
        log.debug("[Tax Retry] Person with taxNumber {} exists in database", taxNumber);
    }

    private void processTaxCalculation(TaxCalculationEvent event) {
        String taxNumber = event.getTaxId();

        // Simulate failure conditions for testing
        if ("TAX888".equals(taxNumber)) {
            throw new RuntimeException("Simulated transient error for " + taxNumber);
        }
        if ("TAX889".equals(taxNumber)) {
            throw new RuntimeException("Simulated fatal error for " + taxNumber);
        }

        try {
            taxService.processTaxCalculationEvent(event);
            log.info("[Tax Retry] Successfully processed tax calculation for {}", taxNumber);
        } catch (Exception e) {
            log.error("[Tax Retry] Failed to process tax calculation for {}: {}", taxNumber, e.getMessage());
            throw e;
        }
    }
}
