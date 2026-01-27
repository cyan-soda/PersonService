package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.TaxService;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaxCalculationBatchRetryConsumer {

    private final TaxService taxService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TAX_RETRY_TOPIC_1 = "tax.kafka.batch.retry-1";
    private static final String TAX_RETRY_TOPIC_2 = "tax.kafka.batch.retry-2";
    private static final String TAX_DLT_TOPIC = "tax.kafka.batch.dlt";
//    private static final String PROCESSED_KEY_PREFIX = "tax:processed:";
    private static final int MAX_RETRIES = 2;

    @KafkaListener(
            topics = {TAX_RETRY_TOPIC_1, TAX_RETRY_TOPIC_2},
            groupId = "tax.batch.retry.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    public void consumeRetries(List<ConsumerRecord<String, TaxCalculationEvent>> records, Acknowledgment ack) {
        log.info("[Tax Retry] Received batch of size {} from topics.", records.size());

        for (ConsumerRecord<String, TaxCalculationEvent> record : records) {
            processSingleRecord(record);
        }

        ack.acknowledge();
        log.info("[Tax Retry] Batch acknowledged.");
    }

    private void processSingleRecord(ConsumerRecord<String, TaxCalculationEvent> record) {
        TaxCalculationEvent event = record.value();

        try {
//            if (isDuplicate(event)) {
//                log.info("[Tax Retry] Skipping duplicate event {}.", event.getEventId());
//                return;
//            }

            log.info("[Tax Retry] Processing event for taxNumber: {}", event.getTaxId());
            taxService.handleTaxCalculation(event.getTaxId(), event.getAmount());

//            markAsProcessed(event);
            log.info("[Tax Retry] Successfully processed event for taxNumber: {}", event.getTaxId());

        } catch (IllegalArgumentException e) {
            log.error("[Tax Retry] Invalid event data for event {}: {}. Sending to DLT.", event.getEventId(), e.getMessage());
            sendToDlt(record, e);
        } catch (Exception e) {
            log.warn("[Tax Retry] Retryable error for event {}: {}. Forwarding for next attempt.", event.getEventId(), e.getMessage());
            forwardToNextTopic(record, e);
        }
    }

    private void forwardToNextTopic(ConsumerRecord<String, TaxCalculationEvent> record, Exception e) {
        int retryCount = 0;
        Header retryHeader = record.headers().lastHeader("retry-count");
        if (retryHeader != null) {
            retryCount = Integer.parseInt(new String(retryHeader.value()));
        }

        if (retryCount >= MAX_RETRIES) {
            log.error("[Tax Retry] Max retries ({}) reached for event {}. Sending to DLT.", MAX_RETRIES, record.key());
            sendToDlt(record, e);
        } else {
            String nextTopic = (retryCount == 1) ? TAX_RETRY_TOPIC_2 : TAX_DLT_TOPIC;
            int nextRetryCount = retryCount + 1;
            log.info("[Tax Retry] Forwarding event {} to {} (attempt {}).", record.key(), nextTopic, nextRetryCount);

            ProducerRecord<String, Object> nextRecord = new ProducerRecord<>(nextTopic, record.key(), record.value());
            nextRecord.headers().add("retry-count", String.valueOf(nextRetryCount).getBytes());
            nextRecord.headers().add("error-message", e.getMessage().getBytes());

            kafkaTemplate.send(nextRecord);
        }
    }

    private void sendToDlt(ConsumerRecord<String, TaxCalculationEvent> record, Exception e) {
        ProducerRecord<String, Object> dltRecord = new ProducerRecord<>(TAX_DLT_TOPIC, record.key(), record.value());
        dltRecord.headers().add("final-error", e.getMessage().getBytes());
        kafkaTemplate.send(dltRecord);
    }

//    private boolean isDuplicate(TaxCalculationEvent event) {
//        String processedKey = PROCESSED_KEY_PREFIX + event.getEventId();
//        return Boolean.TRUE.equals(redisTemplate.hasKey(processedKey));
//    }
//
//    private void markAsProcessed(TaxCalculationEvent event) {
//        String processedKey = PROCESSED_KEY_PREFIX + event.getEventId();
//        redisTemplate.opsForValue().set(processedKey, "processed", 24, TimeUnit.HOURS);
//    }
}
