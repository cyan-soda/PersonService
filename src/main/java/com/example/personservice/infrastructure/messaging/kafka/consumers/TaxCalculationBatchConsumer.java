package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.TaxService;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class TaxCalculationBatchConsumer {

    private final TaxService taxService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TAX_BATCH_TOPIC = "tax.kafka.batch";
    private static final String TAX_RETRY_TOPIC_1 = "tax.kafka.batch.retry-1";
    private static final String TAX_DLT_TOPIC = "tax.kafka.batch.dlt";
//    private static final String PROCESSED_KEY_PREFIX = "tax:processed:";

    @KafkaListener(
            topics = TAX_BATCH_TOPIC,
            groupId = "tax.batch.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    public void consumeBatch(List<ConsumerRecord<String, TaxCalculationEvent>> records, Acknowledgment ack) {
        log.info("[Tax Batch] Received batch of size: {}", records.size());

        for (ConsumerRecord<String, TaxCalculationEvent> record : records) {
            TaxCalculationEvent event = record.value();
            try {
//                if (isDuplicate(event)) {
//                    log.info("[Tax Batch] Skipping duplicate event {}", event.getEventId());
//                    continue;
//                }

                taxService.handleTaxCalculation(event.getTaxId(), event.getAmount());
//                markAsProcessed(event);

            } catch (IllegalArgumentException e) {
                log.error("[Tax Batch] Invalid event data for taxNumber {}. Sending directly to DLT. Error: {}",
                        event.getTaxId(), e.getMessage());
                sendToDlt(record, e);

            } catch (Exception e) {
                log.warn("[Tax Batch] Retryable error for taxNumber {}. Sending to Retry-1. Error: {}",
                        event.getTaxId(), e.getMessage());
                sendToRetry(record, e);
            }
        }

        ack.acknowledge();
        log.info("[Tax Batch] Batch processing complete and acknowledged.");
    }

    private void sendToRetry(ConsumerRecord<String, TaxCalculationEvent> record, Exception e) {
        try {
            ProducerRecord<String, Object> retryRecord =
                    new ProducerRecord<>(TAX_RETRY_TOPIC_1, record.key(), record.value());

            retryRecord.headers().add("retry-count", "1".getBytes());
            retryRecord.headers().add("error-message", e.getMessage().getBytes());

            kafkaTemplate.send(retryRecord);
        } catch (Exception sendEx) {
            log.error("CRITICAL: Failed to send message {} to retry topic. Sending to DLT.", record.key(), sendEx);
            sendToDlt(record, sendEx);
        }
    }

    private void sendToDlt(ConsumerRecord<String, TaxCalculationEvent> record, Exception e) {
        record.headers().add("final-error", e.getMessage().getBytes());
        kafkaTemplate.send(TAX_DLT_TOPIC, record.key(), record.value());
    }

//    private boolean isDuplicate(TaxCalculationEvent event) {
//        String processedKey = PROCESSED_KEY_PREFIX + event.getEventId();
//        return Boolean.TRUE.equals(redisTemplate.hasKey(processedKey));
//    }

//    private void markAsProcessed(TaxCalculationEvent event) {
//        String processedKey = PROCESSED_KEY_PREFIX + event.getEventId();
//        redisTemplate.opsForValue().set(processedKey, "processed", 24, TimeUnit.HOURS);
//    }
}
