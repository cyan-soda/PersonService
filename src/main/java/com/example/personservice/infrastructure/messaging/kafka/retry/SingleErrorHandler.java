package com.example.personservice.infrastructure.messaging.kafka.retry;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.NonNull;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class SingleErrorHandler implements CommonErrorHandler {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ErrorClassifier errorClassifier;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final String RETRY_COUNT_HEADER = "retry-count";
    private static final String RETRY_DELAY_HEADER = "retry-delay";
    private static final String ORIGINAL_TOPIC_HEADER = "original-topic";
    private static final String RETRY_TOPIC_SUFFIX = "-retry";
    private static final String DLT_TOPIC_SUFFIX = "-dlt";

    @Override
    public boolean handleOne(
            @NonNull Exception exception,
            ConsumerRecord<?, ?> record,
            @NonNull Consumer<?, ?> consumer,
            @NonNull MessageListenerContainer container
    ) {
        log.error("Error processing record from topic: {}, partition: {}, offset: {}",
                record.topic(), record.partition(), record.offset(), exception);

        ErrorClassifier.ErrorType errorType = errorClassifier.classifyError(exception);
        int retryCount = getRetryCount(record);

        if (errorType == ErrorClassifier.ErrorType.FATAL || retryCount >= MAX_RETRY_ATTEMPTS) {
            log.warn("Sending record to DLT. Error type: {}, Retry count: {}", errorType, retryCount);
            sendToDlt(record);
            return true; // Acknowledge the message
        }

        log.info("Sending record to retry topic. Retry count: {}", retryCount + 1);
        sendToRetryTopic(record, retryCount + 1);
        return true; // Acknowledge the message
    }

    private int getRetryCount(ConsumerRecord<?, ?> record) {
        Headers headers = record.headers();
        if (headers != null && headers.lastHeader(RETRY_COUNT_HEADER) != null) {
            try {
                String retryCountStr = new String(headers.lastHeader(RETRY_COUNT_HEADER).value());
                return Integer.parseInt(retryCountStr);
            } catch (NumberFormatException e) {
                log.warn("Invalid retry count header, defaulting to 0", e);
                return 0;
            }
        }
        return 0;
    }

    private void sendToRetryTopic(ConsumerRecord<?, ?> record, int retryCount) {
        String retryTopic = getRetryTopicName(record.topic());
        long delay = calculateExponentialBackoff(retryCount);

        Headers headers = new RecordHeaders();
        // Copy existing headers
        if (record.headers() != null) {
            record.headers().forEach(header -> {
                if (!RETRY_COUNT_HEADER.equals(header.key()) &&
                        !RETRY_DELAY_HEADER.equals(header.key()) &&
                        !ORIGINAL_TOPIC_HEADER.equals(header.key())) {
                    headers.add(header.key(), header.value());
                }
            });
        }

        // Add retry-specific headers
        headers.add(RETRY_COUNT_HEADER, String.valueOf(retryCount).getBytes());
        headers.add(RETRY_DELAY_HEADER, String.valueOf(delay).getBytes());
        headers.add(ORIGINAL_TOPIC_HEADER, record.topic().getBytes());

        scheduleRetry(retryTopic, record.key(), record.value(), headers, delay);
    }

    private void sendToDlt(ConsumerRecord<?, ?> record) {
        String dltTopic = getDltTopicName(record.topic());

        Headers headers = new RecordHeaders();
        // Copy existing headers
        if (record.headers() != null) {
            record.headers().forEach(header -> headers.add(header.key(), header.value()));
        }

        // Add DLT-specific headers
        headers.add("dlt-timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
        headers.add("dlt-reason", "max-retries-exceeded".getBytes());

        try {
            kafkaTemplate.send(dltTopic, record.key().toString(), record.value()).get();
            log.info("Successfully sent record to DLT topic: {}", dltTopic);
        } catch (Exception e) {
            log.error("Failed to send record to DLT topic: {}", dltTopic, e);
        }
    }

    private void scheduleRetry(String topic, Object key, Object value, Headers headers, long delay) {
        scheduler.schedule(() -> {
            try {
                CompletableFuture<org.springframework.kafka.support.SendResult<String, Object>> future =
                        kafkaTemplate.send(topic, (String) key, value);

                future.whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to send retry message to topic: {}", topic, throwable);
                    } else {
                        log.info("Successfully sent retry message to topic: {}", topic);
                    }
                });
            } catch (Exception e) {
                log.error("Error scheduling retry for topic: {}", topic, e);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private String getRetryTopicName(String originalTopic) {
        return originalTopic + RETRY_TOPIC_SUFFIX;
    }

    private String getDltTopicName(String originalTopic) {
        return originalTopic + DLT_TOPIC_SUFFIX;
    }

    private long calculateExponentialBackoff(int retryCount) {
        // Base delay: 1 second, exponential backoff with factor of 2
        long baseDelay = 1000L;
        return baseDelay * (long) Math.pow(2, retryCount - 1);
    }
}
