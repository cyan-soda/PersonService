package com.example.personservice.infrastructure.messaging.kafka.retry;

import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class RetryRouter {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    // delay the publish to the next topic
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

    private static final String BASE_TOPIC = "person.kafka.retry-";
    private static final String DLT_TOPIC = "person.kafka.dlt";
    private static final int MAX_RETRIES = 3;

    public void routeToNextTopic(PersonEvent event, String taxNumber, int currentRetryCount) {
        int nextRetryCount = currentRetryCount + 1;

        if (nextRetryCount > MAX_RETRIES) {
            log.warn("Max retries reached for {}. Sending to DLT.", taxNumber);
            sendToDlt(event, taxNumber);
            return;
        }

        String nextTopic = BASE_TOPIC + nextRetryCount;
        long delay = calculateDelay(nextRetryCount);

        log.info("Scheduling retry #{} for {} to topic {} in {}ms", nextRetryCount, taxNumber, nextTopic, delay);

        scheduler.schedule(() -> {
            try {
                ProducerRecord<String, Object> record = new ProducerRecord<>(nextTopic, taxNumber, event);
                record.headers().add("retry-count", String.valueOf(nextRetryCount).getBytes(StandardCharsets.UTF_8));
                kafkaTemplate.send(record);
            } catch (Exception e) {
                log.error("Failed to route to next topic", e);
            }
        }, delay, TimeUnit.MILLISECONDS);

    }

    public void sendToDlt(PersonEvent event, String taxNumber) {
        kafkaTemplate.send(DLT_TOPIC, taxNumber, event);
    }

    private long calculateDelay(int retryCount) {
        return 1000L * (long) Math.pow(2, retryCount - 1);
    }

}
