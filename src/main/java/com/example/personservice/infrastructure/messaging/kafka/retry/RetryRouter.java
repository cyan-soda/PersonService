package com.example.personservice.infrastructure.messaging.kafka.retry;

import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
@Slf4j
@RequiredArgsConstructor
public class RetryRouter {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String BASE_TOPIC = "person.kafka.retry-";
    private static final String DLT_TOPIC = "person.kafka.dlt";
    private static final int MAX_RETRIES = 3;

    public void routeToNextTopic(PersonEvent event, String taxNumber, int currentRetryCount) {
        int nextAttempt = currentRetryCount + 1;

        if (nextAttempt > MAX_RETRIES) {
            log.warn("[Router] Max retries ({}) reached for key {}. Sending to DLT.", MAX_RETRIES, taxNumber);
            sendToDlt(event, taxNumber);
        } else {
            String nextTopic = BASE_TOPIC + nextAttempt;
            log.info("[Router] Routing key {} to topic {} for attempt #{}", taxNumber, nextTopic, nextAttempt);

            ProducerRecord<String, Object> record = new ProducerRecord<>(nextTopic, taxNumber, event);
            record.headers().add("retry-count", String.valueOf(nextAttempt).getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
        }
    }

    public void sendToDlt(PersonEvent event, String taxNumber) {
        kafkaTemplate.send(DLT_TOPIC, taxNumber, event);
    }
}
