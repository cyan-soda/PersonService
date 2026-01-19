package com.example.personservice.infrastructure.messaging.kafka.producers;

import com.example.personservice.infrastructure.exception.KafkaProducerException;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String PERSON_EVENTS_TOPIC = "person.kafka";

    public void publishEvent (PersonEvent event) {

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(PERSON_EVENTS_TOPIC, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("{} event published successfully: ID={}, offset={}",
                        event.getEventType(), event.getPerson().getId(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish {} event for ID: {}",
                        event.getEventType(), event.getPerson().getId(), ex);
                throw new KafkaProducerException("Failed to publish event", ex);
            }
        });

    }
}
