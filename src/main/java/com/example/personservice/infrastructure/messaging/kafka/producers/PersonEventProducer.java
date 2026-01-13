package com.example.personservice.infrastructure.messaging.kafka.producers;

import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PersonEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String PERSON_EVENTS_TOPIC = "person.kafka";

    public PersonEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPersonCreated(PersonEvent event) {
        kafkaTemplate.send(PERSON_EVENTS_TOPIC, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("PersonCreatedEvent published successfully: {}", event.getPerson().getId());
                    } else {
                        log.error("Failed to publish PersonCreatedEvent: {}", event.getPerson().getId(), ex);
                        throw new RuntimeException("Failed to publish PersonCreatedEvent");
                    }
                });
    }

    public void publishPersonUpdated(PersonEvent event) {
        kafkaTemplate.send(PERSON_EVENTS_TOPIC, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("PersonUpdatedEvent published successfully: {}", event.getPerson().getId());
                    } else {
                        log.error("Failed to publish PersonUpdatedEvent: {}", event.getPerson().getId(), ex);
                        throw new RuntimeException("Failed to publish PersonUpdatedEvent", ex);
                    }
                });
    }

    public void publishPersonDeleted(PersonEvent event) {
        kafkaTemplate.send(PERSON_EVENTS_TOPIC, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("PersonDeletedEvent published successfully: {}", event.getPerson().getId());
                    } else {
                        log.error("Failed to publish PersonDeletedEvent: {}", event.getPerson().getId(), ex);
                        throw new RuntimeException("Failed to publish PersonDeletedEvent", ex);
                    }
                });
    }
}
