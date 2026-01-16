package com.example.personservice.infrastructure.messaging.kafka.producers;

import com.example.personservice.infrastructure.exception.KafkaProducerException;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;


// refactor and optimize
@Component
@Slf4j
public class PersonEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String PERSON_EVENTS_TOPIC = "person.kafka";

    public PersonEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPersonCreated(PersonEvent event) {
        log.info("Publishing PersonCreatedEvent for person with tax number: {}",
                event.getPerson().getTaxNumber());

        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(PERSON_EVENTS_TOPIC, event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("PersonCreatedEvent published successfully: taxNumber={}, offset={}",
                            event.getPerson().getTaxNumber(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to publish PersonCreatedEvent for tax number: {}",
                            event.getPerson().getTaxNumber(), ex);
                    throw new KafkaProducerException("Failed to publish PersonCreatedEvent", ex);
                }
            });

        } catch (Exception ex) {
            log.error("Error publishing PersonCreatedEvent for tax number: {}",
                    event.getPerson().getTaxNumber(), ex);
            throw new KafkaProducerException("Failed to publish PersonCreatedEvent", ex);
        }
    }

    public void publishPersonUpdated(PersonEvent event) {
        log.info("Publishing PersonUpdatedEvent for person ID: {}", event.getPerson().getId());

        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(PERSON_EVENTS_TOPIC, event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("PersonUpdatedEvent published successfully: ID={}, offset={}",
                            event.getPerson().getId(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to publish PersonUpdatedEvent for ID: {}",
                            event.getPerson().getId(), ex);
                    throw new KafkaProducerException("Failed to publish PersonUpdatedEvent", ex);
                }
            });

        } catch (Exception ex) {
            log.error("Error publishing PersonUpdatedEvent for ID: {}",
                    event.getPerson().getId(), ex);
            throw new KafkaProducerException("Failed to publish PersonUpdatedEvent", ex);
        }
    }

    public void publishPersonDeleted(PersonEvent event) {
        log.info("Publishing PersonDeletedEvent for person ID: {}", event.getPerson().getId());

        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(PERSON_EVENTS_TOPIC, event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("PersonDeletedEvent published successfully: ID={}, offset={}",
                            event.getPerson().getId(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to publish PersonDeletedEvent for ID: {}",
                            event.getPerson().getId(), ex);
                    throw new KafkaProducerException("Failed to publish PersonDeletedEvent", ex);
                }
            });

        } catch (Exception ex) {
            log.error("Error publishing PersonDeletedEvent for ID: {}",
                    event.getPerson().getId(), ex);
            throw new KafkaProducerException("Failed to publish PersonDeletedEvent", ex);
        }
    }
}
