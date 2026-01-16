package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.domain.model.Person;
import com.example.personservice.infrastructure.exception.KafkaConsumerException;
import com.example.personservice.infrastructure.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.rowset.serial.SerialException;
import java.util.UUID;

@Component
@Slf4j
public class PersonEventConsumer {

    private final PersonRepository repository;

    public PersonEventConsumer(PersonRepository repository) {
        this.repository = repository;
    }

//    @RetryableTopic(
//            attempts = "4",
//            backOff = @BackOff(delay = 1000, multiplier = 2),
////            exclude = {SerialException.class, DeserializationException.class},
//            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
//    )
    @KafkaListener(
            topics = "person.kafka",
            groupId = "person.crud.group",
            containerFactory = "personKafkaListenerContainerFactory"
    )
    @Transactional
    public void handlePersonEvent(
            @Payload PersonEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition
    ) {

        log.info("Received Kafka message from topic: {}, partition: {}",
                topic, partition);

        try {
            if (event == null || event.getPerson() == null) {
                log.error("Received null event or null person data");
                throw new KafkaConsumerException("Invalid event data received");
            }

            log.info("Processing event type: {} for Person with tax number: {}",
                    event.getEventType(), event.getPerson().getTaxNumber());

            switch (event.getEventType()) {
                case CREATE:
                    createPerson(event);
                    break;
                case UPDATE:
                    updatePerson(event);
                    break;
                case DELETE:
                    deletePerson(event);
                    break;
                default:
                    log.warn("Unknown event type: {}", event.getEventType());
                    throw new KafkaConsumerException("Unknown event type: " + event.getEventType());
            }

        } catch (Exception exception) {
            log.error("Error processing person event: {}", exception.getMessage(), exception);

            throw new KafkaConsumerException("Failed to process person event", exception);
        }
    }

    private void createPerson(PersonEvent event) {
        Person data = event.getPerson();

        log.info("Creating person from Kafka event: taxNumber={}",
                data.getTaxNumber());

        try {
            if (repository.existsByTaxNumber(data.getTaxNumber())) {
                log.warn("Person with tax number {} already exists. Skipping creation",
                        data.getTaxNumber());
                return;
            }

            Person person = new Person();
            person.setFirstName(data.getFirstName());
            person.setLastName(data.getLastName());
            person.setDateOfBirth(data.getDateOfBirth());
            person.setTaxNumber(data.getTaxNumber());

            Person saved = repository.save(person);
            log.info("Person created successfully from Kafka: ID={}, taxNumber={}",
                    saved.getId(), saved.getTaxNumber());

        } catch (DataIntegrityViolationException ex) {
            log.warn("Data integrity violation while creating person: {}. Skipping.", ex.getMessage());
        } catch (Exception ex) {
            log.error("Error creating person from Kafka event: {}", ex.getMessage(), ex);
            throw new KafkaConsumerException("Failed to create person", ex);
        }
    }

    private void updatePerson(PersonEvent event) {
        Person data = event.getPerson();

        log.info("Updating person from Kafka event: ID={}", data.getId());

        try {
            repository.findById(data.getId()).ifPresentOrElse(
                    person -> {
                        try {
                            person.updatePersonInfo(data.getFirstName(), data.getLastName(), data.getDateOfBirth());
                            Person updated = repository.save(person);
                            log.info("Person updated successfully from Kafka: ID={}",
                                    updated.getId());
                        } catch (Exception ex) {
                            log.error("Error saving updated person ID={}: {}",
                                    data.getId(), ex.getMessage(), ex);
                            throw new KafkaConsumerException("Failed to save updated person", ex);
                        }
                    },
                    () -> {
                        log.warn("Update failed: Person with ID={} not found",
                                data.getId());
                        // todo: handle ordering issues (throw ex, handle)
                    });

        } catch (Exception ex) {
            log.error("Error updating person from Kafka event: {}", ex.getMessage(), ex);
            throw new KafkaConsumerException("Failed to update person", ex);
        }
    }

    private void deletePerson(PersonEvent event) {
        UUID id = event.getPerson().getId();

        log.info("Deleting person from Kafka event: ID={}", id);

        try {
            if (repository.existsById(id)) {
                repository.deleteById(id);
                log.info("Person with ID={} deleted successfully from Kafka", id);
            } else {
                log.warn("Person with ID={} not found for deletion. Might already be deleted.", id);
            }

        } catch (Exception ex) {
            log.error("Error deleting person from Kafka event : {}", ex.getMessage(), ex);
            throw new KafkaConsumerException("Failed to delete person", ex);
        }
    }
}