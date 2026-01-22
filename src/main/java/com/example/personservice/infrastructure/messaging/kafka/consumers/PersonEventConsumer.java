package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.domain.model.Person;
import com.example.personservice.infrastructure.exception.KafkaConsumerException;
import com.example.personservice.infrastructure.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.ResourceAccessException;

import javax.sql.rowset.serial.SerialException;
import java.net.SocketException;
import java.util.List;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonEventConsumer {

    private final PersonService service;

    // for single, non-blocking retry
//    @RetryableTopic(
//            attempts = "4",
//            backOff = @BackOff(
//                    delay = 1000,
//                    multiplier = 2,
//                    maxDelay = 10000
//            ),
//            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
//            include = {
//                    SocketException.class,
//                    ResourceAccessException.class,
//                    TransientDataAccessException.class,
//                    RecoverableDataAccessException.class,
//                    KafkaConsumerException.class
//            },
//            dltStrategy = DltStrategy.FAIL_ON_ERROR
//    )
    @KafkaListener(
            topics = {"person.kafka", "person.kafka-retry"},
            groupId = "person.crud.group",
            containerFactory = "personKafkaListenerContainerFactory"
    )
    @Transactional
    public void handlePersonEvent(
            @Payload PersonEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {

        log.info("Received PersonEvent from topic: {}, partition: {}, offset: {}",
                topic, partition, offset);

        if (event == null || event.getPerson() == null) {
            log.error("Received null event or null person data");
            throw new IllegalArgumentException("Invalid event data received");
        }

        try {
            processEvent(event);
            acknowledgment.acknowledge();
            log.info("Successfully processed and acknowledged PersonEvent: {}", event.getEventType());
        } catch (Exception e) {
            log.error("Error processing PersonEvent: {}", event, e);
            throw e; // trigger error handler
        }

    }

    private void processEvent(PersonEvent event) {
        Person person = event.getPerson();
        switch (event.getEventType()) {
            case CREATE -> {
                log.info("Processing CREATE event for person: {}", person);
                service.createPersonFromEvent(person);
            }
            case UPDATE -> {
                log.info("Processing UPDATE event for person: {}", person);
                service.updatePersonFromEvent(person);
            }
            case DELETE -> {
                log.info("Processing DELETE event for person ID: {}", person.getId());
                service.deletePersonFromEvent(person);
            }
            default -> {
                log.warn("Unknown event type: {}", event.getEventType());
                throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
            }
        }
    }

}