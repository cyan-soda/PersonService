package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.domain.model.Person;
import com.example.personservice.domain.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PersonEventConsumer {

    private final PersonRepository repository;

    public PersonEventConsumer(PersonRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "person.kafka", groupId = "person.crud.group")
    public void handlePersonEvent(PersonEvent event) {
        try {

            if (event == null || event.getPerson() == null) {

                log.error("Received null event of null person data");
                return;

            }

            log.info("Received event type: {} for Person with tax number: {}",
                    event.getAction(), event.getPerson().getTaxNumber());

            switch (event.getAction()) {
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
                    log.warn("Unknown event type: {}", event.getAction());
            }

        } catch (Exception exception) {

            log.error("Error processing person event: {}",
                    exception.getMessage());
            throw  exception;

        }
    }

    private void createPerson(PersonEvent event) {

        Person data = event.getPerson();

        // Check for idempotency
        if (repository.existsByTaxNumber(data.getTaxNumber())) {

            log.warn("Person with tax number {} already exists. Skipping creation.",
                    data.getTaxNumber());
            return;

        }

        try {

            Person person = new Person();
            person.setFirstName(data.getFirstName());
            person.setLastName(data.getLastName());
            person.setDateOfBirth(data.getDateOfBirth());
            person.setTaxNumber(data.getTaxNumber());

            Person saved = repository.save(person);
            log.info("Person created successfully: ID={}, taxNumber={}",
                    saved.getId(), saved.getTaxNumber());

        } catch (Exception exception) {

            log.warn("Error creating person: {}. Skipping.",
                    exception.getMessage());
            throw exception;

        }

    }

    private void updatePerson(PersonEvent event) {

        Person data = event.getPerson();

        repository.findById(data.getId()).ifPresentOrElse(
                person -> {
                    person.updatePersonInfo(data.getFirstName(), data.getLastName(), data.getDateOfBirth());
                    repository.save(person);
                    log.info("Person updated: ID={}", data.getId());
                },
                () -> log.error("Update failed: Person with ID={} not found",
                        data.getId()));

    }

    private void deletePerson(PersonEvent event) {

        Long id = event.getPerson().getId();

        if (repository.existsById(id)) {
            repository.deleteById(id);
            log.info("Person with ID={} deleted.",
                    event.getPerson().getId());
        } else {
            log.warn("Person with ID={} not found for deletion.",
                    event.getPerson().getId());
        }

    }
}