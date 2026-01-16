package com.example.personservice.application.service;

import com.example.personservice.application.dto.person.OperationResponseDto;
import com.example.personservice.domain.model.Person;
import com.example.personservice.infrastructure.exception.PersonAlreadyExistsException;
import com.example.personservice.infrastructure.exception.PersonNotFoundException;
import com.example.personservice.infrastructure.exception.PersonServiceException;
import com.example.personservice.infrastructure.repository.PersonRepository;
import com.example.personservice.application.dto.person.CreatePersonRequestDto;
import com.example.personservice.application.dto.person.PersonResponseDto;
import com.example.personservice.application.dto.person.UpdatePersonRequestDto;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.producers.PersonEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class PersonService {
    private final PersonRepository repository;
    private final PersonEventProducer producer;

    public OperationResponseDto createPerson(CreatePersonRequestDto request) {
        log.info("Creating person with tax number: {}", request.getTaxNumber());

        try {
            if (repository.existsByTaxNumber(request.getTaxNumber())) {
                log.warn("Person with tax number: {} already exists", request.getTaxNumber());
                throw PersonAlreadyExistsException.withTaxNumber(request.getTaxNumber());
            }

            Person person = new Person();
            person.setFirstName(request.getFirstName());
            person.setLastName(request.getLastName());
            person.setDateOfBirth(request.getDateOfBirth());
            person.setTaxNumber(request.getTaxNumber());

            PersonEvent event = new PersonEvent(PersonEvent.EventType.CREATE, person);
            producer.publishPersonCreated(event);

            return new OperationResponseDto(
                    "Person creation request successfully sent to Kafka for processing",
                    "CREATE"
            );

        } catch (PersonAlreadyExistsException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error creating person with tax number: {}", request.getTaxNumber());
            throw new PersonServiceException("Failed to create person", e);
        }
    }

    public OperationResponseDto updatePerson(UUID id, UpdatePersonRequestDto request) {
        log.info("Updating person with ID: {}", id);

        try {
            Person person = repository.findById(id)
                    .orElseThrow(() -> {
                        log.warn("Person not found for update with ID: {}", id);
                        return PersonNotFoundException.byId(id);
                    });

            log.info("Found person for update: ID={}, taxNumber={}", person.getId(), person.getTaxNumber());

            person.updatePersonInfo(
                    request.getFirstName(),
                    request.getLastName(),
                    request.getDateOfBirth()
            );

            PersonEvent event = new PersonEvent(PersonEvent.EventType.UPDATE, person);
            producer.publishPersonUpdated(event);

            log.info("Person update event published successfully for ID: {}", id);
            return new OperationResponseDto(
                    "Person update request successfully sent to Kafka for processing",
                    "UPDATE"
            );

        } catch (PersonNotFoundException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error updating person with tax ID: {}", id);
            throw new PersonServiceException("Failed to update person", e);
        }
    }

    public OperationResponseDto deletePerson(UUID id) {
        log.info("Deleting person with ID: {}", id);

        try {
            Person person = repository.findById(id)
                    .orElseThrow(() -> {
                        log.warn("Person not found for deletion with ID: {}", id);
                        return PersonNotFoundException.byId(id);
                    });

            log.debug("Found person for deletion: ID={}, taxNumber={}", person.getId(), person.getTaxNumber());

            PersonEvent event = new PersonEvent(PersonEvent.EventType.DELETE, person);
            producer.publishPersonDeleted(event);

            log.info("Person deletion event published successfully for ID: {}", id);
            return new OperationResponseDto(
                    "Person deletion request successfully sent to Kafka for processing",
                    "DELETE"
            );

        } catch (PersonNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("Error deleting person with ID: {}", id, ex);
            throw new PersonServiceException("Failed to delete person", ex);
        }
    }

    public List<PersonResponseDto> findAll() {
        log.info("Retrieving all persons");

        try {
            List<PersonResponseDto> persons = repository
                    .findAll()
                    .stream()
                    .map(this::mapToDto)
                    .collect(Collectors.toList());

            log.info("Retrieved {} persons", persons.size());
            return persons;

        } catch (Exception ex) {
            log.error("Error retrieving all persons", ex);
            throw new PersonServiceException("Failed to retrieve persons", ex);
        }
    }

    public PersonResponseDto findById(UUID id) {
        log.info("Finding person by ID: {}", id);

        try {
            return repository.findById(id)
                    .map(this::mapToDto)
                    .orElseThrow(() -> {
                        log.warn("Person not found with ID: {}", id);
                        return PersonNotFoundException.byId(id);
                    });

        } catch (PersonNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("Error finding person by ID: {}", id, ex);
            throw new PersonServiceException("Failed to find person by ID", ex);
        }
    }

    public PersonResponseDto findByTaxNumber(String taxNumber) {
        log.info("Finding person by tax number: {}", taxNumber);

        try {
            return repository.findByTaxNumber(taxNumber)
                    .map(this::mapToDto)
                    .orElseThrow(() -> {
                        log.warn("Person not found with tax number: {}", taxNumber);
                        return PersonNotFoundException.byTaxNumber(taxNumber);
                    });
        } catch (Exception ex) {
            log.error("Error finding person by tax number: {}", taxNumber, ex);
            throw new PersonServiceException("Failed to find person by tax number", ex);
        }
    }

//    todo: sanitize inputs
    public List<PersonResponseDto> findByNameAndAge(
            String firstNamePrefix,
            String lastNamePrefix,
            Integer minAge
    ) {
        log.info("Searching persons with firstNamePrefix: {}, lastNamePrefix: {}, minAge: {}",
                firstNamePrefix, lastNamePrefix, minAge);

        try {
            List<PersonResponseDto> persons = repository.findByNameAndAge(firstNamePrefix, lastNamePrefix, minAge)
                    .stream()
                    .map(this::mapToDto)
                    .toList();

            log.info("Found {} persons", persons.size());
            return persons;

        } catch (Exception ex) {
            log.error("Error searching persons with firstNamePrefix: {}, lastNamePrefix: {}, minAge: {}",
                    firstNamePrefix, lastNamePrefix, minAge, ex);
            throw new PersonServiceException("Failed to search persons", ex);
        }
    }

    private PersonResponseDto mapToDto(Person person) {
        PersonResponseDto dto = new PersonResponseDto();

        dto.setId(person.getId());
        dto.setFirstName(person.getFirstName());
        dto.setLastName(person.getLastName());
        dto.setAge(person.getAge());
        dto.setTaxDebt(person.getTaxDebt());
        dto.setTaxNumber(person.getTaxNumber());

        return dto;
    }

}