package com.example.personservice.application.service;

import com.example.personservice.application.dto.person.OperationResponseDto;
import com.example.personservice.domain.model.Person;
import com.example.personservice.domain.model.Tax;
import com.example.personservice.domain.specification.PersonSpecification;
import com.example.personservice.infrastructure.exception.KafkaConsumerException;
import com.example.personservice.infrastructure.exception.PersonAlreadyExistsException;
import com.example.personservice.infrastructure.exception.PersonNotFoundException;
import com.example.personservice.infrastructure.exception.PersonServiceException;
import com.example.personservice.infrastructure.repository.PersonRepository;
import com.example.personservice.application.dto.person.CreatePersonRequestDto;
import com.example.personservice.application.dto.person.PersonResponseDto;
import com.example.personservice.application.dto.person.UpdatePersonRequestDto;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.producers.PersonEventProducer;
import com.example.personservice.infrastructure.repository.TaxRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class PersonService {
    private final PersonRepository personRepository;
    private final TaxRepository taxRepository;
    private final PersonEventProducer producer;

    public OperationResponseDto createPerson(CreatePersonRequestDto request) {
        log.info("Creating person with tax number: {}", request.getTaxNumber());

        try {
            if (taxRepository.existsById(request.getTaxNumber())) {
                log.warn("Person with tax number: {} already exists", request.getTaxNumber());
                throw PersonAlreadyExistsException.withTaxNumber(request.getTaxNumber());
            }

            Person person = new Person();
            person.setFirstName(request.getFirstName());
            person.setLastName(request.getLastName());
            person.setDateOfBirth(request.getDateOfBirth());

            Tax tax = new Tax();
            tax.setTaxNumber(request.getTaxNumber());
            tax.setTaxDebt(BigDecimal.ZERO);

            person.setTaxInfo(tax);

            PersonEvent event = new PersonEvent(PersonEvent.EventType.CREATE, person);
            producer.publishEvent(event);

            return new OperationResponseDto(
                    "Person creation request successfully sent to Kafka for processing",
                    "CREATE"
            );

        } catch (Exception e) {
            log.error("Error creating person with tax number: {}", request.getTaxNumber());
            throw new PersonServiceException("Failed to create person", e);
        }
    }

    public OperationResponseDto updatePerson(UUID id, UpdatePersonRequestDto request) {
        log.info("Updating person with ID: {}", id);

        try {
            Person person = personRepository.findById(id)
                    .orElseThrow(() -> {
                        log.warn("Person not found for update with ID: {}", id);
                        return PersonNotFoundException.byId(id);
                    });

            log.info("Found person for update: ID={}, taxNumber={}", person.getId(), person.getTaxInfo().getTaxNumber());

            person.updatePersonInfo(
                    request.getFirstName(),
                    request.getLastName(),
                    request.getDateOfBirth()
            );

            PersonEvent event = new PersonEvent(PersonEvent.EventType.UPDATE, person);
            producer.publishEvent(event);

            log.info("Person update event published successfully for ID: {}", id);
            return new OperationResponseDto(
                    "Person update request successfully sent to Kafka for processing",
                    "UPDATE"
            );

        } catch (Exception e) {
            log.error("Error updating person with tax ID: {}", id);
            throw new PersonServiceException("Failed to update person", e);
        }
    }

    public OperationResponseDto deletePerson(UUID id) {
        log.info("Deleting person with ID: {}", id);

        try {
            Person person = personRepository.findById(id)
                    .orElseThrow(() -> {
                        log.warn("Person not found for deletion with ID: {}", id);
                        return PersonNotFoundException.byId(id);
                    });

            log.debug("Found person for deletion: ID={}, taxNumber={}", person.getId(), person.getTaxInfo().getTaxNumber());

            PersonEvent event = new PersonEvent(PersonEvent.EventType.DELETE, person);
            producer.publishEvent(event);

            log.info("Person deletion event published successfully for ID: {}", id);
            return new OperationResponseDto(
                    "Person deletion request successfully sent to Kafka for processing",
                    "DELETE"
            );

        } catch (Exception ex) {
            log.error("Error deleting person with ID: {}", id, ex);
            throw new PersonServiceException("Failed to delete person", ex);
        }
    }

    public List<PersonResponseDto> findAll() {
        log.info("Retrieving all persons");

        try {
            List<PersonResponseDto> persons = personRepository
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
            return personRepository.findById(id)
                    .map(this::mapToDto)
                    .orElseThrow(() -> {
                        log.warn("Person not found with ID: {}", id);
                        return PersonNotFoundException.byId(id);
                    });

        } catch (Exception ex) {
            log.error("Error finding person by ID: {}", id, ex);
            throw new PersonServiceException("Failed to find person by ID", ex);
        }
    }

    public PersonResponseDto findByTaxNumber(String taxNumber) {
        log.info("Finding person by tax number: {}", taxNumber);

        try {
            return personRepository.findByTaxInfo_TaxNumber(taxNumber)
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

    public Page<PersonResponseDto> findByNameAndAge(
            String firstNamePrefix,
            String lastNamePrefix,
            Integer minAge,
            Pageable pageable
    ) {
        log.info("Searching persons with firstNamePrefix: {}, lastNamePrefix: {}, minAge: {}",
                firstNamePrefix, lastNamePrefix, minAge);

        Specification<Person> spec = PersonSpecification.hasNameAndAge(firstNamePrefix, lastNamePrefix, minAge);
        return personRepository.findAll(spec, pageable).map(this::mapToDto);
    }

    private PersonResponseDto mapToDto(Person person) {
        PersonResponseDto dto = new PersonResponseDto();

        dto.setId(person.getId());
        dto.setFirstName(person.getFirstName());
        dto.setLastName(person.getLastName());
        dto.setAge(person.getAge());
        if (person.getTaxInfo() != null) {
            dto.setTaxNumber(person.getTaxInfo().getTaxNumber());
            dto.setTaxDebt(person.getTaxInfo().getTaxDebt());
        }

        return dto;
    }

    @Transactional
    public void createPersonFromEvent(Person data) {
        String taxNumber = data.getTaxInfo().getTaxNumber();
        log.info("Creating person from Kafka event: taxNumber={}",
                data.getTaxInfo().getTaxNumber());

        if ("retry".equals(data.getFirstName())) {
            log.warn("Simulating DB timeout for CREATE...");
            throw new RecoverableDataAccessException("Simulated DB down during CREATE");
        }

        try {
            if (taxRepository.existsById(taxNumber)) {
                log.warn("Person with tax number {} already exists. Skipping creation",
                        data.getTaxInfo().getTaxNumber());
                return;
            }

            Person saved = personRepository.save(data);
            log.info("Person created successfully from Kafka: ID={}, taxNumber={}",
                    saved.getId(), saved.getTaxInfo().getTaxNumber());

        } catch (DataIntegrityViolationException ex) {
            log.warn("Data integrity violation while creating person: {}. Skipping.", ex.getMessage());
        } catch (Exception ex) {
            log.error("Error creating person from Kafka event: {}", ex.getMessage(), ex);
            throw new KafkaConsumerException("Failed to create person", ex);
        }
    }

    @Transactional
    public void updatePersonFromEvent(Person data) {
        String taxNumber = data.getTaxInfo().getTaxNumber();
        log.info("Updating person from Kafka event. Looking up by TaxNumber: {}", taxNumber);

        if ("retry".equals(data.getFirstName())) {
            log.warn("Simulating DB timeout for UPDATE...");
            throw new RecoverableDataAccessException("Simulated DB down during UPDATE");
        }

        try {
            // Try finding by Tax Number instead of ID for robust batch testing
            personRepository.findByTaxInfo_TaxNumber(taxNumber).ifPresentOrElse(
                    person -> {
                        try {
                            person.updatePersonInfo(data.getFirstName(), data.getLastName(), data.getDateOfBirth());
                            Person updated = personRepository.save(person);
                            log.info("Person updated successfully from Kafka: ID={}", updated.getId());
                        } catch (Exception ex) {
                            log.error("Error saving updated person", ex);
                            throw new KafkaConsumerException("Failed to save updated person", ex);
                        }
                    },
                    () -> {
                        // This is CRITICAL for the retry test.
                        // If Create failed (and is in retry), this lookup returns Empty.
                        // We must Throw Exception so the Batch Consumer knows to WAIT or Fail.
                        log.warn("Update failed: Person with TaxNumber={} not found", taxNumber);
                        throw new PersonNotFoundException("Person not found for update (likely pending creation)");
                    });

        } catch (Exception ex) {
            log.error("Error updating person from Kafka event: {}", ex.getMessage());
            // Ensure we throw a wrapper that the classifier understands
            throw new KafkaConsumerException("Failed to update person", ex);
        }
    }

    @Transactional
    public void deletePersonFromEvent(Person data) {
        String taxNumber = data.getTaxInfo().getTaxNumber();
        log.info("Deleting person from Kafka event, via TaxNumber={}", taxNumber);

        if ("retry".equals(data.getFirstName())) {
            log.warn("Simulating DB timeout for DELETE...");
            throw new RecoverableDataAccessException("Simulated DB down during DELETE");
        }

        try {
            personRepository.findByTaxInfo_TaxNumber(taxNumber).ifPresentOrElse(
                    person -> {
                        personRepository.delete(person);
                        taxRepository.deleteById(taxNumber);
                        log.info("Person with TaxNumber={} deleted successfully from Kafka", taxNumber);
                    },
                    () -> log.warn("Person with TaxNumber={} not found for deletion. Might already be deleted.", taxNumber)
            );
        } catch (Exception ex) {
            log.error("Error deleting person from Kafka event : {}", ex.getMessage(), ex);
            throw new KafkaConsumerException("Failed to delete person", ex);
        }
    }
}