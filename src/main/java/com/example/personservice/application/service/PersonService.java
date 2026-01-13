package com.example.personservice.application.service;

import com.example.personservice.domain.model.Person;
import com.example.personservice.domain.repository.PersonRepository;
import com.example.personservice.application.dto.CreatePersonRequestDto;
import com.example.personservice.application.dto.PersonResponseDto;
import com.example.personservice.application.dto.UpdatePersonRequestDto;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.producers.PersonEventProducer;
import jakarta.transaction.Transactional;
import lombok.Getter;
import lombok.Setter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
@Setter
@Service
@Transactional
public class PersonService {
    private final PersonRepository repository;
    private final PersonEventProducer producer;

    public PersonService(PersonRepository repository, PersonEventProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    public PersonResponseDto createPerson(CreatePersonRequestDto request) {
        if (repository.existsByTaxNumber(request.getTaxNumber())) {
            throw new DuplicateKeyException(request.getTaxNumber());
        }

        Person person = new Person();
        person.setFirstName(request.getFirstName());
        person.setLastName(request.getLastName());
        person.setDateOfBirth(request.getDateOfBirth());
        person.setTaxNumber(request.getTaxNumber());

        PersonEvent event = new PersonEvent(PersonEvent.EventType.CREATE, person);
        producer.publishPersonCreated(event);

        return mapToDto(person);
    }

    public PersonResponseDto updatePerson(Long id, UpdatePersonRequestDto request) {
        Person person = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Person not found."));

        person.updatePersonInfo(
                request.getFirstName(),
                request.getLastName(),
                request.getDateOfBirth()
        );

        PersonEvent event = new PersonEvent(PersonEvent.EventType.UPDATE, person);
        producer.publishPersonUpdated(event);

        return mapToDto(person);
    }

    public void deletePerson(Long id) {
        Person person = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Person not found."));

        PersonEvent event = new PersonEvent(PersonEvent.EventType.DELETE, person);
        producer.publishPersonDeleted(event);
    }

    public List<PersonResponseDto> findAll() {
        return repository
                .findAll()
                .stream()
                .map(this::mapToDto)
                .collect(Collectors.toList());
    }

    public Optional<PersonResponseDto> findById(Long id) {
        return repository.findById(id).map(this::mapToDto);
    }

    public Optional<PersonResponseDto> findByTaxNumber(String taxNumber) {
        return repository.findByTaxNumber(taxNumber).map(this::mapToDto);
    }

    public List<PersonResponseDto> findByNameAndAge() {
        return repository.findByNameAndAge("Mi", 30)
                .stream()
                .map(this::mapToDto)
                .toList();
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