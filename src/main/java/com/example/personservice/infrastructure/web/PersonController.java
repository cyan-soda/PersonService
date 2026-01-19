package com.example.personservice.infrastructure.web;

import com.example.personservice.application.dto.person.OperationResponseDto;
import com.example.personservice.application.service.PersonService;
import com.example.personservice.application.dto.person.CreatePersonRequestDto;
import com.example.personservice.application.dto.person.PersonResponseDto;
import com.example.personservice.application.dto.person.UpdatePersonRequestDto;
import com.example.personservice.infrastructure.validation.validator.ValidTaxNumber;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/person")
@Validated
@Slf4j
public class PersonController {
    private final PersonService service;

    public PersonController(PersonService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<OperationResponseDto> createPerson(
            @Valid @RequestBody CreatePersonRequestDto request
    ) {
        OperationResponseDto response = service.createPerson(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping
    public ResponseEntity<List<PersonResponseDto>> getAll() {
        List<PersonResponseDto> persons = service.findAll();
        return ResponseEntity.ok(persons);
    }

    @GetMapping("/search")
    public ResponseEntity<Page<PersonResponseDto>> searchPerson(
            @RequestParam(required = false) String firstNamePrefix,
            @RequestParam(required = false) String lastNamePrefix,
            @RequestParam Integer minAge,
            Pageable pageable
    ) {
        Page<PersonResponseDto> persons = service.findByNameAndAge(
                firstNamePrefix,
                lastNamePrefix,
                minAge,
                pageable
        );
        return ResponseEntity.ok(persons);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PersonResponseDto> getPersonById(
            @PathVariable UUID id
    ) {
        PersonResponseDto person = service.findById(id);
        return ResponseEntity.ok(person);
    }

    @GetMapping("/tax/{taxNumber}")
    public ResponseEntity<PersonResponseDto> getByTaxNumber(
            @PathVariable
            @ValidTaxNumber String taxNumber
    ) {
        PersonResponseDto person = service.findByTaxNumber(taxNumber);
        return ResponseEntity.ok(person);
    }

    @PutMapping("/{id}")
    public ResponseEntity<OperationResponseDto> updatePerson(
            @PathVariable UUID id,
            @Valid @RequestBody UpdatePersonRequestDto request) {

        OperationResponseDto response = service.updatePerson(id, request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<OperationResponseDto> deletePerson(@PathVariable UUID id) {
        OperationResponseDto response = service.deletePerson(id);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}