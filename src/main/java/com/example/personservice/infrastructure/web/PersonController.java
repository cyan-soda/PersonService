package com.example.personservice.infrastructure.web;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.dto.CreatePersonRequestDto;
import com.example.personservice.dto.PersonResponseDto;
import com.example.personservice.dto.UpdatePersonRequestDto;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/person")
public class PersonController {
    private final PersonService service;

    public PersonController(PersonService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<PersonResponseDto> createPerson(@RequestBody CreatePersonRequestDto request) {
        PersonResponseDto person = service.createPerson(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(person);
    }

    @GetMapping
    public ResponseEntity<List<PersonResponseDto>> getAll() {
        List<PersonResponseDto> persons = service.findAll();
        return ResponseEntity.ok(persons);
    }

    @GetMapping("/search")
    public ResponseEntity<List<PersonResponseDto>> getFilter() {
        List<PersonResponseDto> persons = service.findByNameAndAge();
        return ResponseEntity.ok(persons);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PersonResponseDto> getPersonById(@PathVariable Long id) {
        return service
                .findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/tax/{taxNumber}")
    public ResponseEntity<PersonResponseDto> getByTaxNumber(@PathVariable String taxNumber) {
        return service
                .findByTaxNumber(taxNumber)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public ResponseEntity<PersonResponseDto> updatePerson(
            @PathVariable Long id,
            @RequestBody UpdatePersonRequestDto request) {
        PersonResponseDto person = service.updatePerson(id, request);
        return ResponseEntity.ok(person);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public ResponseEntity<Void> deletePerson(@PathVariable Long id) {
        service.deletePerson(id);
        return ResponseEntity.noContent().build();
    }
}