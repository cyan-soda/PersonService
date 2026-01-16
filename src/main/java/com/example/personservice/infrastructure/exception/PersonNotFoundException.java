package com.example.personservice.infrastructure.exception;

import java.util.UUID;

public class PersonNotFoundException extends RuntimeException {
    public PersonNotFoundException(String message) {
        super(message);
    }

    public PersonNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public static PersonNotFoundException byId(UUID id) {
        return new PersonNotFoundException("Person not found with ID: " + id);
    }

    public static PersonNotFoundException byTaxNumber(String taxNumber) {
        return new PersonNotFoundException("Person not found with tax number: " + taxNumber);
    }
}