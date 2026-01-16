package com.example.personservice.infrastructure.exception;

public class PersonAlreadyExistsException extends RuntimeException {
    public PersonAlreadyExistsException(String message) {
        super(message);
    }

    public PersonAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public static PersonAlreadyExistsException withTaxNumber(String taxNumber) {
        return new PersonAlreadyExistsException("Person already exists with tax number: " + taxNumber);
    }
}