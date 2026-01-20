package com.example.personservice.infrastructure.exception;

public class TaxCalculationException extends RuntimeException {
    public TaxCalculationException(String message) {
        super(message);
    }

    public TaxCalculationException(String message, Throwable cause) {
        super(message, cause);
    }

    public static TaxCalculationException databaseError(String operation) {
        return new TaxCalculationException("Database error during " + operation);
    }

    public static TaxCalculationException processingError(String taxNumber) {
        return new TaxCalculationException("Failed to process tax calculation event for tax number: " + taxNumber);
    }
}
