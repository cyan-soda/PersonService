package com.example.personservice.infrastructure.validation.validator;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.lang.annotation.*;
import java.util.regex.Pattern;

public class TaxNumberValidator implements ConstraintValidator<ValidTaxNumber, String> {

    private static final Pattern TAX_NUMBER_PATTERN =
            Pattern.compile("^TAX\\d{3}$");

    @Override
    public void initialize(ValidTaxNumber constraintAnnotation) {
    }

    @Override
    public boolean isValid(String taxNumber, ConstraintValidatorContext context) {
        if (taxNumber == null || taxNumber.trim().isEmpty()) {
            return false;
        }

        return TAX_NUMBER_PATTERN.matcher(taxNumber.trim()).matches();
    }
}
