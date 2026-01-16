package com.example.personservice.infrastructure.validation.validator;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.math.BigDecimal;

public class TaxAmountValidator implements ConstraintValidator<ValidTaxAmount, BigDecimal> {

    private static final BigDecimal MIN_VALUE = new BigDecimal("0.01");
    private static final BigDecimal MAX_VALUE = new BigDecimal("999999.99");
    private static final int MAX_INTEGER_DIGITS = 6;
    private static final int MAX_FRACTION_DIGITS = 2;

    @Override
    public void initialize(ValidTaxAmount constraintAnnotation) {
    }

    @Override
    public boolean isValid(BigDecimal value, ConstraintValidatorContext context) {
        context.disableDefaultConstraintViolation();
        boolean isValid = true;

        if (value == null) {
            context.buildConstraintViolationWithTemplate("Tax debt amount is required").addConstraintViolation();
            return false;
        }

        if (value.compareTo(MIN_VALUE) < 0) {
            context.buildConstraintViolationWithTemplate("Tax debt amount must be greater than 0").addConstraintViolation();
            isValid = false;
        }

        if (value.compareTo(MAX_VALUE) > 0) {
            context.buildConstraintViolationWithTemplate("Tax debt amount cannot exceed 999,999.99").addConstraintViolation();
            isValid = false;
        }

        if (value.scale() > MAX_FRACTION_DIGITS || (value.precision() - value.scale()) > MAX_INTEGER_DIGITS) {
            context.buildConstraintViolationWithTemplate("Tax debt amount must have at most 6 digits and 2 decimal places").addConstraintViolation();
            isValid = false;
        }

        return isValid;
    }
}