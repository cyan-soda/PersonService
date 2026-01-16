package com.example.personservice.infrastructure.validation.validator;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = TaxAmountValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidTaxAmount {
    String message() default "Invalid tax amount";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
