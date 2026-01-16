package com.example.personservice.application.dto.person;

import com.example.personservice.infrastructure.validation.validator.ValidTaxNumber;
import jakarta.validation.constraints.*;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;

@Getter
@Setter
public class CreatePersonRequestDto {
    @NotBlank(message = "First name cannot be null")
    @Size(min = 2, max = 50, message = "First name must be between 2 and 50 characters")
    @Pattern(regexp = "^[a-zA-Z\\s'-]+$", message = "First name must contain only letters, spaces, hyphens, and apostrophes")
    private String firstName;

    @NotBlank(message = "Last name cannot be null")
    @Size(min = 2, max = 50, message = "First name must be between 2 and 50 characters")
    @Pattern(regexp = "^[a-zA-Z\\s'-]+$", message = "First name must contain only letters, spaces, hyphens, and apostrophes")
    private String lastName;

    @NotNull(message = "Date of birth is required")
    @Past(message = "Date of birth must be in the past")
    private LocalDate dateOfBirth;

    @NotBlank(message = "Tax number is required")
    @ValidTaxNumber
    private String taxNumber;
}