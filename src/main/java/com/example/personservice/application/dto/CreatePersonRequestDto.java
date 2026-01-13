package com.example.personservice.application.dto;

import lombok.Data;

import java.time.LocalDate;

@Data
public class CreatePersonRequestDto {
    private String firstName;
    private String lastName;
    private LocalDate dateOfBirth;
    private String taxNumber;
//    private BigDecimal taxDebt;
}