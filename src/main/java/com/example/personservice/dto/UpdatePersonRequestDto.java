package com.example.personservice.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
public class UpdatePersonRequestDto {
    private String firstName;
    private String lastName;
    private LocalDate dateOfBirth;
//    private BigDecimal taxDebt;
}