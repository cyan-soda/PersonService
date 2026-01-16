package com.example.personservice.application.dto.person;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.UUID;

@Getter
@Setter
public class PersonResponseDto {
    private UUID id;
    private String firstName;
    private String lastName;
    private int age;
    private String taxNumber;
    private BigDecimal taxDebt;
}