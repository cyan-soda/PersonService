package com.example.personservice.application.dto;

import lombok.Data;

@Data
public class PersonResponseDto {
    private Long id;
    private String firstName;
    private String lastName;
    private int age;
    private String taxNumber;
//    private BigDecimal taxDebt;
}