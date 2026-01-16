package com.example.personservice.application.dto.tax;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
public class TaxResponseDto {
    private BigDecimal amount;
}