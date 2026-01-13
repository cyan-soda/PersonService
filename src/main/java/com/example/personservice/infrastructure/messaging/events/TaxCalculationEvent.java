package com.example.personservice.infrastructure.messaging.events;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class TaxCalculationEvent {
    private String action;
    private Long id;
    private String personId;
    private BigDecimal taxDebt;
    private LocalDateTime timestamp;
}