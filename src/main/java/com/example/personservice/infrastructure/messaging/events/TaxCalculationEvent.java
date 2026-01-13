package com.example.personservice.infrastructure.messaging.events;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class TaxCalculationEvent extends DomainEvent<TaxCalculationEvent.EventType> {
    public enum EventType { ADD }

    private String taxId;
    private BigDecimal amount;

    public TaxCalculationEvent() {
        super(null);
    }

    public TaxCalculationEvent(EventType eventType, String taxId, BigDecimal amount) {
        super(eventType);
        this.taxId = taxId;
        this.amount = amount;
    }
}