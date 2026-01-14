package com.example.personservice.application.service;

import com.example.personservice.domain.model.Person;
import com.example.personservice.domain.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import com.example.personservice.infrastructure.messaging.kafka.producers.TaxCalculationEventProducer;
import jakarta.transaction.Transactional;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Optional;

@Slf4j
@Getter
@Setter
@Service
@Transactional
public class TaxService {
    private final PersonRepository repository;
    private final TaxCalculationEventProducer producer;

    public TaxService(PersonRepository repository, TaxCalculationEventProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    public void addTaxDebt(String taxNumber, BigDecimal amount) {
        log.info("Adding taxDebt={} to person with taxNumber={}",
                amount, taxNumber);
        try {
            Optional<Person> person = repository.findByTaxNumber(taxNumber);
            if (person.isPresent()) {
                TaxCalculationEvent event = new TaxCalculationEvent(TaxCalculationEvent.EventType.ADD, taxNumber, amount);
                producer.publishTaxDebtCreated(event);
                log.info("Adding amount={} for person with taxNumber={}",
                        amount, taxNumber);
            }
        } catch (Exception e) {
            log.error("Error adding amount={} to person with taxNumber={}",
                    amount, taxNumber);
            throw new RuntimeException(e);
        }
    }

    public BigDecimal getTaxDebt(String taxNumber) {
        Optional<Person> person = repository.findByTaxNumber(taxNumber);
        return person.map(Person::getTaxDebt)
                .orElse(BigDecimal.ZERO);
    }
}