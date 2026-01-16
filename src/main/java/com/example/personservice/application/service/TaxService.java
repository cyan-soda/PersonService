package com.example.personservice.application.service;

import com.example.personservice.application.dto.person.OperationResponseDto;
import com.example.personservice.domain.model.Person;
import com.example.personservice.infrastructure.exception.KafkaProducerException;
import com.example.personservice.infrastructure.exception.PersonNotFoundException;
import com.example.personservice.infrastructure.exception.TaxCalculationException;
import com.example.personservice.infrastructure.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import com.example.personservice.infrastructure.messaging.kafka.producers.TaxCalculationEventProducer;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
@Transactional
public class TaxService {
    private final PersonRepository repository;
    private final TaxCalculationEventProducer producer;

    public TaxService(PersonRepository repository, TaxCalculationEventProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    public OperationResponseDto handleTaxCalculation(String taxNumber, BigDecimal amount) {
        log.info("Starting tax debt addition process for taxNumber={}, amount={}", taxNumber, amount);

        try {
            Person person = findPersonByTaxNumber(taxNumber);
            log.debug("Found person with id={} for taxNumber={}", person.getId(), taxNumber);

            TaxCalculationEvent event = new TaxCalculationEvent(
                    TaxCalculationEvent.EventType.ADD,
                    taxNumber,
                    amount
            );

            publishTaxCalculationEvent(event, taxNumber, amount);
            log.info("Successfully initiated tax debt addition for taxNumber={}, amount={}", taxNumber, amount);
            return new OperationResponseDto(
                    "Tax debt addition request successfully sent to Kafka for processing",
                    "ADD_TAX_DEBT"
            );

        } catch (PersonNotFoundException | KafkaProducerException ex) {
            throw ex;
        } catch (DataAccessException ex) {
            log.error("Database error while processing tax debt for taxNumber={}, amount={}: {}",
                    taxNumber, amount, ex.getMessage(), ex);
            throw new TaxCalculationException("Database error occurred while processing tax debt", ex);
        } catch (Exception ex) {
            log.error("Unexpected error while adding tax debt for taxNumber={}, amount={}: {}",
                    taxNumber, amount, ex.getMessage(), ex);
            throw new TaxCalculationException("Unexpected error occurred while processing tax debt", ex);
        }
    }

    public BigDecimal getTaxDebt(String taxNumber) {
        log.info("Retrieving tax debt for taxNumber={}", taxNumber);

        try {
            Person person = findPersonByTaxNumber(taxNumber);
            BigDecimal taxDebt = person.getTaxDebt();

            log.info("Retrieved tax debt={} for taxNumber={}", taxDebt, taxNumber);
            return taxDebt;

        } catch (PersonNotFoundException ex) {
            throw ex;
        } catch (DataAccessException ex) {
            log.error("Database error while retrieving tax debt for taxNumber={}: {}",
                    taxNumber, ex.getMessage(), ex);
            throw new TaxCalculationException("Database error occurred while retrieving tax debt", ex);
        } catch (Exception ex) {
            log.error("Unexpected error while retrieving tax debt for taxNumber={}: {}",
                    taxNumber, ex.getMessage(), ex);
            throw new TaxCalculationException("Unexpected error occurred while retrieving tax debt", ex);
        }
    }

    private Person findPersonByTaxNumber(String taxNumber) {
        try {
            return repository.findByTaxNumber(taxNumber)
                    .orElseThrow(() -> {
                        log.warn("Person not found with taxNumber={}", taxNumber);
                        return PersonNotFoundException.byTaxNumber(taxNumber);
                    });
        } catch (DataAccessException ex) {
            log.error("Database error while finding person with taxNumber={}: {}", taxNumber, ex.getMessage(), ex);
            throw new TaxCalculationException("Database error occurred while finding person", ex);
        }
    }

    private void publishTaxCalculationEvent(TaxCalculationEvent event, String taxNumber, BigDecimal amount) {
        try {
            producer.publishTaxDebtCreated(event);
            log.debug("Successfully published tax calculation event for taxNumber={}, amount={}", taxNumber, amount);
        } catch (Exception ex) {
            log.error("Failed to publish tax calculation event for taxNumber={}, amount={}: {}",
                    taxNumber, amount, ex.getMessage(), ex);
            throw new KafkaProducerException("Failed to publish tax calculation event", ex);
        }
    }
}
