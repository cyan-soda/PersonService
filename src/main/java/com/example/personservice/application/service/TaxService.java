package com.example.personservice.application.service;

import com.example.personservice.application.dto.person.OperationResponseDto;
import com.example.personservice.application.dto.tax.TaxResponseDto;
import com.example.personservice.domain.model.Tax;
import com.example.personservice.infrastructure.exception.KafkaProducerException;
import com.example.personservice.infrastructure.exception.PersonNotFoundException; // Can be replaced with a more generic NotFoundException
import com.example.personservice.infrastructure.exception.TaxCalculationException;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import com.example.personservice.infrastructure.messaging.kafka.producers.TaxCalculationEventProducer;
import com.example.personservice.infrastructure.repository.TaxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaxService {
    private final TaxRepository taxRepository;
    private final TaxCalculationEventProducer producer;

    public OperationResponseDto handleTaxCalculation(String taxNumber, BigDecimal amount) {
        log.info("Starting tax debt addition process for taxNumber={}, amount={}", taxNumber, amount);

        if ("TAX001".equals(taxNumber)) {
            throw new RecoverableDataAccessException("Simulated Tax Calculation Error");
        } else if (BigDecimal.ZERO.equals(amount)) {
            throw new IllegalArgumentException("Simulate Fatal Error for Tax Calculation");
        }

        try {
            // Check if tax entity exists before publishing event
            if (!taxRepository.existsById(taxNumber)) {
                log.warn("Tax number {} not found.", taxNumber);
                throw PersonNotFoundException.byTaxNumber(taxNumber);
            }

            TaxCalculationEvent event = new TaxCalculationEvent(TaxCalculationEvent.EventType.ADD, taxNumber, amount);
            publishTaxCalculationEvent(event, taxNumber, amount);

            log.info("Successfully initiated tax debt addition for taxNumber={}, amount={}", taxNumber, amount);
            return new OperationResponseDto("Tax debt addition request successfully sent to Kafka for processing", "ADD_TAX_DEBT");

        } catch (PersonNotFoundException | KafkaProducerException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("Unexpected error while adding tax debt for taxNumber={}, amount={}: {}", taxNumber, amount, ex.getMessage(), ex);
            throw new TaxCalculationException("Unexpected error occurred while processing tax debt", ex);
        }
    }

    public TaxResponseDto getTaxDebt(String taxNumber) {
        log.info("Retrieving tax debt for taxNumber={}", taxNumber);
        try {
            Tax tax = findTaxByTaxNumber(taxNumber);
            BigDecimal taxDebt = tax.getTaxDebt();
            log.info("Retrieved tax debt={} for taxNumber={}", taxDebt, taxNumber);

            TaxResponseDto response = new TaxResponseDto();
            response.setAmount(taxDebt);
            return response;
        } catch (PersonNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("Unexpected error while retrieving tax debt for taxNumber={}: {}", taxNumber, ex.getMessage(), ex);
            throw new TaxCalculationException("Unexpected error occurred while retrieving tax debt", ex);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void processBatch(List<TaxCalculationEvent> events) {
        log.info("[Tax Service] Starting atomic batch processing for {} events", events.size());
        for (TaxCalculationEvent event : events) {
            processTaxCalculationEvent(event);
        }
        log.info("[Tax Service] Batch DB operations completed successfully (pending commit)");
    }

    public void processTaxCalculationEvent(TaxCalculationEvent event) {
        String taxNumber = event.getTaxId();
        BigDecimal amount = event.getAmount();

        if ("TAX888".equals(taxNumber)) {
            throw new RuntimeException("Simulated transient error for " + taxNumber);
        }
        if ("TAX889".equals(taxNumber)) {
            throw new RuntimeException("Simulated fatal error for " + taxNumber);
        }

        try {
            Tax tax = taxRepository.findById(taxNumber)
                    .orElseThrow(() -> new PersonNotFoundException("Tax entity with number " + taxNumber + " not found"));

            tax.addTaxDebt(amount);
            taxRepository.save(tax);

            log.info("Added tax debt of {} to tax entity {}", amount, taxNumber);
        } catch (Exception ex) {
            log.error("Error processing taxNumber={}: {}", taxNumber, ex.getMessage());
            throw ex; // Re-throw to trigger transactional rollback
        }
    }

    private Tax findTaxByTaxNumber(String taxNumber) {
        return taxRepository.findById(taxNumber)
                .orElseThrow(() -> {
                    log.warn("Tax entity not found with taxNumber={}", taxNumber);
                    return PersonNotFoundException.byTaxNumber(taxNumber);
                });
    }

    private void publishTaxCalculationEvent(TaxCalculationEvent event, String taxNumber, BigDecimal amount) {
        try {
            producer.publishTaxDebtCreated(event);
            log.debug("Successfully published tax calculation event for taxNumber={}, amount={}", taxNumber, amount);
        } catch (Exception ex) {
            log.error("Failed to publish tax calculation event for taxNumber={}, amount={}: {}", taxNumber, amount, ex.getMessage(), ex);
            throw new KafkaProducerException("Failed to publish tax calculation event", ex);
        }
    }
}
