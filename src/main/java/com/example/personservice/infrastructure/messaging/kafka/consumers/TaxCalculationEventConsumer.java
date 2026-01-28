package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.domain.model.Tax;
import com.example.personservice.infrastructure.exception.KafkaConsumerException;
import com.example.personservice.infrastructure.exception.PersonNotFoundException;
import com.example.personservice.infrastructure.repository.TaxRepository;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaxCalculationEventConsumer {

    private final TaxRepository taxRepository;

    @KafkaListener(
            topics = "tax.calculation.kafka",
            groupId = "tax.calculation.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    public void handleBatchTaxCalculationEvent(
            @Payload List<TaxCalculationEvent> events,
            Acknowledgment acknowledgment) {
        log.info("Processing batch of {} tax calculation events", events.size());

        // This consumer's logic was largely commented out, but I've updated the handleEvent method
        // to show the correct implementation.
        for (TaxCalculationEvent event : events) {
            try {
                handleEvent(event);
            } catch (Exception e) {
                log.error("Error processing event: {}. Stopping batch processing and triggering error handler", event, e);
                throw e; // Let the container's error handler deal with it
            }
        }

        acknowledgment.acknowledge();
        log.info("Successfully processed and acknowledged batch of {} tax events", events.size());
    }

    @Transactional
    public void handleEvent(TaxCalculationEvent event) {
        String taxNumber = event.getTaxId();
        BigDecimal amount = event.getAmount();

        if ("TAX888".equals(taxNumber)) {
            log.error("!!! Simulating Transient Error for: {}", event.getTaxId());
            throw new KafkaConsumerException("Simulated Network Failure");
        }
        if ("TAX889".equals(taxNumber)) {
            log.error("!!! Simulating Fatal Error for: {}", event.getTaxId());
            throw new KafkaConsumerException("Simulated Fatal Failure");
        }

        log.info("Processing tax event for tax number: {}", taxNumber);

        if (taxNumber == null || taxNumber.trim().isEmpty() || amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("Invalid TaxCalculationEvent. Skipping record: {}", event);
            return;
        }

        Tax tax = taxRepository.findById(taxNumber)
                .orElseThrow(() -> PersonNotFoundException.byTaxNumber(taxNumber));

        tax.addTaxDebt(amount);
        taxRepository.save(tax);

        log.info("Successfully added tax debt for taxId: {}", taxNumber);
    }
}
