package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.domain.model.Person;
import com.example.personservice.infrastructure.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;

@Slf4j
@Component
public class TaxCalculationEventConsumer {

    private final PersonRepository repository;

    public TaxCalculationEventConsumer(PersonRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            topics = "tax.calculation.kafka",
            groupId = "tax.calculation.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    public void handleTaxCalculationEvent(
            @Payload TaxCalculationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic
    ) {

        log.info("Received TaxCalculationEvent from topic: {}, event: {}",
                topic, event);

        try {
            if (event.getTaxId() == null || event.getTaxId().trim().isEmpty()) {
                log.warn("Received TaxCalculationEvent with null or empty taxId: {}", event);
                return;
            }

            if (event.getAmount() == null || event.getAmount().compareTo(java.math.BigDecimal.ZERO) <= 0) {
                log.warn("Received TaxCalculationEvent with invalid amount: {}", event);
                return;
            }

            String taxNumber = event.getTaxId();
            BigDecimal amount = event.getAmount();

            Person person = repository.findByTaxNumber(taxNumber)
                    .orElseThrow(() -> new RuntimeException("Person not found with tax number: " + taxNumber));
            person.addTaxDebt(amount);
            repository.save(person);
            log.info("Successfully processed TaxCalculationEvent for taxId: {}, amount: {}, total debt: {}",
                    taxNumber, amount, person.getTaxDebt());

        } catch (Exception e) {
            log.error("Error processing TaxCalculationEvent: {}", event, e);
        }
    }
}
