package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.domain.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class TaxCalculationEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TaxCalculationEventConsumer.class);

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

        logger.info("Received TaxCalculationEvent from topic: {}, event: {}",
                topic, event);

        try {
            if (event.getTaxId() == null || event.getTaxId().trim().isEmpty()) {
                logger.warn("Received TaxCalculationEvent with null or empty taxId: {}", event);
                return;
            }

            if (event.getAmount() == null || event.getAmount().compareTo(java.math.BigDecimal.ZERO) <= 0) {
                logger.warn("Received TaxCalculationEvent with invalid amount: {}", event);
                return;
            }

            String taxNumber = event.getTaxId();
            BigDecimal amount = event.getAmount();

            repository.findByTaxNumber(taxNumber).ifPresentOrElse(
                    person -> {
                        person.addTaxDebt(amount);
                        repository.save(person);
                        logger.info("Successfully processed TaxCalculationEvent for taxId: {}, amount: {}, total debt: {}",
                                taxNumber, amount, person.getTaxDebt());
                    },
                    () -> logger.error("Add tax debt failed: Person with taxId: {} not found",
                            taxNumber)
            );

        } catch (Exception e) {
            logger.error("Error processing TaxCalculationEvent: {}", event, e);
        }
    }
}
