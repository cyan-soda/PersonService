package com.example.personservice.infrastructure.messaging.kafka.producers;

import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TaxCalculationEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TAX_CALCULATION_EVENTS_TOPIC = "tax.calculation.kafka";

    public TaxCalculationEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishTaxDebtCreated(TaxCalculationEvent event) {
        kafkaTemplate.send(TAX_CALCULATION_EVENTS_TOPIC, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("TaxCalculationEvent published successfully for taxId: {}", event.getTaxId());
                    } else {
                        log.error("Failed to publish TaxCalculationEvent for tax Id: {}", event.getTaxId(), ex);
                        throw new RuntimeException("Failed to publish TaxCalculationEvent");
                    }
                });
    }

}
