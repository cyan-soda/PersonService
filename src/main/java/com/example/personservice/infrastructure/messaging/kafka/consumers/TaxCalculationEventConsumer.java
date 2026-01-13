package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TaxCalculationEventConsumer {

    private final PersonService service;

    public TaxCalculationEventConsumer(
            PersonService service
    ) {
        this.service = service;
    }

    @KafkaListener(topics = "tax.calculation.kafka", groupId = "tax.calculation.group")
    public void handleTaxCalculationEvent(
            TaxCalculationEvent event,
            String topic,
            int partition,
            String key
//            Acknowledgement
    ) {
        try {

            if (event == null || event.getTaxId() == null) {

                log.error("Received null event");
                return;

            }

            log.info("Received tax calculation event for Person with tax number: {}",
                    event.getTaxId());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}