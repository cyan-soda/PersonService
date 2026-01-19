package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.TaxService;
import com.example.personservice.domain.model.Person;
import com.example.personservice.infrastructure.repository.PersonRepository;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaxCalculationEventConsumer {

    private final PersonRepository repository;
    private final AtomicInteger count = new AtomicInteger(0);
    private final TaxService taxService;

//    @KafkaListener(
//            topics = "tax.calculation.kafka",
//            groupId = "tax.calculation.group",
//            containerFactory = "taxKafkaListenerContainerFactory"
//    )
//    public void handleTaxCalculationEvent(
//            @Payload TaxCalculationEvent event,
//            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic
//    ) {
//
//        log.info("Received TaxCalculationEvent from topic: {}, event: {}",
//                topic, event);
//
//        try {
//            if (event.getTaxId() == null || event.getTaxId().trim().isEmpty()) {
//                log.warn("Received TaxCalculationEvent with null or empty taxId: {}", event);
//                return;
//            }
//
//            if (event.getAmount() == null || event.getAmount().compareTo(java.math.BigDecimal.ZERO) <= 0) {
//                log.warn("Received TaxCalculationEvent with invalid amount: {}", event);
//                return;
//            }
//
//            String taxNumber = event.getTaxId();
//            BigDecimal amount = event.getAmount();
//
//            Person person = repository.findByTaxNumber(taxNumber)
//                    .orElseThrow(() -> new RuntimeException("Person not found with tax number: " + taxNumber));
//            person.addTaxDebt(amount);
//            repository.save(person);
//            log.info("Successfully processed TaxCalculationEvent for taxId: {}, amount: {}, total debt: {}",
//                    taxNumber, amount, person.getTaxDebt());
//
//        } catch (Exception e) {
//            log.error("Error processing TaxCalculationEvent: {}", event, e);
//        }
//    }

    @KafkaListener(
            topics = "tax.calculation.kafka",
            groupId = "tax.calculation.group",
            containerFactory = "taxKafkaListenerContainerFactory"
    )
    public void handleBatchTaxCalculationEvent(
            @Payload List<TaxCalculationEvent> events,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment acknowledgment) {

        log.info("Processing batch of {} tax calculation events", events.size());

        int processed = 0;

        for (int i = 0; i < events.size(); i ++) {
            TaxCalculationEvent event = events.get(i);
            int partition = partitions.get(i);
            long offset = offsets.get(i);

            String taxNumber = event.getTaxId();
            BigDecimal amount = event.getAmount();

            log.info("Processing event {}/{} - TaxId: {}, Amount: {}, Partition: {}, Offset: {}",
                    i + 1, events.size(), event.getTaxId(), event.getAmount(), partition, offset);

            try {
                // Simulate error conditions for testing
//                simulateErrorConditions(event, count.incrementAndGet());

                // Process the tax calculation event
                Person person = repository.findByTaxNumber(taxNumber)
                        .orElseThrow(() -> new RuntimeException("Person not found with tax number: " + taxNumber));
                person.addTaxDebt(amount);
                repository.save(person);
                processed++;

                log.info("Successfully processed tax calculation for TaxId: {}", event.getTaxId());

            } catch (Exception e) {
                log.error("Failed to process tax calculation event for TaxId: {} at position {} in batch. Error: {}",
                        event.getTaxId(), i, e.getMessage(), e);

                // For batch processing, we have different strategies:
                // Strategy 1: Fail the entire batch (current implementation)
                // Strategy 2: Skip failed records and process the rest (would need individual ACK)
                throw new RuntimeException("Batch processing failed at position " + i + " for TaxId: " + event.getTaxId(), e);
            }

        }

        acknowledgment.acknowledge();
        log.info("Successfully processed and acknowledged batch of {} tax calculation events", processed);
    }
}
