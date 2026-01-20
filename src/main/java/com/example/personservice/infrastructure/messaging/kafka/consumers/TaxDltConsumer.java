package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TaxDltConsumer {

    @KafkaListener(
            topics = "tax.calculation.kafka-dlt",
            groupId = "tax.calculation.dlt.group"
    )
    public void processDltMessages(
            TaxCalculationEvent event,
            @Header(KafkaHeaders.ORIGINAL_OFFSET) byte[] offset,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {

//        log.error("[DLT] Alert! Message failed processing. " +
//                        "TaxId: {}, Reason: {}, Original Offset: {}",
//                event.getTaxId(), exceptionMessage, new String(offset));

        log.error("[DLT] An error occurred while processing the message with taxId: {}",
                event.getTaxId());

        // 1. Store in a 'failed_events' database table
        // 2. Or send an alert
    }
}