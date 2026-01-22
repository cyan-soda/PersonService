package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.ErrorClassifier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonSingleConsumer {

    private final PersonService personService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ErrorClassifier errorClassifier;

    @KafkaListener(
            topics = "person.kafka.single",
            containerFactory = "personSingleContainerFactory",
            groupId = "person.single.group"
    )
    public void consumeSingle(PersonEvent event, Acknowledgment ack) {
        try {
            log.info("[Single] Processing event: {}", event.getEventType());
            processEvent(event);
            ack.acknowledge();
        } catch (Exception e) {
            handleError(event, e);
            ack.acknowledge(); // Commit offset because we moved it to retry/dlt
        }
    }

    private void handleError(PersonEvent event, Exception e) {
        ErrorClassifier.ErrorType type = errorClassifier.classifyError(e);
        if (type == ErrorClassifier.ErrorType.FATAL) {
            log.error("[Single] Fatal error. Sending to DLT.");
            kafkaTemplate.send("person.kafka.dlt", event.getPerson().getTaxNumber(), event);
        } else {
            log.info("[Single] Retryable error. Sending to Retry Topic.");
            // Send to shared retry topic
            kafkaTemplate.send("person.kafka.retry", event.getPerson().getTaxNumber(), event);
        }
    }

    private void processEvent(PersonEvent event) {
        switch (event.getEventType()) {
            case CREATE -> personService.createPersonFromEvent(event.getPerson());
            case UPDATE -> personService.updatePersonFromEvent(event.getPerson());
            case DELETE -> personService.deletePersonFromEvent(event.getPerson());
        }
    }
}
