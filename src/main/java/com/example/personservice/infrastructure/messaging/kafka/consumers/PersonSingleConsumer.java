package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.ErrorClassifier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonSingleConsumer {

    private final PersonService personService;
    private final ErrorClassifier errorClassifier;

    @KafkaListener(
            topics = "person.kafka.single",
            groupId = "person.single.group",
            containerFactory = "personSingleContainerFactory"
    )
    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 10000),
            autoCreateTopics = "true",
            dltTopicSuffix = ".dlt",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            include = Exception.class
    )
    public void consumeSingle(PersonEvent event) {
        log.info("[Single] Processing event: {}", event.getEventType());

        try {
            processEvent(event);
        } catch (Exception e) {
            ErrorClassifier.ErrorType type = errorClassifier.classifyError(e);

            if (type == ErrorClassifier.ErrorType.FATAL) {
                log.error("[Single] Fatal error → skipping retries, sending to DLT");
                throw new DataIntegrityViolationException(e.getMessage());
            }

            log.warn("[Single] Retryable error → triggering retry");
            throw e;
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
