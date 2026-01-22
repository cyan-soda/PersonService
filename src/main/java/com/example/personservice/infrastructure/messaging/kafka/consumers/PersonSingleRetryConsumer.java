package com.example.personservice.infrastructure.messaging.kafka.consumers;

import com.example.personservice.application.service.PersonService;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.RetryRouter;
import com.example.personservice.infrastructure.messaging.redis.RetryLatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class PersonSingleRetryConsumer {

    private final PersonService service;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RetryLatch retryLatch;
    private final RetryRouter router;

    private static final int MAX_RETRIES = 3;

    @KafkaListener(
            topics = {"person.kafka.retry-1", "person.kafka.retry-2", "person.kafka.retry-3"},
            groupId = "person.retry.group",
            containerFactory = "personSingleContainerFactory"
    )
    public void consumeRetry(ConsumerRecord<String, PersonEvent> record,
                             @Header(name = "retry-count", defaultValue = "1") String retryCountStr,
                             Acknowledgment acknowledgment) {

        int retryCount = Integer.parseInt(retryCountStr);
        String taxNumber = record.key();
        PersonEvent event = record.value();

        log.info("[RetryWorker] Processing {} from topic {} (Attempt {})", taxNumber, record.topic(), retryCount);

        try {
            processEvent(event);

            // SUCCESS
            log.info("[RetryWorker] Success for {}. Notifying Batch Consumer.", taxNumber);
            retryLatch.notifyResult(taxNumber, RetryLatch.RetryStatus.SUCCESS);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("[RetryWorker] Failed attempt {} for {}", retryCount, taxNumber);

            // ack so this message leaves this retry-N topic
            acknowledgment.acknowledge();

            if (retryCount >= 3) {
                router.sendToDlt(event, taxNumber);
                retryLatch.notifyResult(taxNumber, RetryLatch.RetryStatus.DLT);
            } else {
                router.routeToNextTopic(event, taxNumber, retryCount);
            }
        }

    }

    private void processEvent(PersonEvent event) {
        switch (event.getEventType()) {
            case CREATE -> service.createPersonFromEvent(event.getPerson());
            case UPDATE -> service.updatePersonFromEvent(event.getPerson());
            case DELETE -> service.deletePersonFromEvent(event.getPerson());
        }
    }

}
