package com.example.personservice.infrastructure.messaging.redis;

import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class PersonEventBufferService {

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String STATE_PREFIX = "person:state:"; // Stores states: RETRYING or DLT
    private static final String BUFFER_PREFIX = "person:buffer:"; // Stores list of waiting events

    private static final String STATE_RETRYING = "RETRYING";
    private static final String STATE_DLT = "DLT";
    private static final Duration STATE_TTL = Duration.ofHours(1); // TTL for state keys

    /**
     * Checks if a key is marked as RETRYING or DLT.
     */
    public boolean isKeyInRetry(String taxNumber) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(STATE_PREFIX + taxNumber));
    }

    /**
     * Marks a key as being in a retry loop.
     */
    public void markAsRetrying(String taxNumber) {
        String key = STATE_PREFIX + taxNumber;
        redisTemplate.opsForValue().set(key, STATE_RETRYING, STATE_TTL);
        log.info("[Redis] Marked key {} as {}.", taxNumber, STATE_RETRYING);
    }

    /**
     * Marks a key as having failed fatally.
     */
    public void markAsDlt(String taxNumber) {
        String key = STATE_PREFIX + taxNumber;
        redisTemplate.opsForValue().set(key, STATE_DLT, STATE_TTL);
        log.info("[Redis] Marked key {} as {}.", taxNumber, STATE_DLT);
    }

    /**
     * Adds a list of events to the tail of the buffer for a given key.
     */
    public void bufferEvents(String taxNumber, List<PersonEvent> events) {
        if (events == null || events.isEmpty()) return;
        String key = BUFFER_PREFIX + taxNumber;
        redisTemplate.opsForList().rightPushAll(key, events.toArray());
        redisTemplate.expire(key, STATE_TTL);
        log.info("[Redis] Buffered {} event(s) for key: {}", events.size(), taxNumber);
    }

    /**
     * Retrieves and removes the next event from the head of the buffer.
     * @return The next PersonEvent, or null if the buffer is empty.
     */
    public PersonEvent popNextBufferedEvent(String taxNumber) {
        Object event = redisTemplate.opsForList().leftPop(BUFFER_PREFIX + taxNumber);
        return event != null ? (PersonEvent) event : null;
    }

    /**
     * Clears the state and the buffer for a key once the entire chain is successfully processed.
     */
    public void clearRetryState(String taxNumber) {
        redisTemplate.delete(List.of(STATE_PREFIX + taxNumber, BUFFER_PREFIX + taxNumber));
        log.info("[Redis] Cleared retry state and buffer for key {}.", taxNumber);
    }
}
