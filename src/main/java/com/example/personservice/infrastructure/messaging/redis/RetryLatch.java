package com.example.personservice.infrastructure.messaging.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class RetryLatch {

    private final StringRedisTemplate redisTemplate;

    private static final String SIGNAL_KEY_PREFIX = "kafka:retry:signal:";
    private static final String RETRY_FLAG_PREFIX = "kafka:retry:flag:";

    public enum RetryStatus { SUCCESS, DLT, TIMEOUT }

    /**
     * NEW METHOD: Clears any stale signals for this tax number.
     * This must be called by the BatchConsumer before processing events for a key.
     * This prevents the "Ghost Signal" issue where a current event reads a DLT signal
     * left over from a previous test run.
     */
    public void clearSignal(String taxNumber) {
        String key = SIGNAL_KEY_PREFIX + taxNumber;
        Boolean deleted = redisTemplate.delete(key);
        if (Boolean.TRUE.equals(deleted)) {
            log.info("Cleared stale retry signal for key: {}", key);
        }
    }

    // Called by batch consumer
    // Blocks until result is available or timeout
    public RetryStatus waitForResult(String taxNumber, long timeout) {
        String key = SIGNAL_KEY_PREFIX + taxNumber;
        log.info("Waiting for retry result for key: {}", key);

        // leftPop is a blocking call on Redis side
        String result = redisTemplate.opsForList().leftPop(key, timeout, TimeUnit.SECONDS);

        if (result == null) {
            log.warn("Timeout waiting for retry result for key: {}", key);
            return RetryStatus.TIMEOUT;
        }

        return RetryStatus.valueOf(result);
    }

    // Called by retry consumer
    // Pushes result to redis to unblock batch consumer
    public void notifyResult(String taxNumber, RetryStatus status) {
        String key = SIGNAL_KEY_PREFIX + taxNumber;
        log.info("Notifying result {} for key {}", status, key);

        // rightPush adds to the list, waking up the leftPop
        redisTemplate.opsForList().rightPush(key, status.name());
        redisTemplate.expire(key, 1, TimeUnit.HOURS);

        // Clear the "currently retrying" flag
        setRetrying(taxNumber, false);
    }

    public void setRetrying(String taxNumber, boolean isRetrying) {
        String key = RETRY_FLAG_PREFIX + taxNumber;
        if (isRetrying) {
            redisTemplate.opsForValue().set(key, "RETRYING", 1, TimeUnit.MINUTES);
        } else {
            redisTemplate.delete(key);
        }
    }

    public boolean isRetrying(String taxNumber) {
        String key = RETRY_FLAG_PREFIX + taxNumber;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }
}
