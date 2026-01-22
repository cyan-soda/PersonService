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

    public enum RetryStatus { SUCCESS, DLT, TIMEOUT }

    // called by batch consumer
    // block until result is available or timeout
    public RetryStatus waitForResult(String taxNumber, long timeout) {
        String key = SIGNAL_KEY_PREFIX + taxNumber;
        log.info("Waiting for retry result for key: {}", key);

        String result = redisTemplate.opsForList().leftPop(key, timeout, TimeUnit.SECONDS);

        if (result == null) {
            log.warn("Timeout waiting for retry result for key: {}", key);
            return RetryStatus.TIMEOUT;
        }

        return RetryStatus.valueOf(result);
    }

    // called by retry consumer
    // push result to redis to unblock batch consumer
    public void notifyResult(String taxNumber, RetryStatus status) {
        String key = SIGNAL_KEY_PREFIX + taxNumber;
        log.info("Notifying result {} for key {}", status, key);

        redisTemplate.opsForList().rightPush(key, status.name());
        redisTemplate.expire(key, 1, TimeUnit.HOURS);
    }

}
