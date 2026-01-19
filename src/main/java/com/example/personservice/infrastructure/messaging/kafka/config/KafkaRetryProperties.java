package com.example.personservice.infrastructure.messaging.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "kafka.retry")
public class KafkaRetryProperties {
    private int maxAttempts = 4;
    private long initialDelay = 1000; // ms
    private long maxDelay = 30000; // ms
    private double multiplier = 2.0;

    private int retryTopicPartitions = 3;
    private int retryTopicReplicas = 1;
    private int dltPartitions = 1;
    private String retryTopicSuffix = ".retry";
    private String dltTopicSuffix = ".dlt";
}
