package com.example.personservice.infrastructure.messaging.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "kafka-person")
public class KafkaPersonProperties {

    public enum ProcessingMode { SINGLE, BATCH }

    private String singleTopic = "person.kafka.single";
    private String batchTopic = "person.kafka.batch";
    private ProcessingMode defaultMode = ProcessingMode.SINGLE;
    private boolean enableTopicSelection = true;

}
