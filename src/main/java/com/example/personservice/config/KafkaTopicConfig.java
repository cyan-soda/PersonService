package com.example.personservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic PersonEventTopic() {
        return TopicBuilder.name("person.kafka")
                .partitions(3)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic TaxCalculationTopic() {
        return TopicBuilder.name("tax.calculation.kafka")
                .partitions(3)
                .replicas(1)
                .build();
    }
}