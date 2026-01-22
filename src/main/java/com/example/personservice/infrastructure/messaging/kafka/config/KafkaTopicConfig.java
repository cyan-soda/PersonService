package com.example.personservice.infrastructure.messaging.kafka.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

    private final KafkaPersonProperties personProperties;

    @Bean
    public NewTopic PersonSingleTopic() {
        return TopicBuilder.name(personProperties.getSingleTopic())
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic PersonBatchTopic() {
        return TopicBuilder.name(personProperties.getBatchTopic())
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic PersonEventTopic() {
        return TopicBuilder.name("person.kafka")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic PersonRetryTopic1() {
        return TopicBuilder.name("person.kafka-retry-1")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic PersonRetryTopic2() {
        return TopicBuilder.name("person.kafka-retry-2")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic PersonRetryTopic3() {
        return TopicBuilder.name("person.kafka-retry-3")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic PersonDltTopic() {
        return TopicBuilder.name("person.kafka-dlt")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic TaxCalculationTopic() {
        return TopicBuilder.name("tax.calculation.kafka")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic TaxCalculationRetryTopic1() {
        return TopicBuilder.name("tax.calculation.kafka-retry-1")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic TaxCalculationRetryTopic2() {
        return TopicBuilder.name("tax.calculation.kafka-retry-2")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic TaxCalculationRetryTopic3() {
        return TopicBuilder.name("tax.calculation.kafka-retry-3")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic TaxCalculationDltTopic() {
        return TopicBuilder.name("tax.calculation.kafka-dlt")
                .partitions(1)
                .replicas(1)
                .build();
    }

}