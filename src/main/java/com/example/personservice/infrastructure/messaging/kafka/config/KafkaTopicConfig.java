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

    public static final String TAX_TOPIC_BATCH = "tax.kafka.batch";
    public static final String TAX_RETRY_TOPIC_1 = "tax.kafka.batch.retry-1";
    public static final String TAX_RETRY_TOPIC_2 = "tax.kafka.batch.retry-2";
    public static final String TAX_DLT_TOPIC = "tax.kafka.batch.dlt";

    @Bean
    public NewTopic taxBatchTopic() {
        return TopicBuilder.name(TAX_TOPIC_BATCH).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic taxRetryTopic1() {
        return TopicBuilder.name(TAX_RETRY_TOPIC_1).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic taxRetryTopic2() {
        return TopicBuilder.name(TAX_RETRY_TOPIC_2).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic taxDltTopic() {
        return TopicBuilder.name(TAX_DLT_TOPIC).partitions(1).replicas(1).build();
    }

}