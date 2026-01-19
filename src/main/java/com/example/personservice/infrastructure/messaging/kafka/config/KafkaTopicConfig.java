package com.example.personservice.infrastructure.messaging.kafka.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

    private KafkaRetryProperties retryProperties;

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

//    @Bean
//    public NewTopic PersonRetryTopic() {
//        return TopicBuilder.name(("person.kafka" + retryProperties.getRetryTopicSuffix()))
//                .partitions(retryProperties.getRetryTopicPartitions())
//                .replicas(retryProperties.getRetryTopicReplicas())
//                .build();
//    }
//
//    @Bean
//    public NewTopic PersonDltTopic() {
//        return TopicBuilder.name("person.kafka" + retryProperties.getDltTopicSuffix())
//                .partitions(retryProperties.getDltPartitions())
//                .replicas(retryProperties.getRetryTopicReplicas())
//                .build();
//    }

}