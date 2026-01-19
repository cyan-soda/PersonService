package com.example.personservice.infrastructure.messaging.kafka.config;

import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public CommonErrorHandler commonErrorHandler() {
        // retry 0 times for fatal errors (deserialization, etc.)
        return new DefaultErrorHandler(new FixedBackOff(0L, 0L));
    }

    // Config for Person Event consumers
    @Bean("personConsumerFactory")
    public ConsumerFactory<String, PersonEvent> personEventConsumerFactory() {
        return createConsumerFactory(PersonEvent.class, "person.crud.group");
    }

    @Bean("personKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, PersonEvent> personKafkaListenerContainerFactory(CommonErrorHandler commonErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, PersonEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(personEventConsumerFactory());
        factory.setCommonErrorHandler(commonErrorHandler);
        return factory;
    }

    // Config for Tax Calculation Event consumers
    @Bean("taxCalculationConsumerFactory")
    public ConsumerFactory<String, TaxCalculationEvent> taxCalculationEventConsumerFactory() {
        return createConsumerFactory(TaxCalculationEvent.class, "tax.calculation.group");
    }

    @Bean("taxKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, TaxCalculationEvent> taxKafkaListenerContainerFactory(CommonErrorHandler commonErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, TaxCalculationEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taxCalculationEventConsumerFactory());
        factory.setCommonErrorHandler(commonErrorHandler);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(5000);
        return factory;
    }

    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> eventType, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, eventType.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.personservice.infrastructure.messaging.events");

        // for batch processing
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }
}