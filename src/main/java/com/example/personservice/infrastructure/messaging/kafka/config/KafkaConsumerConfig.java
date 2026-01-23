package com.example.personservice.infrastructure.messaging.kafka.config;

import com.example.personservice.infrastructure.exception.KafkaConsumerException;
import com.example.personservice.infrastructure.messaging.events.PersonEvent;
import com.example.personservice.infrastructure.messaging.events.TaxCalculationEvent;
import com.example.personservice.infrastructure.messaging.kafka.retry.SingleErrorHandler;
import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.client.ResourceAccessException;

import java.net.SocketException;
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

    @Bean("taxErrorHandler")
    public DefaultErrorHandler taxErrorHandler(KafkaTemplate<String, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxInterval(10000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addRetryableExceptions(
                SocketException.class,
                ResourceAccessException.class,
                TransientDataAccessException.class,
                RecoverableDataAccessException.class,
                KafkaConsumerException.class
        );
        handler.setSeekAfterError(true);
        return handler;
    }

    @Bean("personSingleContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, PersonEvent> personSingleContainerFactory(
            ConsumerFactory<String, PersonEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, PersonEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(false);

        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean("personBatchContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, PersonEvent> personBatchContainerFactory(
            ConsumerFactory<String, PersonEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, PersonEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 3L)));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    // Config for Person Event consumers
    @Bean("personConsumerFactory")
    public ConsumerFactory<String, PersonEvent> personEventConsumerFactory() {
        return createConsumerFactory(PersonEvent.class, "person.crud.group");
    }

    @Bean("personKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, PersonEvent> personKafkaListenerContainerFactory(SingleErrorHandler singleErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, PersonEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(personEventConsumerFactory());
        factory.setCommonErrorHandler(singleErrorHandler);
        factory.setConcurrency(3);
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    // Config for Tax Calculation Event consumers
    @Bean("taxCalculationConsumerFactory")
    public ConsumerFactory<String, TaxCalculationEvent> taxCalculationEventConsumerFactory() {
        return createConsumerFactory(TaxCalculationEvent.class, "tax.calculation.group");
    }

    @Bean("taxKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, TaxCalculationEvent> taxKafkaListenerContainerFactory(
            DefaultErrorHandler taxErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, TaxCalculationEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taxCalculationEventConsumerFactory());
        factory.setCommonErrorHandler(taxErrorHandler);
        factory.setBatchListener(true);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> eventType, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, eventType.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.personservice.infrastructure.messaging.events");

        // for batch processing
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10000);
//        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10240);

        return new DefaultKafkaConsumerFactory<>(props);
    }
}