package com.example.personservice.infrastructure.messaging.kafka;

import com.example.personservice.application.dto.ManualBatchResponseDto;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Service
@RequiredArgsConstructor
public class TaxManualService {

    private final ConsumerFactory<String, String> consumerFactory;

    private static final String TAX_BATCH_TOPIC = "tax.kafka.manual.batch";

    public ManualBatchResponseDto fetchBatch(int maxEvents) {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxEvents);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tax.manual.group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        try (Consumer<String, String> consumer = consumerFactory.createConsumer("tax.manual.group", null, null, props)) {

            // Subscribe to the topic
            consumer.subscribe(List.of(TAX_BATCH_TOPIC));

            // Poll for data
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            List<String> events = new ArrayList<>();
            records.forEach(rec -> events.add(rec.value()));

            // Commit offsets manually
            if (!records.isEmpty()) {
                consumer.commitSync();
            }

            // Calculate remaining messages
            long totalMsgLeft = calculateTotalMsgLeft(consumer);

            return new ManualBatchResponseDto(
                    events,
                    totalMsgLeft,
                    totalMsgLeft > 0
            );
        }
    }

    private long calculateTotalMsgLeft(Consumer<?, ?> consumer) {
        var assignment = consumer.assignment();
        if (assignment == null || assignment.isEmpty()) {
            return 0;
        }

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);

        long totalMsgLeft = 0;
        for (TopicPartition topicPartition : assignment) {
            long currentOffset = consumer.position(topicPartition);
            long endOffset = endOffsets.get(topicPartition);
            totalMsgLeft += (endOffset - currentOffset);
        }

        return totalMsgLeft;
    }
}
