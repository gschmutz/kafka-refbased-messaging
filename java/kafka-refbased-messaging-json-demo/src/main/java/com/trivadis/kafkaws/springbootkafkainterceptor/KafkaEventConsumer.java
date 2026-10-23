package com.trivadis.kafkaws.springbootkafkainterceptor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consuming counterpart of {@link KafkaEventProducer} for the JSON demo.
 * <p>
 * The producer writes {@code Person} events (serialized as JSON) to {@code ${topic.name}}, where the
 * {@code ProducerInterceptorToStorageHeaders} stores large payloads in object storage and publishes a
 * {@code null}-valued record carrying the reference in Kafka headers (small payloads stay inline).
 * This consumer listens on that topic; the {@code ConsumerInterceptorToStorageHeaders} (wired via
 * {@code interceptor.classes}) fetches externalized payloads from storage and reconstructs them with
 * the configured JSON deserializer before they reach this listener, so the record value received here
 * is always the materialized {@code Person}.
 */
@Component
public class KafkaEventConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventConsumer.class);

    @KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<Long, Object> record) {
        LOGGER.info("received record(key={} value={}) meta(partition={}, offset={}) timestamp={}",
                record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }
}
