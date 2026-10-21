package com.trivadis.kafkaws.springbootkafkainterceptor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consuming counterpart of {@link KafkaEventProducer}.
 * <p>
 * The producer writes {@code Person} events to {@code ${topic.name}}, where the
 * {@code ProducerInterceptorToStorage} stores the payload in object storage and
 * forwards only a {@code BusinessEvent} reference to the {@code ${topic.name}-refb}
 * topic. This consumer listens on that reference topic; the
 * {@code ConsumerInterceptorToStorage} (wired via {@code interceptor.classes}) resolves
 * each reference back into the original payload before it reaches this listener, so the
 * record value received here is the materialized event (e.g. a {@code Person}).
 */
@Component
public class KafkaEventConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventConsumer.class);

    @KafkaListener(topics = "${topic.name}-refb", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<Long, Object> record) {
        LOGGER.info("received record(key={} value={}) meta(partition={}, offset={}) timestamp={}",
                record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }
}
