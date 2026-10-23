package com.trivadis.kafka.claimcheck.interceptor;

import com.trivadis.kafka.claimcheck.interceptor.metrics.ConsumerInterceptorMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ConsumerInterceptorMetricsMBean;
import com.trivadis.kafka.claimcheck.interceptor.metrics.JmxMetrics;
import com.trivadis.kafka.claimcheck.storage.S3StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;

import javax.management.ObjectName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Header-based, <strong>serializer-agnostic</strong> variant of {@link ConsumerInterceptorToStorage}
 * that reverses the Reference-Based Messaging (Claim Check) applied by
 * {@link ProducerInterceptorToStorageHeaders}.
 * <p>
 * The header-based producer interceptor externalizes large payloads to object storage (as bytes
 * produced by the producer's {@code value.serializer}) and publishes a {@code null}-valued record
 * carrying the storage reference as Kafka headers, while leaving small payloads inline. On the way out
 * of the consumer, this interceptor inspects each record:
 * <ul>
 *   <li>if the {@link ProducerInterceptorToStorageHeaders#HEADER_REFERENCE} header is present, it
 *       fetches the referenced object through a {@link StorageBackend} (by default
 *       {@link S3StorageBackend}) and reconstructs the value by handing the stored bytes to the
 *       consumer's configured {@code value.deserializer} &mdash; so application code sees the
 *       materialized payload instead of a {@code null};</li>
 *   <li>otherwise the record was never externalized and is passed through unchanged.</li>
 * </ul>
 * Because reconstruction goes through the configured {@code value.deserializer}, this works for any
 * payload format (Avro, JSON, String, byte[], &hellip;) &mdash; the interceptor contains no
 * type-specific code.
 * <p>
 * <strong>Configuration.</strong> The {@code interceptor.storage.*} keys are read by the
 * {@link StorageBackend} (see {@link S3StorageBackend}); the standard {@code value.deserializer} the
 * consumer is already configured with is used to reconstruct the value.
 * <p>
 * <strong>Monitoring.</strong> Behaviour counters (records materialized / passed through, bytes
 * fetched, errors) are exposed as a JMX MBean under
 * {@code com.trivadis.kafka.claimcheck:type=ConsumerInterceptor,client-id=<client.id>}; see
 * {@link ConsumerInterceptorMetricsMBean}.
 *
 * @param <K> record key type
 * @param <V> record value type (opaque; reconstructed via the configured {@code value.deserializer})
 */
public class ConsumerInterceptorToStorageHeaders<K, V> implements ConsumerInterceptor<K, V> {

    private final ConsumerInterceptorMetrics metrics = new ConsumerInterceptorMetrics();
    private ObjectName metricsName;
    private StorageBackend storage;
    private Deserializer<Object> valueDeserializer;

    /**
     * Materializes any externalized payloads in the polled batch before the records reach the
     * application. Records carrying the reference header are rehydrated through the
     * {@link StorageBackend} (the stored bytes are turned back into a value by the configured
     * {@code value.deserializer}); all others are passed through unchanged. Partition/offset/key/
     * headers/timestamp metadata is preserved; only the value of an externalized record is replaced
     * (the original {@code null} value becomes the materialized payload).
     *
     * @param records the records returned by the poll
     * @return a new batch with references resolved to their original payloads
     * @throws RuntimeException if a referenced object cannot be fetched or deserialized
     */
    @Override
    @SuppressWarnings("unchecked")
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> newRecords = new HashMap<>();
        records.forEach(r -> {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());

            // only records carrying the reference header were externalized; others were left inline
            // by the producer interceptor and are passed through unchanged
            Header referenceHeader = r.headers().lastHeader(ProducerInterceptorToStorageHeaders.HEADER_REFERENCE);
            if (referenceHeader == null) {
                metrics.recordPassThrough();
                newRecords.computeIfAbsent(tp, k -> new ArrayList<>()).add(r);
                return;
            }

            String uri = new String(referenceHeader.value(), StandardCharsets.UTF_8);
            try {
                // fetch the externalized bytes through the storage backend
                byte[] byteArray = storage.checkOut(new StorageReference(uri));
                // reconstruct the opaque value via the consumer's configured value deserializer
                V value = (V) valueDeserializer.deserialize(r.topic(), r.headers(), byteArray);

                // build a new record with the resolved value in place of the null value; the
                // reference headers are kept for traceability
                ConsumerRecord<K, V> newRecord = new ConsumerRecord<>(
                        r.topic(),
                        r.partition(),
                        r.offset(),
                        r.timestamp(),
                        r.timestampType(),
                        r.serializedKeySize(),
                        r.serializedValueSize(),
                        r.key(),
                        value,
                        r.headers(),
                        r.leaderEpoch());

                metrics.recordMaterialized(byteArray.length);
                newRecords.computeIfAbsent(tp, k -> new ArrayList<>()).add(newRecord);
            } catch (RuntimeException e) {
                metrics.recordError();
                throw e;
            }
        });

        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        // close connection to S3
    }

    /**
     * Unregisters the metrics MBean and closes the storage backend and the value deserializer.
     */
    @Override
    public void close() {
        JmxMetrics.unregister(metricsName);
        if (storage != null) {
            storage.close();
        }
        if (valueDeserializer != null) {
            valueDeserializer.close();
        }
    }

    /**
     * Creates / configures the {@link StorageBackend} (which reads the {@code interceptor.storage.*}
     * keys from the consumer configuration), instantiates a private copy of the configured
     * {@code value.deserializer}, and registers the metrics MBean.
     *
     * @param map the consumer configuration passed by the Kafka client
     */
    @Override
    public void configure(Map<String, ?> map) {
        valueDeserializer = instantiateValueDeserializer(map);
        storage = new S3StorageBackend();
        storage.configure(map);
        metricsName = JmxMetrics.register(metrics, "ConsumerInterceptor", getOrDefault(map, "client.id", ""));
    }

    /**
     * Instantiates and configures a copy of the consumer's {@code value.deserializer} so the
     * interceptor can reconstruct opaque values from the stored bytes. The value is accepted either
     * as a {@link Class} or as a class-name {@link String} (both forms Kafka may put in the config).
     */
    @SuppressWarnings("unchecked")
    private static Deserializer<Object> instantiateValueDeserializer(final Map<String, ?> configs) {
        Object configured = configs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        if (configured == null) {
            throw new IllegalStateException(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
                    + " must be configured for " + ConsumerInterceptorToStorageHeaders.class.getName());
        }
        try {
            Class<?> clazz = configured instanceof Class ? (Class<?>) configured : Class.forName(configured.toString());
            Deserializer<Object> deserializer = (Deserializer<Object>) clazz.getDeclaredConstructor().newInstance();
            deserializer.configure(configs, false);
            return deserializer;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Could not instantiate value deserializer " + configured, e);
        }
    }

    private static String getOrDefault(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }
}
