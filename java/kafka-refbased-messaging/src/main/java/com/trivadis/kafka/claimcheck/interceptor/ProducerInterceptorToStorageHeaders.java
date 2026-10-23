package com.trivadis.kafka.claimcheck.interceptor;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;

import javax.management.ObjectName;

import com.trivadis.kafka.claimcheck.interceptor.metrics.JmxMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetricsMBean;
import com.trivadis.kafka.claimcheck.storage.S3StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageReference;

/**
 * Header-based, <strong>serializer-agnostic</strong> variant of {@link ProducerInterceptorToStorage}
 * implementing Reference-Based Messaging (the Claim Check pattern) for large payloads of
 * <em>any</em> type.
 * <p>
 * Unlike {@link ProducerInterceptorToStorage} (which carries the reference as an Avro
 * {@code ClaimCheck} value), this interceptor treats the record value as opaque: to externalize a
 * payload it serializes the value with the producer's configured {@code value.serializer}, hands the
 * bytes to a {@link StorageBackend} (by default {@link S3StorageBackend}), and then publishes the
 * record with:
 * <ul>
 *   <li>a <strong>{@code null} value</strong> &mdash; which every Kafka {@code Serializer} accepts
 *       regardless of type; and</li>
 *   <li>the storage reference in <strong>Kafka record headers</strong> ({@link #HEADER_REFERENCE},
 *       plus the informational {@link #HEADER_PAYLOAD_TYPE} and {@link #HEADER_TS}).</li>
 * </ul>
 * Because the bytes are produced by the configured serializer (Avro, JSON, String, byte[], &hellip;),
 * the matching {@link ConsumerInterceptorToStorageHeaders} reconstructs the value using the consumer's
 * configured {@code value.deserializer} &mdash; no type-specific code in the interceptor itself.
 * <p>
 * Records whose serialized value is {@code null} or at or below the configured
 * {@link #SIZE_THRESHOLD_CONFIG size threshold} are passed through unchanged (inline, with no
 * reference headers).
 * <p>
 * <strong>Caveat.</strong> A {@code null} value is the log-compaction tombstone marker, so this
 * variant must not be used on a compacted topic (every externalized record would mark its key for
 * deletion). On normal delete/time-retention topics it is safe.
 * <p>
 * <strong>Configuration.</strong> The size threshold below is read from the producer configuration;
 * all {@code interceptor.storage.*} keys are read by the {@link StorageBackend} (see
 * {@link S3StorageBackend}); and the standard {@code value.serializer} the producer is already
 * configured with is used to serialize the value.
 * <p>
 * <strong>Monitoring.</strong> Behaviour counters (records externalized / inline, bytes externalized,
 * errors) are exposed as a JMX MBean under
 * {@code com.trivadis.kafka.claimcheck:type=ProducerInterceptor,client-id=<client.id>}; see
 * {@link ProducerInterceptorMetricsMBean}.
 *
 * @param <K> record key type
 * @param <V> record value type (opaque; serialized via the configured {@code value.serializer})
 */
public class ProducerInterceptorToStorageHeaders<K, V>
                implements ProducerInterceptor<K, V> {

    /** Common prefix for all reference headers written by this interceptor. */
    public static final String HEADER_PREFIX = "refbased.";
    /** Header carrying the storage reference URI (e.g. {@code s3://bucket/key}). Its presence marks a record as externalized. */
    public static final String HEADER_REFERENCE = HEADER_PREFIX + "reference";
    /** Header carrying the payload's Java class name at produce time (informational / traceability only). */
    public static final String HEADER_PAYLOAD_TYPE = HEADER_PREFIX + "payloadType";
    /** Header carrying the reference creation timestamp, in epoch milliseconds (UTF-8 decimal string). */
    public static final String HEADER_TS = HEADER_PREFIX + "ts";

    /**
     * Producer config key for the payload size threshold (in bytes). Only records whose
     * serialized value is strictly larger than this are externalized to object storage;
     * smaller records are left inline on the topic unchanged.
     */
    public static final String SIZE_THRESHOLD_CONFIG = "interceptor.storage.size.threshold.bytes";

    private static final int DEFAULT_SIZE_THRESHOLD = 1048576; // 1 MiB

    private final ProducerInterceptorMetrics metrics = new ProducerInterceptorMetrics();
    private ObjectName metricsName;
    private StorageBackend storage;
    private Serializer<Object> valueSerializer;
    private int sizeThreshold = DEFAULT_SIZE_THRESHOLD;

    /**
     * Intercepts a record on its way to the broker and applies the claim-check logic.
     * <p>
     * The record value is serialized to bytes using the configured {@code value.serializer}. If the
     * result is {@code null} or at or below the configured {@link #SIZE_THRESHOLD_CONFIG size
     * threshold}, the original record is returned unchanged and stays inline. Otherwise the bytes are
     * stored through the {@link StorageBackend}, and a new record is returned that keeps the original
     * topic and key but carries a {@code null} value and the storage reference as headers.
     *
     * @param record the record about to be sent
     * @return the original record (inline) or a header-carrying, null-valued replacement (externalized)
     * @throws RuntimeException if the object-store write fails
     */
    @Override
    public ProducerRecord onSend(ProducerRecord<K, V> record) {
        // serialize the opaque value via the producer's configured value serializer; a null value
        // (or a serializer that yields null) can never be externalized, so pass it through
        byte[] bytes = valueSerializer.serialize(record.topic(), record.value());
        if (bytes == null || bytes.length <= sizeThreshold) {
            metrics.recordInline();
            return record;
        }

        // record the payload's Java class for traceability (the consumer does not need it: it
        // reconstructs the value with the configured value.deserializer)
        String payloadType = record.value() != null ? record.value().getClass().getName() : "";

        // hand the serialized payload to the storage backend; it returns a reference URI
        ProducerRecord<byte[], byte[]> toStore =
                new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), null, bytes);
        StorageReference reference;
        try {
            reference = storage.checkIn(toStore);
        } catch (RuntimeException e) {
            metrics.recordError();
            throw e;
        }
        metrics.recordExternalized(bytes.length);

        // carry the reference in headers (preserving any headers the application already set) and
        // send a null value, so the configured value serializer is irrelevant for this record
        RecordHeaders headers = new RecordHeaders();
        record.headers().forEach(headers::add);
        headers.add(new RecordHeader(HEADER_REFERENCE, reference.getURI().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(HEADER_PAYLOAD_TYPE, payloadType.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(HEADER_TS, Long.toString(Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8)));

        return new ProducerRecord<K, V>(record.topic(), null, record.key(), null, headers);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
        }
    }

    /**
     * Unregisters the metrics MBean and closes the storage backend and the value serializer.
     */
    @Override
    public void close() {
        JmxMetrics.unregister(metricsName);
        if (storage != null) {
            storage.close();
        }
        if (valueSerializer != null) {
            valueSerializer.close();
        }
    }

    /**
     * Reads the size threshold from the producer configuration, creates / configures the
     * {@link StorageBackend} (which reads the {@code interceptor.storage.*} keys from the same map),
     * and instantiates a private copy of the configured {@code value.serializer} used to turn record
     * values into the bytes that get stored.
     *
     * @param configs the producer configuration passed by the Kafka client
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        sizeThreshold = getIntOrDefault(configs, SIZE_THRESHOLD_CONFIG, DEFAULT_SIZE_THRESHOLD);
        valueSerializer = instantiateValueSerializer(configs);
        storage = new S3StorageBackend();
        storage.configure(configs);
        metricsName = JmxMetrics.register(metrics, "ProducerInterceptor", getOrDefault(configs, "client.id", ""));
    }

    /**
     * Instantiates and configures a copy of the producer's {@code value.serializer} so the
     * interceptor can serialize opaque values to the bytes it stores. The value is accepted either
     * as a {@link Class} or as a class-name {@link String} (both forms Kafka may put in the config).
     */
    @SuppressWarnings("unchecked")
    private static Serializer<Object> instantiateValueSerializer(final Map<String, ?> configs) {
        Object configured = configs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        if (configured == null) {
            throw new IllegalStateException(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
                    + " must be configured for " + ProducerInterceptorToStorageHeaders.class.getName());
        }
        try {
            Class<?> clazz = configured instanceof Class ? (Class<?>) configured : Class.forName(configured.toString());
            Serializer<Object> serializer = (Serializer<Object>) clazz.getDeclaredConstructor().newInstance();
            serializer.configure(configs, false);
            return serializer;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Could not instantiate value serializer " + configured, e);
        }
    }

    private static String getOrDefault(final Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private static int getIntOrDefault(final Map<String, ?> configs, String key, int defaultValue) {
        Object value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString().trim());
    }

}
