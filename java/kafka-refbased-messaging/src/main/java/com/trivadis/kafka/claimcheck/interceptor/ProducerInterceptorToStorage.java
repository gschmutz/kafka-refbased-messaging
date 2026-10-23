package com.trivadis.kafka.claimcheck.interceptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.trivadis.demo.dto.ClaimCheck;
import com.trivadis.kafka.claimcheck.interceptor.metrics.JmxMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetricsMBean;
import com.trivadis.kafka.claimcheck.storage.S3StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageReference;

/**
 * Kafka {@link ProducerInterceptor} that implements Reference-Based Messaging (the Claim Check
 * pattern) for large Avro payloads.
 * <p>
 * Kafka is optimized for relatively small messages. When a record value exceeds a configurable
 * size threshold, this interceptor avoids sending the full payload through the broker: it
 * <ol>
 *   <li>serializes the record value to Avro binary,</li>
 *   <li>hands those bytes to a {@link StorageBackend} (by default {@link S3StorageBackend}), which
 *       stores them in an object store and returns a {@link ClaimCheck}, and</li>
 *   <li>replaces the record value with that lightweight {@link ClaimCheck}, which points at the
 *       stored object via a {@code s3://bucket/key} URI.</li>
 * </ol>
 * Records whose serialized value is at or below the threshold are passed through unchanged, so they
 * stay inline on the topic. The reference is published on the <em>same</em> topic as the original
 * record; the {@code ClaimCheck} schema and the original payload schema coexist on that topic
 * provided the value serializer uses a record-aware subject naming strategy such as Confluent's
 * {@code TopicRecordNameStrategy}. The matching {@code ConsumerInterceptorToStorage} reverses the
 * process, fetching externalized payloads from storage and passing inline records through untouched.
 * <p>
 * All object-storage mechanics (S3 client, bucket/key naming, stored-object metadata, the reference
 * URI) live in the {@link StorageBackend}; this interceptor only decides <em>whether</em> to
 * externalize (the size threshold), serializes the Avro value, and stamps the
 * {@link ClaimCheck#getPayloadType() payloadType}.
 * <p>
 * <strong>Monitoring.</strong> Behaviour counters (records externalized / inline, bytes externalized,
 * errors) are exposed as a JMX MBean under
 * {@code com.trivadis.kafka.claimcheck:type=ProducerInterceptor,client-id=<client.id>}; see
 * {@link ProducerInterceptorMetricsMBean}.
 * <p>
 * <strong>Configuration.</strong> The size threshold below is read from the producer configuration
 * (the same map Kafka passes to {@link #configure(Map)}); all {@code interceptor.storage.*} keys are
 * read by the {@link StorageBackend} (see {@link S3StorageBackend}). Each key has a default so the
 * interceptor works out of the box:
 * <ul>
 *   <li>{@link #SIZE_THRESHOLD_CONFIG} &ndash; externalize only payloads larger than this many bytes
 *       (default {@value #DEFAULT_SIZE_THRESHOLD}, i.e. 1&nbsp;MiB)</li>
 * </ul>
 * <p>
 * <strong>Note:</strong> the {@link ClaimCheck#getPayloadType() payloadType} written on the
 * reference is currently hard-coded to {@code "Person"}; the consumer side uses it to resolve the
 * payload class, so this interceptor only round-trips {@code Person} payloads correctly as written.
 * <p>
 * This class is not thread-safe for configuration, but after {@link #configure(Map)} completes the
 * {@link #onSend(ProducerRecord)} method is safe for the concurrent use Kafka's producer makes of it.
 *
 * @param <K> record key type
 * @param <V> record value type; must be an Avro {@link org.apache.avro.specific.SpecificRecordBase}
 */
public class ProducerInterceptorToStorage<K,V extends org.apache.avro.specific.SpecificRecordBase>
                implements ProducerInterceptor<K, V> {

    /**
     * Producer config key for the payload size threshold (in bytes). Only records whose
     * serialized value is strictly larger than this are externalized to object storage;
     * smaller records are left inline on the topic unchanged.
     */
    public static final String SIZE_THRESHOLD_CONFIG = "interceptor.storage.size.threshold.bytes";

    private static final int DEFAULT_SIZE_THRESHOLD = 1048576; // 1 MiB

    private int sizeThreshold = DEFAULT_SIZE_THRESHOLD;
    private StorageBackend storage;
    private final ProducerInterceptorMetrics metrics = new ProducerInterceptorMetrics();
    private ObjectName metricsName;

    /**
     * Serializes an Avro {@link org.apache.avro.specific.SpecificRecordBase} to its Avro binary
     * encoding. This is the raw payload (no Schema Registry framing / magic byte) that gets stored
     * as the externalized object.
     *
     * @param value the Avro record to serialize
     * @param <V>   the Avro record type
     * @return the Avro-binary-encoded bytes
     * @throws IOException if encoding fails
     */
    public static <V extends org.apache.avro.specific.SpecificRecordBase> byte[] serialize(V value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<V> writer = new SpecificDatumWriter<V>(value.getSchema());

        writer.write(value, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /**
     * Intercepts a record on its way to the broker and applies the claim-check logic.
     * <p>
     * The record value is serialized to Avro binary. If the result is at or below the configured
     * {@link #SIZE_THRESHOLD_CONFIG size threshold}, the original record is returned unchanged and
     * stays inline on the topic. Otherwise the bytes are handed to the {@link StorageBackend}, and a
     * new record carrying the returned {@link ClaimCheck} &mdash; keyed and topic-ed identically to
     * the original &mdash; is returned in its place.
     *
     * @param record the record about to be sent
     * @return the original record (inline) or a reference-carrying replacement (externalized)
     * @throws RuntimeException if serialization or the object-store write fails
     */
    @Override
    public ProducerRecord onSend(ProducerRecord<K, V> record) {
        byte[] bytes;
        try {
            bytes = serialize(record.value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // claim-check only kicks in for large payloads; smaller records stay inline on the
        // topic unchanged (the consumer interceptor passes non-reference records through)
        if (bytes.length <= sizeThreshold) {
            metrics.recordInline();
            return record;
        }

        // hand the serialized payload to the storage backend; it stores the bytes and returns a
        // ClaimCheck referencing them. Topic/partition/timestamp are forwarded so the backend can
        // derive the storage location and record the Kafka context as object metadata.
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

        // wrap the storage reference in a ClaimCheck, stamping the payload type the consumer uses to
        // resolve the Avro class (hard-coded for now)
        ClaimCheck claimCheck = ClaimCheck.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("Person")
                .setPayloadReference(reference.getURI())
                .build();

        // publish the reference on the same topic and key as the original record; the ClaimCheck
        // and the original payload schema coexist because the serializer uses a record-aware
        // subject naming strategy (e.g. TopicRecordNameStrategy)
        return new ProducerRecord<>(record.topic(), record.key(), claimCheck);
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
     * Unregisters the metrics MBean and closes the storage backend.
     */
    @Override
    public void close() {
        JmxMetrics.unregister(metricsName);
        if (storage != null) {
            storage.close();
        }
    }

    /**
     * Reads the size threshold from the producer configuration and creates / configures the
     * {@link StorageBackend} (which reads the {@code interceptor.storage.*} keys from the same map).
     *
     * @param configs the producer configuration passed by the Kafka client
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        sizeThreshold = getIntOrDefault(configs, SIZE_THRESHOLD_CONFIG, DEFAULT_SIZE_THRESHOLD);
        storage = new S3StorageBackend();
        storage.configure(configs);
        metricsName = JmxMetrics.register(metrics, "ProducerInterceptor", getOrDefault(configs, "client.id", ""));
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
