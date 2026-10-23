package com.trivadis.kafka.claimcheck.interceptor;

import com.trivadis.demo.dto.ClaimCheck;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ConsumerInterceptorMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ConsumerInterceptorMetricsMBean;
import com.trivadis.kafka.claimcheck.interceptor.metrics.JmxMetrics;
import com.trivadis.kafka.claimcheck.storage.S3StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageBackend;
import com.trivadis.kafka.claimcheck.storage.StorageReference;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

/**
 * Kafka {@link ConsumerInterceptor} that reverses the Reference-Based Messaging (Claim Check)
 * applied by {@code ProducerInterceptorToStorage}.
 * <p>
 * The producer interceptor externalizes large Avro payloads to an object store and puts a lightweight
 * {@link ClaimCheck} on the topic in their place, while leaving small payloads inline. On the way out
 * of the consumer, this interceptor inspects each record:
 * <ul>
 *   <li>if the value is an {@link ClaimCheck}, it fetches the referenced object through a
 *       {@link StorageBackend} (by default {@link S3StorageBackend}), deserializes it back into the
 *       original Avro record, and substitutes that as the record value &mdash; so application code
 *       never sees the reference;</li>
 *   <li>otherwise the record was never externalized and is passed through unchanged.</li>
 * </ul>
 * Because it runs transparently via {@code interceptor.classes}, consumers receive the materialized
 * payload (e.g. a {@code Person}) regardless of whether it travelled inline or through storage.
 * <p>
 * All object-storage mechanics (S3 client, fetching by the {@code s3://bucket/key} reference URI)
 * live in the {@link StorageBackend}; this interceptor only resolves the Avro payload type and
 * deserializes the fetched bytes.
 * <p>
 * <strong>Payload type resolution.</strong> The Avro bytes stored in the object store carry no Schema
 * Registry framing, so the writer type is recovered from {@link ClaimCheck#getPayloadType()
 * payloadType}: a simple name (e.g. {@code "Person"}) is resolved against the package of
 * {@link ClaimCheck}, while a fully-qualified name is used as-is. The payload classes must therefore
 * be on the consumer's classpath.
 * <p>
 * <strong>Configuration.</strong> The object-store connection is read by the {@link StorageBackend}
 * from the consumer configuration (the map passed to {@link #configure(Map)}); see
 * {@link S3StorageBackend} for the recognized {@code interceptor.storage.*} keys. Note there is no
 * size threshold here &mdash; whether to externalize is a producer-only decision; the consumer reacts
 * to the record value type.
 * <p>
 * <strong>Monitoring.</strong> Behaviour counters (records materialized / passed through, bytes
 * fetched, errors) are exposed as a JMX MBean under
 * {@code com.trivadis.kafka.claimcheck:type=ConsumerInterceptor,client-id=<client.id>}; see
 * {@link ConsumerInterceptorMetricsMBean}.
 *
 * @param <K> record key type
 * @param <V> record value type (the materialized payload, or {@link ClaimCheck} on the wire)
 */
public class ConsumerInterceptorToStorage<K, V> implements ConsumerInterceptor<K, V> {

    private StorageBackend storage;
    private final ConsumerInterceptorMetrics metrics = new ConsumerInterceptorMetrics();
    private ObjectName metricsName;

    // DTOs are generated into the same package as ClaimCheck (e.g. com.trivadis.demo.dto)
    private static final String DTO_PACKAGE = ClaimCheck.class.getPackage().getName();

    /**
     * Resolves the Avro payload class from the {@code payloadType} carried on the reference. A value
     * containing a dot is treated as a fully-qualified class name; otherwise it is resolved as a
     * simple name within the package of {@link ClaimCheck}.
     *
     * @param payloadType simple or fully-qualified payload type name
     * @return the resolved Avro specific-record class
     * @throws RuntimeException if no matching class is on the classpath
     */
    @SuppressWarnings("unchecked")
    private static Class<? extends SpecificRecordBase> resolvePayloadClass(String payloadType) {
        try {
            // payloadType may be a simple name ("Person") or already fully qualified
            String className = payloadType.contains(".") ? payloadType : DTO_PACKAGE + "." + payloadType;
            return (Class<? extends SpecificRecordBase>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unknown payload type: " + payloadType, e);
        }
    }

    /**
     * Deserializes Avro binary bytes (as written by the producer interceptor) back into a specific
     * Avro record of the given type.
     *
     * @param value the Avro-binary-encoded bytes fetched from storage
     * @param clazz the target Avro specific-record class
     * @param <T>   the Avro record type
     * @return the deserialized record
     * @throws IOException if decoding fails
     */
    public static <T extends SpecificRecordBase> T deserialize(byte[] value, Class clazz) throws IOException {
        DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        return datumReader.read(null, decoder);
    }

    /**
     * Materializes any externalized payloads in the polled batch before the records reach the
     * application. Reference-carrying records are rehydrated through the {@link StorageBackend}; all
     * others are passed through unchanged. Partition/offset/key/headers/timestamp metadata is
     * preserved; only the value of a reference record is replaced.
     *
     * @param records the records returned by the poll
     * @return a new batch with references resolved to their original payloads
     * @throws RuntimeException if a referenced object cannot be fetched or deserialized
     */
    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> newRecords = new HashMap<>();
        records.forEach(r -> {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());

            // only records carrying an ClaimCheck were externalized; smaller payloads were
            // left inline by the producer interceptor and are passed through unchanged
            if (!(r.value() instanceof ClaimCheck)) {
                metrics.recordPassThrough();
                newRecords.computeIfAbsent(tp, k -> new ArrayList<>()).add(r);
                return;
            }

            // fetch the externalized bytes through the storage backend (the reference is a
            // s3://bucket/key URI the backend knows how to resolve)
            ClaimCheck be = (ClaimCheck) r.value();
            try {
                byte[] byteArray = storage.checkOut(new StorageReference(be.getPayloadReference().toString()));
                // resolve the payload type from the payloadType field of the ClaimCheck
                Class<? extends SpecificRecordBase> payloadClass = resolvePayloadClass(be.getPayloadType().toString());
                SpecificRecordBase srb = deserialize(byteArray, payloadClass);
                metrics.recordMaterialized(byteArray.length);

                // build a new record with the resolved value in place of the reference
                ConsumerRecord<K, V> newRecord = new ConsumerRecord<>(
                        r.topic(),
                        r.partition(),
                        r.offset(),
                        r.timestamp(),
                        r.timestampType(),
                        r.serializedKeySize(),
                        r.serializedValueSize(),
                        r.key(),
                        (V) srb,
                        r.headers(),
                        r.leaderEpoch());

                newRecords.computeIfAbsent(tp, k -> new ArrayList<>()).add(newRecord);
            } catch (IOException e) {
                metrics.recordError();
                throw new RuntimeException(e);
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
     * Creates / configures the {@link StorageBackend} (which reads the {@code interceptor.storage.*}
     * keys from the consumer configuration) and registers the metrics MBean.
     *
     * @param map the consumer configuration passed by the Kafka client
     */
    @Override
    public void configure(Map<String, ?> map) {
        storage = new S3StorageBackend();
        storage.configure(map);
        metricsName = JmxMetrics.register(metrics, "ConsumerInterceptor", getOrDefault(map, "client.id", ""));
    }

    private static String getOrDefault(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }
}
