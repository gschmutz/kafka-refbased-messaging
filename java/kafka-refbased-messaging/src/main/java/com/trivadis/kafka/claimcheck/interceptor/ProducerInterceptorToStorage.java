package com.trivadis.kafka.claimcheck.interceptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.management.ObjectName;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.trivadis.demo.dto.ObjectReference;
import com.trivadis.kafka.claimcheck.interceptor.metrics.JmxMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetrics;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetricsMBean;

/**
 * Kafka {@link ProducerInterceptor} that implements Reference-Based Messaging (the Claim Check
 * pattern) for large Avro payloads.
 * <p>
 * Kafka is optimized for relatively small messages. When a record value exceeds a configurable
 * size threshold, this interceptor avoids sending the full payload through the broker: it
 * <ol>
 *   <li>serializes the record value to Avro binary,</li>
 *   <li>stores those bytes as an object in an S3-compatible object store (e.g. AWS S3 or MinIO), and</li>
 *   <li>replaces the record value with a lightweight {@link ObjectReference} that points at the stored
 *       object (bucket + object name).</li>
 * </ol>
 * Records whose serialized value is at or below the threshold are passed through unchanged, so they
 * stay inline on the topic. The reference is published on the <em>same</em> topic as the original
 * record; the {@code ObjectReference} schema and the original payload schema coexist on that topic
 * provided the value serializer uses a record-aware subject naming strategy such as Confluent's
 * {@code TopicRecordNameStrategy}. The matching {@code ConsumerInterceptorToStorage} reverses the
 * process, fetching externalized payloads from storage and passing inline records through untouched.
 * <p>
 * Each stored object also carries the Kafka context available at produce time as S3 user metadata
 * ({@code x-amz-meta-*}): {@code kafka-topic}, {@code kafka-key}, {@code payload-class} and
 * {@code payload-size-bytes}, plus {@code kafka-partition} / {@code kafka-timestamp} when the
 * application set them. The <em>offset</em> is deliberately absent: in a producer interceptor the
 * record has not been appended yet, so no offset exists.
 * <p>
 * <strong>Monitoring.</strong> Behaviour counters (records externalized / inline, bytes externalized,
 * errors) are exposed as a JMX MBean under
 * {@code com.trivadis.kafka.claimcheck:type=ProducerInterceptor,client-id=<client.id>}; see
 * {@link ProducerInterceptorMetricsMBean}.
 * <p>
 * <strong>Configuration.</strong> All settings are read from the producer configuration (the same
 * map Kafka passes to {@link #configure(Map)}); each has a default so the interceptor works out of
 * the box:
 * <ul>
 *   <li>{@link #SIZE_THRESHOLD_CONFIG} &ndash; externalize only payloads larger than this many bytes
 *       (default {@value #DEFAULT_SIZE_THRESHOLD}, i.e. 1&nbsp;MiB)</li>
 *   <li>{@link #BUCKET_NAME_CONFIG} &ndash; target bucket for stored objects (may contain the {@value #TOPIC_PLACEHOLDER} placeholder)</li>
 *   <li>{@link #OBJECT_PREFIX_CONFIG} &ndash; optional key prefix ("folder") under which objects are stored (may contain {@value #TOPIC_PLACEHOLDER})</li>
 *   <li>{@link #TOPIC_TRANSFORM_REGEX_CONFIG} / {@link #TOPIC_TRANSFORM_REPLACEMENT_CONFIG} &ndash; optional regex applied to the topic before it fills {@value #TOPIC_PLACEHOLDER}</li>
 *   <li>{@link #PARTITION_GRANULARITY_CONFIG} &ndash; optional time-based partitioning ({@code none}/{@code monthly}/{@code daily}/{@code hourly})</li>
 *   <li>{@link #SERVICE_ENDPOINT_CONFIG} &ndash; object-store endpoint URL</li>
 *   <li>{@link #REGION_CONFIG} &ndash; signing region</li>
 *   <li>{@link #ACCESS_KEY_CONFIG} / {@link #SECRET_KEY_CONFIG} &ndash; object-store credentials</li>
 * </ul>
 * Example (Spring Boot {@code application.yml} producer properties):
 * <pre>{@code
 * interceptor.classes: com.trivadis.kafka.claimcheck.interceptor.ProducerInterceptorToStorage
 * interceptor.storage.bucket.name: large-object-bucket
 * interceptor.storage.service.endpoint: http://minio:9000
 * interceptor.storage.region: us-east-2
 * interceptor.storage.access.key: <key>
 * interceptor.storage.secret.key: <secret>
 * interceptor.storage.size.threshold.bytes: 1048576
 * }</pre>
 * <p>
 * <strong>Note:</strong> the {@link ObjectReference#getPayloadType() payloadType} written on the
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

    /** Producer config key used to configure the target bucket for the stored objects. */
    public static final String BUCKET_NAME_CONFIG = "interceptor.storage.bucket.name";
    /** Producer config key used to configure the S3 access key. */
    public static final String ACCESS_KEY_CONFIG = "interceptor.storage.access.key";
    /** Producer config key used to configure the S3 secret key. */
    public static final String SECRET_KEY_CONFIG = "interceptor.storage.secret.key";
    /** Producer config key used to configure the S3 service endpoint. */
    public static final String SERVICE_ENDPOINT_CONFIG = "interceptor.storage.service.endpoint";
    /** Producer config key used to configure the S3 signing region. */
    public static final String REGION_CONFIG = "interceptor.storage.region";
    /**
     * Producer config key for the payload size threshold (in bytes). Only records whose
     * serialized value is strictly larger than this are externalized to object storage;
     * smaller records are left inline on the topic unchanged.
     */
    public static final String SIZE_THRESHOLD_CONFIG = "interceptor.storage.size.threshold.bytes";
    /**
     * Producer config key for the object key prefix ("folder") under which stored objects are
     * placed in the bucket. Empty by default (objects are stored at the bucket root). Leading and
     * trailing slashes are ignored, so {@code foo}, {@code foo/} and {@code /foo/} are equivalent.
     */
    public static final String OBJECT_PREFIX_CONFIG = "interceptor.storage.object.prefix";
    /**
     * Producer config key for time-based partitioning of stored objects. Inserts a UTC date path
     * between the {@link #OBJECT_PREFIX_CONFIG prefix} and the object name, which makes it easy to
     * apply age-based retention / lifecycle rules on the bucket. Accepted values (case-insensitive):
     * <ul>
     *   <li>{@code none} (default) &ndash; no date path</li>
     *   <li>{@code monthly} &ndash; {@code yyyy/MM}</li>
     *   <li>{@code daily} &ndash; {@code yyyy/MM/dd}</li>
     *   <li>{@code hourly} &ndash; {@code yyyy/MM/dd/HH}</li>
     * </ul>
     */
    public static final String PARTITION_GRANULARITY_CONFIG = "interceptor.storage.partition.granularity";
    /**
     * Producer config key for an optional regex applied to the record's topic name before it is
     * substituted into {@link #BUCKET_NAME_CONFIG} / {@link #OBJECT_PREFIX_CONFIG} via the
     * {@value #TOPIC_PLACEHOLDER} placeholder. Useful to turn a topic name into a valid bucket name
     * (e.g. replace {@code .} / {@code _} with {@code -}). Empty by default (no transformation).
     * Used together with {@link #TOPIC_TRANSFORM_REPLACEMENT_CONFIG}.
     */
    public static final String TOPIC_TRANSFORM_REGEX_CONFIG = "interceptor.storage.topic.transform.regex";
    /**
     * Producer config key for the replacement string used with {@link #TOPIC_TRANSFORM_REGEX_CONFIG}
     * (standard {@link java.util.regex.Matcher#replaceAll(String)} semantics; {@code $1} etc. refer
     * to capture groups). Empty by default.
     */
    public static final String TOPIC_TRANSFORM_REPLACEMENT_CONFIG = "interceptor.storage.topic.transform.replacement";

    /** Placeholder, usable in {@link #BUCKET_NAME_CONFIG} and {@link #OBJECT_PREFIX_CONFIG}, that is replaced with the (transformed) topic name. */
    public static final String TOPIC_PLACEHOLDER = "${topic}";

    private static final String DEFAULT_BUCKET_NAME = "large-object-bucket";
    private static final String DEFAULT_ACCESS_KEY = "admin";
    private static final String DEFAULT_SECRET_KEY = "abc123abc123!";
    private static final String DEFAULT_SERVICE_ENDPOINT = "http://dataplatform:9005";
    private static final String DEFAULT_REGION = Regions.US_EAST_2.getName();
    private static final int DEFAULT_SIZE_THRESHOLD = 1048576; // 1 MiB
    private static final String DEFAULT_OBJECT_PREFIX = "";

    private static final String PARTITION_NONE = "none";
    private static final String PARTITION_MONTHLY = "monthly";
    private static final String PARTITION_DAILY = "daily";
    private static final String PARTITION_HOURLY = "hourly";

    private static final DateTimeFormatter MONTHLY_FMT = DateTimeFormatter.ofPattern("yyyy/MM").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DAILY_FMT = DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOURLY_FMT = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH").withZone(ZoneOffset.UTC);

    private AmazonS3 s3client;
    private String bucketName = DEFAULT_BUCKET_NAME;
    private int sizeThreshold = DEFAULT_SIZE_THRESHOLD;
    private String objectPrefix = DEFAULT_OBJECT_PREFIX;
    private String partitionGranularity = PARTITION_NONE;
    private Pattern topicTransformPattern;
    private String topicTransformReplacement = "";
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
     * stays inline on the topic. Otherwise the bytes are stored in the configured bucket (created on
     * demand) under a random object name, and a new record carrying an {@link ObjectReference} to
     * that object &mdash; keyed and topic-ed identically to the original &mdash; is returned in its place.
     *
     * @param record the record about to be sent
     * @return the original record (inline) or a reference-carrying replacement (externalized)
     * @throws RuntimeException if serialization or the object-store write fails
     */
    @Override
    public ProducerRecord onSend(ProducerRecord<K, V> record) {
        byte[] bytes = new byte[0];
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

        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("application/avro-binary");
        // attach the Kafka context available at produce time as S3 user metadata (x-amz-meta-*).
        // Note: offset is unknown here (not yet appended) and partition/timestamp are only present
        // if the application set them explicitly - the broker otherwise assigns them after send.
        meta.addUserMetadata("kafka-topic", record.topic());
        if (record.partition() != null) {
            meta.addUserMetadata("kafka-partition", record.partition().toString());
        }
        if (record.timestamp() != null) {
            meta.addUserMetadata("kafka-timestamp", record.timestamp().toString());
        }
        if (record.key() != null) {
            meta.addUserMetadata("kafka-key", record.key().toString());
        }
        meta.addUserMetadata("payload-class", record.value().getClass().getName());
        meta.addUserMetadata("payload-size-bytes", Integer.toString(bytes.length));

        // the (optionally regex-transformed) topic name feeds the ${topic} placeholder in the
        // bucket name and object prefix; the reference itself is still published on the real topic
        String resolvedTopic = transformTopic(record.topic());
        String bucket = applyTopicPlaceholder(bucketName, resolvedTopic);
        String objectName = buildObjectName(resolvedTopic);

        try {
            // make sure the target bucket exists before storing the object
            if (!s3client.doesBucketExistV2(bucket)) {
                s3client.createBucket(bucket);
            }

            s3client.putObject(
                    bucket,
                    objectName,
                    stream,
                    meta
            );
        } catch (RuntimeException e) {
            metrics.recordError();
            throw e;
        }
        metrics.recordExternalized(bytes.length);

        ObjectReference objectReference = ObjectReference.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("Person")
                .setBucketName(bucket)
                .setObjectName(objectName)
                .build();

        // publish the reference on the same topic as the original record; the ObjectReference
        // and the original payload schema coexist because the serializer uses a record-aware
        // subject naming strategy (e.g. TopicRecordNameStrategy)
        ProducerRecord<K,ObjectReference> newRecord = new ProducerRecord<>(record.topic(), record.key(), objectReference);

        return newRecord;
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
     * {@inheritDoc}
     */
    @Override
    public void close() {
        JmxMetrics.unregister(metricsName);
    }

    /**
     * Reads the interceptor settings from the producer configuration and builds the S3 client used
     * to store externalized payloads. Missing keys fall back to their defaults. See the class-level
     * documentation for the recognized {@code interceptor.storage.*} keys.
     *
     * @param configs the producer configuration passed by the Kafka client
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        bucketName = getOrDefault(configs, BUCKET_NAME_CONFIG, DEFAULT_BUCKET_NAME);
        sizeThreshold = getIntOrDefault(configs, SIZE_THRESHOLD_CONFIG, DEFAULT_SIZE_THRESHOLD);
        objectPrefix = normalizePrefix(getOrDefault(configs, OBJECT_PREFIX_CONFIG, DEFAULT_OBJECT_PREFIX));
        partitionGranularity = parseGranularity(getOrDefault(configs, PARTITION_GRANULARITY_CONFIG, PARTITION_NONE));
        String topicRegex = getOrDefault(configs, TOPIC_TRANSFORM_REGEX_CONFIG, "");
        topicTransformPattern = topicRegex.isEmpty() ? null : Pattern.compile(topicRegex);
        topicTransformReplacement = getOrDefault(configs, TOPIC_TRANSFORM_REPLACEMENT_CONFIG, "");
        metricsName = JmxMetrics.register(metrics, "ProducerInterceptor", getOrDefault(configs, "client.id", ""));
        String accessKey = getOrDefault(configs, ACCESS_KEY_CONFIG, DEFAULT_ACCESS_KEY);
        String secretKey = getOrDefault(configs, SECRET_KEY_CONFIG, DEFAULT_SECRET_KEY);
        String serviceEndpoint = getOrDefault(configs, SERVICE_ENDPOINT_CONFIG, DEFAULT_SERVICE_ENDPOINT);
        String region = getOrDefault(configs, REGION_CONFIG, DEFAULT_REGION);

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    /**
     * Builds the S3 object key for a new stored object: an optional {@link #OBJECT_PREFIX_CONFIG
     * prefix} (with the {@value #TOPIC_PLACEHOLDER} placeholder resolved to {@code resolvedTopic}),
     * then an optional time-based {@link #PARTITION_GRANULARITY_CONFIG partition} path, then a random
     * UUID (e.g. {@code my/prefix/2026/10/23/14/<uuid>}).
     */
    private String buildObjectName(String resolvedTopic) {
        String prefix = applyTopicPlaceholder(objectPrefix, resolvedTopic);
        StringBuilder key = new StringBuilder();
        if (!prefix.isEmpty()) {
            key.append(prefix).append('/');
        }
        String partition = partitionPath();
        if (!partition.isEmpty()) {
            key.append(partition).append('/');
        }
        key.append(UUID.randomUUID());
        return key.toString();
    }

    /** Applies the optional topic-transform regex to the given topic name. */
    private String transformTopic(String topic) {
        return topicTransformPattern == null
                ? topic
                : topicTransformPattern.matcher(topic).replaceAll(topicTransformReplacement);
    }

    /** Replaces every {@value #TOPIC_PLACEHOLDER} occurrence in {@code template} with the topic name. */
    private static String applyTopicPlaceholder(String template, String topic) {
        return template.contains(TOPIC_PLACEHOLDER) ? template.replace(TOPIC_PLACEHOLDER, topic) : template;
    }

    /** Returns the UTC date path for the configured granularity, or an empty string for {@code none}. */
    private String partitionPath() {
        Instant now = Instant.now();
        switch (partitionGranularity) {
            case PARTITION_MONTHLY:
                return MONTHLY_FMT.format(now);
            case PARTITION_DAILY:
                return DAILY_FMT.format(now);
            case PARTITION_HOURLY:
                return HOURLY_FMT.format(now);
            default:
                return "";
        }
    }

    /** Validates and normalizes the partition granularity value, failing fast on an unknown setting. */
    private static String parseGranularity(String value) {
        String g = value.trim().toLowerCase();
        switch (g) {
            case PARTITION_NONE:
            case PARTITION_MONTHLY:
            case PARTITION_DAILY:
            case PARTITION_HOURLY:
                return g;
            default:
                throw new IllegalArgumentException("Invalid " + PARTITION_GRANULARITY_CONFIG + ": '" + value
                        + "' (expected one of none, monthly, daily, hourly)");
        }
    }

    /** Trims whitespace and strips leading/trailing slashes so the prefix yields a well-formed key. */
    private static String normalizePrefix(String prefix) {
        String p = prefix.trim();
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        while (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
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
