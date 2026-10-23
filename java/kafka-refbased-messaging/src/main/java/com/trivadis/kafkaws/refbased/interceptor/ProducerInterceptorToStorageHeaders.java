package com.trivadis.kafkaws.refbased.interceptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * Header-based variant of {@link ProducerInterceptorToStorage} implementing Reference-Based
 * Messaging (the Claim Check pattern) for large Avro payloads.
 * <p>
 * This variant differs from {@link ProducerInterceptorToStorage} only in <em>how</em> the reference
 * is carried on the topic. Instead of replacing the record value with an Avro
 * {@link com.trivadis.demo.dto.ObjectReference}, it externalizes the payload to object storage and
 * then publishes the record with:
 * <ul>
 *   <li>a <strong>{@code null} value</strong> &mdash; which every Kafka {@code Serializer} accepts
 *       regardless of type, so the configured {@code value.serializer} is irrelevant for
 *       externalized records; and</li>
 *   <li>the reference data in <strong>Kafka record headers</strong> ({@link #HEADER_BUCKET},
 *       {@link #HEADER_OBJECT}, {@link #HEADER_PAYLOAD_TYPE}, {@link #HEADER_TS}).</li>
 * </ul>
 * The matching {@link ConsumerInterceptorToStorageHeaders} reverses the process, reading the headers
 * and fetching / deserializing the externalized payload from storage.
 * <p>
 * As with the Avro-reference variant, records whose serialized value is at or below the configured
 * {@link #SIZE_THRESHOLD_CONFIG size threshold} are passed through unchanged (inline, with no
 * reference headers), and each stored object carries the same Kafka context as S3 user metadata.
 * <p>
 * <strong>Caveat.</strong> A {@code null} value is the log-compaction tombstone marker, so this
 * variant must not be used on a compacted topic (every externalized record would mark its key for
 * deletion). On normal delete/time-retention topics it is safe.
 * <p>
 * <strong>Configuration</strong> is identical to {@link ProducerInterceptorToStorage}; see that
 * class for the full list of {@code interceptor.storage.*} keys.
 *
 * @param <K> record key type
 * @param <V> record value type; must be an Avro {@link org.apache.avro.specific.SpecificRecordBase}
 */
public class ProducerInterceptorToStorageHeaders<K,V extends org.apache.avro.specific.SpecificRecordBase>
                implements ProducerInterceptor<K, V> {

    /** Common prefix for all reference headers written by this interceptor. */
    public static final String HEADER_PREFIX = "refbased.";
    /** Header carrying the storage bucket holding the externalized payload. Its presence marks a record as externalized. */
    public static final String HEADER_BUCKET = HEADER_PREFIX + "bucket";
    /** Header carrying the object name (key) of the externalized payload within the bucket. */
    public static final String HEADER_OBJECT = HEADER_PREFIX + "object";
    /** Header carrying the payload type (simple or fully-qualified name) used by the consumer to resolve the Avro class. */
    public static final String HEADER_PAYLOAD_TYPE = HEADER_PREFIX + "payloadType";
    /** Header carrying the reference creation timestamp, in epoch milliseconds (UTF-8 decimal string). */
    public static final String HEADER_TS = HEADER_PREFIX + "ts";

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
     * between the {@link #OBJECT_PREFIX_CONFIG prefix} and the object name. Accepted values
     * (case-insensitive): {@code none} (default), {@code monthly} ({@code yyyy/MM}), {@code daily}
     * ({@code yyyy/MM/dd}) and {@code hourly} ({@code yyyy/MM/dd/HH}).
     */
    public static final String PARTITION_GRANULARITY_CONFIG = "interceptor.storage.partition.granularity";
    /**
     * Producer config key for an optional regex applied to the record's topic name before it is
     * substituted into {@link #BUCKET_NAME_CONFIG} / {@link #OBJECT_PREFIX_CONFIG} via the
     * {@value #TOPIC_PLACEHOLDER} placeholder. Empty by default. Used together with
     * {@link #TOPIC_TRANSFORM_REPLACEMENT_CONFIG}.
     */
    public static final String TOPIC_TRANSFORM_REGEX_CONFIG = "interceptor.storage.topic.transform.regex";
    /**
     * Producer config key for the replacement string used with {@link #TOPIC_TRANSFORM_REGEX_CONFIG}
     * (standard {@link java.util.regex.Matcher#replaceAll(String)} semantics). Empty by default.
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

    /**
     * Serializes an Avro {@link org.apache.avro.specific.SpecificRecordBase} to its Avro binary
     * encoding (no Schema Registry framing / magic byte) &mdash; the raw payload stored as the
     * externalized object.
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
     * stays inline. Otherwise the bytes are stored in the configured bucket (created on demand), and
     * a new record is returned that keeps the original topic and key but carries a {@code null} value
     * and the reference as headers.
     *
     * @param record the record about to be sent
     * @return the original record (inline) or a header-carrying, null-valued replacement (externalized)
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
        // topic unchanged (the consumer interceptor passes records without reference headers through)
        if (bytes.length <= sizeThreshold) {
            return record;
        }

        // capture the payload type before the value is dropped; the consumer uses it to resolve the
        // Avro class for deserialization (resolved against the DTO package by simple name)
        String payloadType = record.value().getClass().getSimpleName();

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

        // carry the reference in headers (preserving any headers the application already set) and
        // send a null value, so the configured value serializer is irrelevant for this record
        RecordHeaders headers = new RecordHeaders();
        record.headers().forEach(headers::add);
        headers.add(new RecordHeader(HEADER_BUCKET, bucket.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(HEADER_OBJECT, objectName.getBytes(StandardCharsets.UTF_8)));
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
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    /**
     * Reads the interceptor settings from the producer configuration and builds the S3 client used
     * to store externalized payloads. Missing keys fall back to their defaults. See
     * {@link ProducerInterceptorToStorage} for the recognized {@code interceptor.storage.*} keys.
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
