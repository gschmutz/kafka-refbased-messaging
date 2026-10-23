package com.trivadis.kafka.claimcheck.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

/**
 * {@link StorageBackend} backed by an S3-compatible object store (AWS S3 or MinIO).
 * <p>
 * This class encapsulates the object-storage mechanics of Reference-Based Messaging (the Claim Check
 * pattern) &mdash; the same logic the {@code ProducerInterceptorToStorage} /
 * {@code ConsumerInterceptorToStorage} interceptors apply inline &mdash; behind the
 * {@link StorageBackend} abstraction:
 * <ul>
 *   <li>{@link #checkIn(ProducerRecord)} stores the (already-serialized) record value as an object
 *       and returns a {@link StorageReference} holding a {@code s3://bucket/key} URI;</li>
 *   <li>{@link #checkOut(StorageReference)} parses that URI and fetches the object bytes back.</li>
 * </ul>
 * The target bucket name and object key are derived exactly as in the interceptor: an optional
 * {@code ${topic}} placeholder (fed by the record's topic, optionally regex-transformed), an optional
 * key prefix, and an optional time-based partition path, followed by a random UUID.
 * <p>
 * The backend is payload-type agnostic and only moves bytes; mapping the {@link StorageReference} to
 * a messaging reference (e.g. an Avro {@code ClaimCheck}) is the caller's responsibility.
 * <p>
 * <strong>Configuration</strong> (read from {@link #configure(Map)}; each key has a default):
 * <ul>
 *   <li>{@link #BUCKET_NAME_CONFIG} &ndash; target bucket (may contain the {@value #TOPIC_PLACEHOLDER} placeholder)</li>
 *   <li>{@link #OBJECT_PREFIX_CONFIG} &ndash; optional key prefix (may contain {@value #TOPIC_PLACEHOLDER})</li>
 *   <li>{@link #PARTITION_GRANULARITY_CONFIG} &ndash; time partitioning: {@code none}/{@code monthly}/{@code daily}/{@code hourly}</li>
 *   <li>{@link #TOPIC_TRANSFORM_REGEX_CONFIG} / {@link #TOPIC_TRANSFORM_REPLACEMENT_CONFIG} &ndash; optional regex applied to the topic before it fills {@value #TOPIC_PLACEHOLDER}</li>
 *   <li>{@link #SERVICE_ENDPOINT_CONFIG} / {@link #REGION_CONFIG} / {@link #ACCESS_KEY_CONFIG} / {@link #SECRET_KEY_CONFIG} &ndash; object-store connection</li>
 * </ul>
 */
public class S3StorageBackend implements StorageBackend {

    /** URI scheme used for the reference (e.g. {@code s3://bucket/key}). */
    public static final String URI_SCHEME = "s3";

    /** Config key for the target bucket for the stored objects. */
    public static final String BUCKET_NAME_CONFIG = "interceptor.storage.bucket.name";
    /** Config key for the S3 access key. */
    public static final String ACCESS_KEY_CONFIG = "interceptor.storage.access.key";
    /** Config key for the S3 secret key. */
    public static final String SECRET_KEY_CONFIG = "interceptor.storage.secret.key";
    /** Config key for the S3 service endpoint. */
    public static final String SERVICE_ENDPOINT_CONFIG = "interceptor.storage.service.endpoint";
    /** Config key for the S3 signing region. */
    public static final String REGION_CONFIG = "interceptor.storage.region";
    /** Config key for the object key prefix ("folder") under which stored objects are placed. */
    public static final String OBJECT_PREFIX_CONFIG = "interceptor.storage.object.prefix";
    /** Config key for time-based partitioning ({@code none}/{@code monthly}/{@code daily}/{@code hourly}). */
    public static final String PARTITION_GRANULARITY_CONFIG = "interceptor.storage.partition.granularity";
    /** Config key for an optional regex applied to the topic before it fills {@value #TOPIC_PLACEHOLDER}. */
    public static final String TOPIC_TRANSFORM_REGEX_CONFIG = "interceptor.storage.topic.transform.regex";
    /** Config key for the replacement string used with {@link #TOPIC_TRANSFORM_REGEX_CONFIG}. */
    public static final String TOPIC_TRANSFORM_REPLACEMENT_CONFIG = "interceptor.storage.topic.transform.replacement";

    /** Placeholder, usable in {@link #BUCKET_NAME_CONFIG} and {@link #OBJECT_PREFIX_CONFIG}, replaced with the (transformed) topic name. */
    public static final String TOPIC_PLACEHOLDER = "${topic}";

    private static final String DEFAULT_BUCKET_NAME = "large-object-bucket";
    private static final String DEFAULT_ACCESS_KEY = "admin";
    private static final String DEFAULT_SECRET_KEY = "abc123abc123!";
    private static final String DEFAULT_SERVICE_ENDPOINT = "http://dataplatform:9005";
    private static final String DEFAULT_REGION = Regions.US_EAST_2.getName();
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
    private String objectPrefix = DEFAULT_OBJECT_PREFIX;
    private String partitionGranularity = PARTITION_NONE;
    private Pattern topicTransformPattern;
    private String topicTransformReplacement = "";

    /**
     * Stores the record's (already-serialized) value as an object in the configured bucket, creating
     * the bucket on demand, and returns a {@link StorageReference} holding a {@code s3://bucket/key}
     * URI.
     *
     * @param largeRecord the record whose {@code byte[]} value should be externalized
     * @return the reference to the stored object
     */
    @Override
    public StorageReference checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
        byte[] payload = largeRecord.value();

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(payload.length);
        meta.setContentType("application/octet-stream");
        // attach the Kafka context available at produce time as S3 user metadata (x-amz-meta-*).
        // Note: offset is unknown here (not yet appended); partition/timestamp are only present if
        // the application set them explicitly - the broker otherwise assigns them after send.
        meta.addUserMetadata("kafka-topic", largeRecord.topic());
        if (largeRecord.partition() != null) {
            meta.addUserMetadata("kafka-partition", largeRecord.partition().toString());
        }
        if (largeRecord.timestamp() != null) {
            meta.addUserMetadata("kafka-timestamp", largeRecord.timestamp().toString());
        }
        meta.addUserMetadata("payload-size-bytes", Integer.toString(payload.length));

        // the (optionally regex-transformed) topic name feeds the ${topic} placeholder in the
        // bucket name and object prefix
        String resolvedTopic = transformTopic(largeRecord.topic());
        String bucket = applyTopicPlaceholder(bucketName, resolvedTopic);
        String objectName = buildObjectName(resolvedTopic);

        // make sure the target bucket exists before storing the object
        if (!s3client.doesBucketExistV2(bucket)) {
            s3client.createBucket(bucket);
        }

        s3client.putObject(bucket, objectName, new ByteArrayInputStream(payload), meta);

        // the reference is a single URI encoding both the bucket and the object key
        return new StorageReference(URI_SCHEME + "://" + bucket + "/" + objectName);
    }

    /**
     * Fetches the object referenced by the {@code s3://bucket/key} URI held in the
     * {@link StorageReference} and returns its bytes.
     *
     * @param storageReference the reference issued by {@link #checkIn(ProducerRecord)}
     * @return the stored payload bytes
     * @throws RuntimeException if the object cannot be fetched or read
     */
    @Override
    public byte[] checkOut(StorageReference storageReference) {
        URI ref = URI.create(storageReference.getURI());
        String bucket = ref.getHost();
        String key = ref.getPath().startsWith("/") ? ref.getPath().substring(1) : ref.getPath();

        S3Object s3object = s3client.getObject(bucket, key);
        try {
            return IOUtils.toByteArray(s3object.getObjectContent());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the storage settings from the configuration and builds the S3 client. Missing keys fall
     * back to their defaults.
     *
     * @param configs the configuration (the same map Kafka passes to interceptors / serializers)
     */
    @Override
    public void configure(Map<String, ?> configs) {
        bucketName = getOrDefault(configs, BUCKET_NAME_CONFIG, DEFAULT_BUCKET_NAME);
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
     * Shuts down the S3 client and releases its resources.
     */
    @Override
    public void close() {
        if (s3client != null) {
            s3client.shutdown();
        }
    }

    /**
     * Builds the S3 object key for a new stored object: an optional {@link #OBJECT_PREFIX_CONFIG
     * prefix} (with {@value #TOPIC_PLACEHOLDER} resolved to {@code resolvedTopic}), then an optional
     * time-based {@link #PARTITION_GRANULARITY_CONFIG partition} path, then a random UUID.
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
}
