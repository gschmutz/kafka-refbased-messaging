package com.trivadis.kafkaws.springbootkafkainterceptor.interceptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

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

    private static final String DEFAULT_BUCKET_NAME = "large-object-bucket";
    private static final String DEFAULT_ACCESS_KEY = "admin";
    private static final String DEFAULT_SECRET_KEY = "abc123abc123!";
    private static final String DEFAULT_SERVICE_ENDPOINT = "http://dataplatform:9005";
    private static final String DEFAULT_REGION = Regions.US_EAST_2.getName();

    private AmazonS3 s3client;
    private String bucketName = DEFAULT_BUCKET_NAME;

    public static <V extends org.apache.avro.specific.SpecificRecordBase> byte[] serialize(V value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<V> writer = new SpecificDatumWriter<V>(value.getSchema());

        writer.write(value, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    @Override
    public ProducerRecord onSend(ProducerRecord<K, V> record) {
        V emptyValue = null;
        byte[] bytes = new byte[0];
        try {
            bytes = serialize(record.value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("application/avro-binary");

        String objectName = UUID.randomUUID().toString();

        // make sure the target bucket exists before storing the object
        if (!s3client.doesBucketExistV2(bucketName)) {
            s3client.createBucket(bucketName);
        }

        s3client.putObject(
                bucketName,
                objectName,
                stream,
                meta
        );

        ObjectReference objectReference = ObjectReference.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("Person")
                .setBucketName(bucketName)
                .setObjectName(objectName)
                .build();

        ProducerRecord<K,ObjectReference> newRecord = new ProducerRecord<>(record.topic() + "-refb", record.key(), objectReference);

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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        bucketName = getOrDefault(configs, BUCKET_NAME_CONFIG, DEFAULT_BUCKET_NAME);
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

    private static String getOrDefault(final Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }

}
