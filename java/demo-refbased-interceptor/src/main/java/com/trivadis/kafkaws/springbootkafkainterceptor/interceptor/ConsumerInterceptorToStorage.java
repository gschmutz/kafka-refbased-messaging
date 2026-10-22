package com.trivadis.kafkaws.springbootkafkainterceptor.interceptor;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import com.github.javafaker.Business;
import com.trivadis.demo.dto.ObjectReference;
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

public class ConsumerInterceptorToStorage<K, V> implements ConsumerInterceptor<K, V> {
    /** Consumer config key used to configure the S3 access key. */
    public static final String ACCESS_KEY_CONFIG = "interceptor.storage.access.key";
    /** Consumer config key used to configure the S3 secret key. */
    public static final String SECRET_KEY_CONFIG = "interceptor.storage.secret.key";
    /** Consumer config key used to configure the S3 service endpoint. */
    public static final String SERVICE_ENDPOINT_CONFIG = "interceptor.storage.service.endpoint";
    /** Consumer config key used to configure the S3 signing region. */
    public static final String REGION_CONFIG = "interceptor.storage.region";

    private static final String DEFAULT_ACCESS_KEY = "admin";
    private static final String DEFAULT_SECRET_KEY = "abc123abc123!";
    private static final String DEFAULT_SERVICE_ENDPOINT = "http://dataplatform:9005";
    private static final String DEFAULT_REGION = Regions.US_EAST_2.getName();

    private AmazonS3 s3client;

    // DTOs are generated into the same package as ObjectReference (e.g. com.trivadis.demo.dto)
    private static final String DTO_PACKAGE = ObjectReference.class.getPackage().getName();

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

    public static <T extends SpecificRecordBase> T deserialize(byte[] value, Class clazz) throws IOException {
        DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        return datumReader.read(null, decoder);
    }

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> newRecords = new HashMap<>();
        records.forEach(r -> {
            // retrieve Blob referenced by the ObjectReference
            ObjectReference be = (ObjectReference) r.value();
            S3Object s3object = s3client.getObject(be.getBucketName(), be.getObjectName());
            try {
                byte[] byteArray = IOUtils.toByteArray(s3object.getObjectContent());
                // resolve the payload type from the payloadType field of the ObjectReference
                Class<? extends SpecificRecordBase> payloadClass = resolvePayloadClass(be.getPayloadType().toString());
                SpecificRecordBase srb = deserialize(byteArray, payloadClass);

                // build a new record with the resolved value in place of the reference
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
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
                throw new RuntimeException(e);
            }
        });

        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        // close connection to S3
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        String accessKey = getOrDefault(map, ACCESS_KEY_CONFIG, DEFAULT_ACCESS_KEY);
        String secretKey = getOrDefault(map, SECRET_KEY_CONFIG, DEFAULT_SECRET_KEY);
        String serviceEndpoint = getOrDefault(map, SERVICE_ENDPOINT_CONFIG, DEFAULT_SERVICE_ENDPOINT);
        String region = getOrDefault(map, REGION_CONFIG, DEFAULT_REGION);

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    private static String getOrDefault(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }
}
