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
import com.trivadis.demo.dto.BusinessEvent;
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
    private AmazonS3 s3client;

    // DTOs are generated into the same package as BusinessEvent (e.g. com.trivadis.demo.dto)
    private static final String DTO_PACKAGE = BusinessEvent.class.getPackage().getName();

    @SuppressWarnings("unchecked")
    private static Class<? extends SpecificRecordBase> resolvePayloadClass(String objectType) {
        try {
            // objectType may be a simple name ("Person") or already fully qualified
            String className = objectType.contains(".") ? objectType : DTO_PACKAGE + "." + objectType;
            return (Class<? extends SpecificRecordBase>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unknown payload type: " + objectType, e);
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
            // retrieve Blob referenced by the BusinessEvent
            BusinessEvent be = (BusinessEvent) r.value();
            S3Object s3object = s3client.getObject(be.getBucketName(), be.getObjectName());
            try {
                byte[] byteArray = IOUtils.toByteArray(s3object.getObjectContent());
                // resolve the payload type from the objectType field of the BusinessEvent
                Class<? extends SpecificRecordBase> payloadClass = resolvePayloadClass(be.getObjectType().toString());
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
        AWSCredentials credentials = new BasicAWSCredentials(
                "admin",
                "abc123abc123!"
        );

        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://dataplatform:9005", Regions.US_EAST_2.getName()))
                .withPathStyleAccessEnabled(true)
                .build();
    }
}
