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
import java.util.List;
import java.util.Map;

public class ConsumerInterceptorToStorage<K, V> implements ConsumerInterceptor<K, V> {
    private AmazonS3 s3client;

    public static <T extends SpecificRecordBase> T deserialize(byte[] value, Class clazz) throws IOException {
        DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        return datumReader.read(null, decoder);
    }

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        List<ConsumerRecord<K,V> newRecords = new ArrayList<>();
        records.forEach(r -> {
            // retrieve Blob
            BusinessEvent be = (BusinessEvent)r.value();
            S3Object s3object = s3client.getObject(be.getBucketName(), be.getObjectName());
            try {
                byte[] byteArray = IOUtils.toByteArray(s3object.getObjectContent());
                SpecificRecordBase srb = deserialize(byteArray, BusinessEvent.class);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return new ConsumerRecords<>()
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
                "minio",
                "1234567890"
        );

        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://dataplatform:9000", Regions.US_EAST_2.getName()))
                .withPathStyleAccessEnabled(true)
                .build();
    }
}
