package com.trivadis.kafkaws.springbootkafkainterceptor.interceptor;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.trivadis.demo.dto.BusinessEvent;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class ProducerInterceptorToStorage<K,V extends org.apache.avro.specific.SpecificRecordBase>
                implements ProducerInterceptor<K, V> {

    private AmazonS3 s3client;

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

        String bucketName = "large-object-bucket";
        String objectName = UUID.randomUUID().toString();

        s3client.putObject(
                bucketName,
                objectName,
                stream,
                meta
        );

        BusinessEvent businessEvent = BusinessEvent.newBuilder()
                .setTs(Instant.now())
                .setObjectType("Person")
                .setBucketName(bucketName)
                .setObjectName(objectName).build();

        ProducerRecord<K,BusinessEvent> newRecord = new ProducerRecord<>(record.topic() + "-refb", record.key(), businessEvent);

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
