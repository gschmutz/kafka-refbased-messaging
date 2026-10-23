package com.trivadis.kafka.claimcheck.interceptor;

import com.amazonaws.services.s3.model.S3Object;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ConsumerInterceptorMetricsMBean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the serializer-agnostic header variant using a plain {@link StringDeserializer} payload (no
 * Avro) to demonstrate that the interceptor treats the stored bytes as opaque.
 */
class ConsumerInterceptorToStorageHeadersTest {

    private ConsumerInterceptorToStorageHeaders<Long, Object> configured(FakeAmazonS3 s3) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        ConsumerInterceptorToStorageHeaders<Long, Object> interceptor = new ConsumerInterceptorToStorageHeaders<>();
        interceptor.configure(cfg);
        TestSupport.setField(interceptor, "s3client", s3);
        return interceptor;
    }

    private static ConsumerRecords<Long, Object> singleton(ConsumerRecord<Long, Object> record) {
        Map<TopicPartition, List<ConsumerRecord<Long, Object>>> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()), Collections.singletonList(record));
        return new ConsumerRecords<>(map);
    }

    @Test
    void recordsWithoutReferenceHeadersArePassedThrough() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        ConsumerInterceptorToStorageHeaders<Long, Object> interceptor = configured(s3);

        ConsumerRecord<Long, Object> record = new ConsumerRecord<>("t", 0, 0L, 1L, "just-a-string");

        ConsumerRecords<Long, Object> out = interceptor.onConsume(singleton(record));

        var it = out.iterator();
        assertTrue(it.hasNext());
        assertEquals("just-a-string", it.next().value());
        assertFalse(it.hasNext());
        assertEquals(0, s3.getCount);

        ConsumerInterceptorMetricsMBean m = (ConsumerInterceptorMetricsMBean) TestSupport.getField(interceptor, "metrics");
        assertEquals(0, m.getRecordsMaterialized());
        assertEquals(1, m.getRecordsPassedThrough());
    }

    @Test
    void referenceRecordsAreMaterializedFromHeaders() {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

        S3Object s3o = new S3Object();
        s3o.setBucketName("bucket");
        s3o.setKey("key123");
        s3o.setObjectContent(new ByteArrayInputStream(bytes));

        FakeAmazonS3 s3 = new FakeAmazonS3();
        s3.getResponse = s3o;

        ConsumerInterceptorToStorageHeaders<Long, Object> interceptor = configured(s3);

        // a null-valued record carrying the reference as headers, as the producer interceptor emits
        RecordHeaders headers = new RecordHeaders();
        headers.add(ProducerInterceptorToStorageHeaders.HEADER_BUCKET, "bucket".getBytes(StandardCharsets.UTF_8));
        headers.add(ProducerInterceptorToStorageHeaders.HEADER_OBJECT, "key123".getBytes(StandardCharsets.UTF_8));
        headers.add(ProducerInterceptorToStorageHeaders.HEADER_PAYLOAD_TYPE, "java.lang.String".getBytes(StandardCharsets.UTF_8));
        headers.add(ProducerInterceptorToStorageHeaders.HEADER_TS, "123".getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<Long, Object> record = new ConsumerRecord<>(
                "t", 0, 5L, 9L, TimestampType.CREATE_TIME,
                -1, -1, 9L, (Object) null, headers, Optional.empty());

        ConsumerRecords<Long, Object> out = interceptor.onConsume(singleton(record));

        ConsumerRecord<Long, Object> materialized = out.iterator().next();
        assertEquals("hello", materialized.value());
        // metadata is preserved; only the value is replaced
        assertEquals(5L, materialized.offset());
        assertEquals(Long.valueOf(9L), materialized.key());
        assertEquals("bucket", s3.getBucket);
        assertEquals("key123", s3.getKey);

        ConsumerInterceptorMetricsMBean m = (ConsumerInterceptorMetricsMBean) TestSupport.getField(interceptor, "metrics");
        assertEquals(1, m.getRecordsMaterialized());
        assertEquals(0, m.getRecordsPassedThrough());
        assertEquals(bytes.length, m.getBytesFetched());
        assertEquals(0, m.getFetchErrors());
    }
}
