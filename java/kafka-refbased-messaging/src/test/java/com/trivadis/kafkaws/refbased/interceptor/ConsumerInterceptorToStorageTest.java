package com.trivadis.kafkaws.refbased.interceptor;

import com.amazonaws.services.s3.model.S3Object;
import com.trivadis.demo.dto.ObjectReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerInterceptorToStorageTest {

    private ObjectReference payload() {
        return ObjectReference.newBuilder()
                .setTs(Instant.ofEpochMilli(123))
                .setPayloadType("Person")
                .setBucketName("pb")
                .setObjectName("po")
                .build();
    }

    private static ConsumerRecords<Long, Object> singleton(ConsumerRecord<Long, Object> record) {
        Map<TopicPartition, List<ConsumerRecord<Long, Object>>> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()), Collections.singletonList(record));
        return new ConsumerRecords<>(map);
    }

    @Test
    void deserializeRoundTripsProducerSerialization() throws Exception {
        ObjectReference original = payload();
        byte[] bytes = ProducerInterceptorToStorage.serialize(original);

        ObjectReference back = ConsumerInterceptorToStorage.deserialize(bytes, ObjectReference.class);

        assertEquals(original, back);
    }

    @Test
    void nonReferenceRecordsArePassedThrough() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        ConsumerInterceptorToStorage<Long, Object> interceptor = new ConsumerInterceptorToStorage<>();
        TestSupport.setField(interceptor, "s3client", s3);

        ConsumerRecord<Long, Object> record = new ConsumerRecord<>("t", 0, 0L, 1L, "just-a-string");

        ConsumerRecords<Long, Object> out = interceptor.onConsume(singleton(record));

        Iterator<ConsumerRecord<Long, Object>> it = out.iterator();
        assertTrue(it.hasNext());
        assertEquals("just-a-string", it.next().value());
        assertFalse(it.hasNext());
        assertEquals(0, s3.getCount);
    }

    @Test
    void referenceRecordsAreMaterializedFromStorage() throws Exception {
        ObjectReference storedPayload = payload();
        byte[] bytes = ProducerInterceptorToStorage.serialize(storedPayload);

        S3Object s3o = new S3Object();
        s3o.setBucketName("bucket");
        s3o.setKey("key123");
        s3o.setObjectContent(new ByteArrayInputStream(bytes));

        FakeAmazonS3 s3 = new FakeAmazonS3();
        s3.getResponse = s3o;

        ConsumerInterceptorToStorage<Long, Object> interceptor = new ConsumerInterceptorToStorage<>();
        TestSupport.setField(interceptor, "s3client", s3);

        ObjectReference reference = ObjectReference.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("ObjectReference") // resolved against ObjectReference's package
                .setBucketName("bucket")
                .setObjectName("key123")
                .build();
        ConsumerRecord<Long, Object> record = new ConsumerRecord<>("t", 0, 5L, 9L, reference);

        ConsumerRecords<Long, Object> out = interceptor.onConsume(singleton(record));

        ConsumerRecord<Long, Object> materialized = out.iterator().next();
        assertEquals(storedPayload, materialized.value());
        // metadata is preserved; only the value is replaced
        assertEquals(5L, materialized.offset());
        assertEquals(Long.valueOf(9L), materialized.key());
        assertEquals("bucket", s3.getBucket);
        assertEquals("key123", s3.getKey);

        ConsumerInterceptorMetricsMBean m = (ConsumerInterceptorMetricsMBean) TestSupport.getField(interceptor, "metrics");
        assertEquals(1, m.getRecordsMaterialized());
        assertEquals(0, m.getRecordsPassedThrough());
        assertEquals(bytes.length, m.getBytesFetched());
    }
}
