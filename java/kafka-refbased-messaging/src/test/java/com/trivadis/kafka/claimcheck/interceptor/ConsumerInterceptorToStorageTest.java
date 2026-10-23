package com.trivadis.kafka.claimcheck.interceptor;

import com.amazonaws.services.s3.model.S3Object;
import com.trivadis.demo.dto.ClaimCheck;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ConsumerInterceptorMetricsMBean;
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

    private ClaimCheck payload() {
        return ClaimCheck.newBuilder()
                .setTs(Instant.ofEpochMilli(123))
                .setPayloadType("Person")
                .setPayloadReference("s3://pb/po")
                .build();
    }

    private static ConsumerRecords<Long, Object> singleton(ConsumerRecord<Long, Object> record) {
        Map<TopicPartition, List<ConsumerRecord<Long, Object>>> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()), Collections.singletonList(record));
        return new ConsumerRecords<>(map);
    }

    /** Configure the interceptor, then replace the S3 client inside its storage backend with the fake. */
    private ConsumerInterceptorToStorage<Long, Object> configured(FakeAmazonS3 s3) {
        ConsumerInterceptorToStorage<Long, Object> interceptor = new ConsumerInterceptorToStorage<>();
        interceptor.configure(new HashMap<>());
        TestSupport.setField(TestSupport.getField(interceptor, "storage"), "s3client", s3);
        return interceptor;
    }

    @Test
    void deserializeRoundTripsProducerSerialization() throws Exception {
        ClaimCheck original = payload();
        byte[] bytes = ProducerInterceptorToStorage.serialize(original);

        ClaimCheck back = ConsumerInterceptorToStorage.deserialize(bytes, ClaimCheck.class);

        assertEquals(original, back);
    }

    @Test
    void nonReferenceRecordsArePassedThrough() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        ConsumerInterceptorToStorage<Long, Object> interceptor = configured(s3);

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
        ClaimCheck storedPayload = payload();
        byte[] bytes = ProducerInterceptorToStorage.serialize(storedPayload);

        S3Object s3o = new S3Object();
        s3o.setBucketName("bucket");
        s3o.setKey("key123");
        s3o.setObjectContent(new ByteArrayInputStream(bytes));

        FakeAmazonS3 s3 = new FakeAmazonS3();
        s3.getResponse = s3o;

        ConsumerInterceptorToStorage<Long, Object> interceptor = configured(s3);

        ClaimCheck reference = ClaimCheck.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("ClaimCheck") // resolved against ClaimCheck's package
                .setPayloadReference("s3://bucket/key123")
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
