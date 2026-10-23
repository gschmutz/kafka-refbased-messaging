package com.trivadis.kafkaws.refbased.interceptor;

import com.trivadis.demo.dto.ObjectReference;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ProducerInterceptorToStorageHeadersTest {

    private ObjectReference samplePayload() {
        return ObjectReference.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("Person")
                .setBucketName("b")
                .setObjectName("o")
                .build();
    }

    /** Configure the interceptor, then replace its internally-built S3 client with the given fake. */
    private ProducerInterceptorToStorageHeaders<Long, ObjectReference> configured(Map<String, Object> cfg, FakeAmazonS3 s3) {
        ProducerInterceptorToStorageHeaders<Long, ObjectReference> interceptor = new ProducerInterceptorToStorageHeaders<>();
        interceptor.configure(cfg);
        TestSupport.setField(interceptor, "s3client", s3);
        return interceptor;
    }

    private static String header(ProducerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void smallPayloadIsLeftInline() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        ProducerInterceptorToStorageHeaders<Long, ObjectReference> i = configured(new HashMap<>(), s3);

        ProducerRecord<Long, ObjectReference> record = new ProducerRecord<>("t", 1L, samplePayload());
        ProducerRecord<?, ?> result = i.onSend(record);

        assertSame(record, result);
        assertEquals(0, s3.putCount);
    }

    @Test
    void largePayloadIsExternalizedWithNullValueAndHeaders() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorageHeaders.SIZE_THRESHOLD_CONFIG, "0"); // externalize everything
        cfg.put(ProducerInterceptorToStorageHeaders.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorageHeaders<Long, ObjectReference> i = configured(cfg, s3);

        ProducerRecord<?, ?> result = i.onSend(new ProducerRecord<>("mytopic", 7L, samplePayload()));

        // same topic and key, value dropped to null, reference carried in headers
        assertEquals("mytopic", result.topic());
        assertEquals(Long.valueOf(7L), result.key());
        assertNull(result.value());

        assertEquals("bucket", header(result, ProducerInterceptorToStorageHeaders.HEADER_BUCKET));
        assertNotNull(header(result, ProducerInterceptorToStorageHeaders.HEADER_OBJECT));
        // payload type is the value's simple class name, used by the consumer to resolve the class
        assertEquals("ObjectReference", header(result, ProducerInterceptorToStorageHeaders.HEADER_PAYLOAD_TYPE));
        assertNotNull(header(result, ProducerInterceptorToStorageHeaders.HEADER_TS));

        assertEquals(1, s3.putCount);
        assertEquals("bucket", s3.putBucket);
        assertEquals(header(result, ProducerInterceptorToStorageHeaders.HEADER_OBJECT), s3.putKey);
    }

    @Test
    void existingHeadersArePreserved() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorageHeaders.SIZE_THRESHOLD_CONFIG, "0");
        ProducerInterceptorToStorageHeaders<Long, ObjectReference> i = configured(cfg, s3);

        ProducerRecord<Long, ObjectReference> record = new ProducerRecord<>("t", 1L, samplePayload());
        record.headers().add("message-version", "1.1".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<?, ?> result = i.onSend(record);

        assertEquals("1.1", header(result, "message-version"));
        assertNotNull(header(result, ProducerInterceptorToStorageHeaders.HEADER_BUCKET));
    }

    @Test
    void userMetadataIsAttached() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorageHeaders.SIZE_THRESHOLD_CONFIG, "0");
        cfg.put(ProducerInterceptorToStorageHeaders.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorageHeaders<Long, ObjectReference> i = configured(cfg, s3);

        i.onSend(new ProducerRecord<>("mytopic", 7L, samplePayload()));

        Map<String, String> um = s3.putMetadata.getUserMetadata();
        assertEquals("mytopic", um.get("kafka-topic"));
        assertEquals("7", um.get("kafka-key"));
        assertEquals(ObjectReference.class.getName(), um.get("payload-class"));
        assertNotNull(um.get("payload-size-bytes"));
    }
}
