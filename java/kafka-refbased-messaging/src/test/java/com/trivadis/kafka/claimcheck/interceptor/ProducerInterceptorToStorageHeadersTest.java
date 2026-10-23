package com.trivadis.kafka.claimcheck.interceptor;

import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetricsMBean;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the serializer-agnostic header variant using a plain {@link StringSerializer} payload (no
 * Avro) to demonstrate that the interceptor treats the value as opaque.
 */
class ProducerInterceptorToStorageHeadersTest {

    /** Configure the interceptor, then replace its internally-built S3 client with the given fake. */
    private ProducerInterceptorToStorageHeaders<Long, String> configured(Map<String, Object> cfg, FakeAmazonS3 s3) {
        cfg.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ProducerInterceptorToStorageHeaders<Long, String> interceptor = new ProducerInterceptorToStorageHeaders<>();
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
        // default threshold is 1 MiB, the sample payload is tiny -> stays inline
        ProducerInterceptorToStorageHeaders<Long, String> i = configured(new HashMap<>(), s3);

        ProducerRecord<Long, String> record = new ProducerRecord<>("t", 1L, "small");
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
        ProducerInterceptorToStorageHeaders<Long, String> i = configured(cfg, s3);

        ProducerRecord<?, ?> result = i.onSend(new ProducerRecord<>("mytopic", 7L, "hello"));

        // same topic and key, value dropped to null, reference carried in headers
        assertEquals("mytopic", result.topic());
        assertEquals(Long.valueOf(7L), result.key());
        assertNull(result.value());

        assertEquals("bucket", header(result, ProducerInterceptorToStorageHeaders.HEADER_BUCKET));
        assertNotNull(header(result, ProducerInterceptorToStorageHeaders.HEADER_OBJECT));
        // payload type header carries the value's Java class name (informational)
        assertEquals("java.lang.String", header(result, ProducerInterceptorToStorageHeaders.HEADER_PAYLOAD_TYPE));
        assertNotNull(header(result, ProducerInterceptorToStorageHeaders.HEADER_TS));

        assertEquals(1, s3.putCount);
        assertEquals("bucket", s3.putBucket);
        assertEquals(header(result, ProducerInterceptorToStorageHeaders.HEADER_OBJECT), s3.putKey);

        ProducerInterceptorMetricsMBean m = (ProducerInterceptorMetricsMBean) TestSupport.getField(i, "metrics");
        assertEquals(1, m.getRecordsExternalized());
        assertEquals(0, m.getRecordsInline());
        assertTrue(m.getBytesExternalized() > 0);
        assertEquals(0, m.getExternalizationErrors());
    }

    @Test
    void nullValueIsPassedThrough() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorageHeaders.SIZE_THRESHOLD_CONFIG, "0");
        ProducerInterceptorToStorageHeaders<Long, String> i = configured(cfg, s3);

        ProducerRecord<Long, String> record = new ProducerRecord<>("t", 1L, null);
        ProducerRecord<?, ?> result = i.onSend(record);

        assertSame(record, result);
        assertEquals(0, s3.putCount);
    }

    @Test
    void existingHeadersArePreserved() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorageHeaders.SIZE_THRESHOLD_CONFIG, "0");
        ProducerInterceptorToStorageHeaders<Long, String> i = configured(cfg, s3);

        ProducerRecord<Long, String> record = new ProducerRecord<>("t", 1L, "hello");
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
        ProducerInterceptorToStorageHeaders<Long, String> i = configured(cfg, s3);

        i.onSend(new ProducerRecord<>("mytopic", 7L, "hello"));

        Map<String, String> um = s3.putMetadata.getUserMetadata();
        assertEquals("mytopic", um.get("kafka-topic"));
        assertEquals("7", um.get("kafka-key"));
        assertEquals("java.lang.String", um.get("payload-class"));
        assertNotNull(um.get("payload-size-bytes"));
    }
}
