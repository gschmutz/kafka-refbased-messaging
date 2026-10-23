package com.trivadis.kafka.claimcheck.interceptor;

import com.trivadis.demo.dto.ClaimCheck;
import com.trivadis.kafka.claimcheck.interceptor.metrics.ProducerInterceptorMetricsMBean;
import com.trivadis.kafka.claimcheck.storage.S3StorageBackend;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerInterceptorToStorageTest {

    private ClaimCheck samplePayload() {
        return ClaimCheck.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("Person")
                .setPayloadReference("s3://b/o")
                .build();
    }

    /** Configure the interceptor, then replace the S3 client inside its storage backend with the fake. */
    private ProducerInterceptorToStorage<Long, ClaimCheck> configured(Map<String, Object> cfg, FakeAmazonS3 s3) {
        ProducerInterceptorToStorage<Long, ClaimCheck> interceptor = new ProducerInterceptorToStorage<>();
        interceptor.configure(cfg);
        TestSupport.setField(TestSupport.getField(interceptor, "storage"), "s3client", s3);
        return interceptor;
    }

    @Test
    void configureAppliesDefaults() {
        ProducerInterceptorToStorage<Long, ClaimCheck> i = new ProducerInterceptorToStorage<>();
        i.configure(new HashMap<>());

        assertEquals(1048576, TestSupport.getField(i, "sizeThreshold"));
        Object backend = TestSupport.getField(i, "storage");
        assertEquals("large-object-bucket", TestSupport.getField(backend, "bucketName"));
        assertEquals("", TestSupport.getField(backend, "objectPrefix"));
        assertEquals("none", TestSupport.getField(backend, "partitionGranularity"));
    }

    @Test
    void configureParsesAndNormalizesValues() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "2048");
        cfg.put(S3StorageBackend.BUCKET_NAME_CONFIG, "my-bucket");
        cfg.put(S3StorageBackend.OBJECT_PREFIX_CONFIG, "/foo/bar/");
        cfg.put(S3StorageBackend.PARTITION_GRANULARITY_CONFIG, "DAILY");

        ProducerInterceptorToStorage<Long, ClaimCheck> i = new ProducerInterceptorToStorage<>();
        i.configure(cfg);

        assertEquals(2048, TestSupport.getField(i, "sizeThreshold"));
        Object backend = TestSupport.getField(i, "storage");
        assertEquals("my-bucket", TestSupport.getField(backend, "bucketName"));
        assertEquals("foo/bar", TestSupport.getField(backend, "objectPrefix"));
        assertEquals("daily", TestSupport.getField(backend, "partitionGranularity"));
    }

    @Test
    void configureRejectsInvalidGranularity() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(S3StorageBackend.PARTITION_GRANULARITY_CONFIG, "weekly");

        ProducerInterceptorToStorage<Long, ClaimCheck> i = new ProducerInterceptorToStorage<>();
        assertThrows(IllegalArgumentException.class, () -> i.configure(cfg));
    }

    @Test
    void smallPayloadIsLeftInline() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        // default threshold is 1 MiB, the sample payload is tiny -> stays inline
        ProducerInterceptorToStorage<Long, ClaimCheck> i = configured(new HashMap<>(), s3);

        ProducerRecord<Long, ClaimCheck> record = new ProducerRecord<>("t", 1L, samplePayload());
        ProducerRecord<?, ?> result = i.onSend(record);

        assertSame(record, result);
        assertEquals(0, s3.putCount);
    }

    @Test
    void largePayloadIsExternalized() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "0"); // externalize everything
        cfg.put(S3StorageBackend.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorage<Long, ClaimCheck> i = configured(cfg, s3);

        ProducerRecord<?, ?> result = i.onSend(new ProducerRecord<>("mytopic", 7L, samplePayload()));

        // same topic and key, value replaced by an ClaimCheck
        assertEquals("mytopic", result.topic());
        assertEquals(Long.valueOf(7L), result.key());
        assertTrue(result.value() instanceof ClaimCheck);
        ClaimCheck ref = (ClaimCheck) result.value();
        // the reference is a single s3://bucket/key URI, with the payload type stamped by the interceptor
        assertEquals("s3://bucket/" + s3.putKey, ref.getPayloadReference().toString());
        assertEquals("Person", ref.getPayloadType().toString());

        assertEquals(1, s3.putCount);
        assertEquals("bucket", s3.putBucket);
    }

    @Test
    void storedKeyUsesPrefixAndDailyPartition() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "0");
        cfg.put(S3StorageBackend.BUCKET_NAME_CONFIG, "bucket");
        cfg.put(S3StorageBackend.OBJECT_PREFIX_CONFIG, "pre");
        cfg.put(S3StorageBackend.PARTITION_GRANULARITY_CONFIG, "daily");
        ProducerInterceptorToStorage<Long, ClaimCheck> i = configured(cfg, s3);

        i.onSend(new ProducerRecord<>("t", 1L, samplePayload()));

        assertTrue(s3.putKey.matches("pre/\\d{4}/\\d{2}/\\d{2}/[0-9a-f-]{36}"), "unexpected key: " + s3.putKey);
    }

    @Test
    void topicPlaceholderIsResolvedWithRegexTransform() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "0");
        cfg.put(S3StorageBackend.BUCKET_NAME_CONFIG, "refbased-${topic}");
        cfg.put(S3StorageBackend.OBJECT_PREFIX_CONFIG, "${topic}/data");
        // sanitize the topic into a valid bucket segment: '.'/'_' -> '-'
        cfg.put(S3StorageBackend.TOPIC_TRANSFORM_REGEX_CONFIG, "[._]");
        cfg.put(S3StorageBackend.TOPIC_TRANSFORM_REPLACEMENT_CONFIG, "-");
        ProducerInterceptorToStorage<Long, ClaimCheck> i = configured(cfg, s3);

        ProducerRecord<?, ?> result = i.onSend(new ProducerRecord<>("my.Person_topic", 1L, samplePayload()));

        // topic transformed to "my-Person-topic" for naming, but the reference stays on the real topic
        assertEquals("my.Person_topic", result.topic());
        assertEquals("refbased-my-Person-topic", s3.putBucket);
        assertEquals("s3://refbased-my-Person-topic/" + s3.putKey, ((ClaimCheck) result.value()).getPayloadReference().toString());
        assertTrue(s3.putKey.startsWith("my-Person-topic/data/"), "unexpected key: " + s3.putKey);
    }

    @Test
    void metricsCountInlineAndExternalized() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "1024");
        cfg.put(S3StorageBackend.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorage<Long, ClaimCheck> i = configured(cfg, s3);

        // tiny payload -> inline
        i.onSend(new ProducerRecord<>("t", 1L, samplePayload()));
        // padded payload -> externalized (payloadReference inflated well beyond 1 KiB)
        ClaimCheck big = ClaimCheck.newBuilder()
                .setTs(Instant.now()).setPayloadType("Person")
                .setPayloadReference(new String(new char[4096]).replace('\0', 'x'))
                .build();
        i.onSend(new ProducerRecord<>("t", 2L, big));

        ProducerInterceptorMetricsMBean m = (ProducerInterceptorMetricsMBean) TestSupport.getField(i, "metrics");
        assertEquals(1, m.getRecordsInline());
        assertEquals(1, m.getRecordsExternalized());
        assertTrue(m.getBytesExternalized() > 1024);
        assertEquals(0, m.getExternalizationErrors());
    }

    @Test
    void userMetadataIsAttached() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "0");
        cfg.put(S3StorageBackend.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorage<Long, ClaimCheck> i = configured(cfg, s3);

        i.onSend(new ProducerRecord<>("mytopic", 7L, samplePayload()));

        // the storage backend only sees the serialized bytes, so it records topic and size
        // (the binary key and the Avro class are not available to it)
        Map<String, String> um = s3.putMetadata.getUserMetadata();
        assertEquals("mytopic", um.get("kafka-topic"));
        assertNotNull(um.get("payload-size-bytes"));
    }
}
