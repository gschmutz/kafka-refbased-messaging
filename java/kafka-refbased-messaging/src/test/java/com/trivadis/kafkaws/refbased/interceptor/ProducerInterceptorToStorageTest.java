package com.trivadis.kafkaws.refbased.interceptor;

import com.trivadis.demo.dto.ObjectReference;
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

    private ObjectReference samplePayload() {
        return ObjectReference.newBuilder()
                .setTs(Instant.now())
                .setPayloadType("Person")
                .setBucketName("b")
                .setObjectName("o")
                .build();
    }

    /** Configure the interceptor, then replace its internally-built S3 client with the given fake. */
    private ProducerInterceptorToStorage<Long, ObjectReference> configured(Map<String, Object> cfg, FakeAmazonS3 s3) {
        ProducerInterceptorToStorage<Long, ObjectReference> interceptor = new ProducerInterceptorToStorage<>();
        interceptor.configure(cfg);
        TestSupport.setField(interceptor, "s3client", s3);
        return interceptor;
    }

    @Test
    void configureAppliesDefaults() {
        ProducerInterceptorToStorage<Long, ObjectReference> i = new ProducerInterceptorToStorage<>();
        i.configure(new HashMap<>());

        assertEquals(1048576, TestSupport.getField(i, "sizeThreshold"));
        assertEquals("large-object-bucket", TestSupport.getField(i, "bucketName"));
        assertEquals("", TestSupport.getField(i, "objectPrefix"));
        assertEquals("none", TestSupport.getField(i, "partitionGranularity"));
    }

    @Test
    void configureParsesAndNormalizesValues() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "2048");
        cfg.put(ProducerInterceptorToStorage.BUCKET_NAME_CONFIG, "my-bucket");
        cfg.put(ProducerInterceptorToStorage.OBJECT_PREFIX_CONFIG, "/foo/bar/");
        cfg.put(ProducerInterceptorToStorage.PARTITION_GRANULARITY_CONFIG, "DAILY");

        ProducerInterceptorToStorage<Long, ObjectReference> i = new ProducerInterceptorToStorage<>();
        i.configure(cfg);

        assertEquals(2048, TestSupport.getField(i, "sizeThreshold"));
        assertEquals("my-bucket", TestSupport.getField(i, "bucketName"));
        assertEquals("foo/bar", TestSupport.getField(i, "objectPrefix"));
        assertEquals("daily", TestSupport.getField(i, "partitionGranularity"));
    }

    @Test
    void configureRejectsInvalidGranularity() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.PARTITION_GRANULARITY_CONFIG, "weekly");

        ProducerInterceptorToStorage<Long, ObjectReference> i = new ProducerInterceptorToStorage<>();
        assertThrows(IllegalArgumentException.class, () -> i.configure(cfg));
    }

    @Test
    void smallPayloadIsLeftInline() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        // default threshold is 1 MiB, the sample payload is tiny -> stays inline
        ProducerInterceptorToStorage<Long, ObjectReference> i = configured(new HashMap<>(), s3);

        ProducerRecord<Long, ObjectReference> record = new ProducerRecord<>("t", 1L, samplePayload());
        ProducerRecord<?, ?> result = i.onSend(record);

        assertSame(record, result);
        assertEquals(0, s3.putCount);
    }

    @Test
    void largePayloadIsExternalized() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "0"); // externalize everything
        cfg.put(ProducerInterceptorToStorage.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorage<Long, ObjectReference> i = configured(cfg, s3);

        ProducerRecord<?, ?> result = i.onSend(new ProducerRecord<>("mytopic", 7L, samplePayload()));

        // same topic and key, value replaced by an ObjectReference
        assertEquals("mytopic", result.topic());
        assertEquals(Long.valueOf(7L), result.key());
        assertTrue(result.value() instanceof ObjectReference);
        ObjectReference ref = (ObjectReference) result.value();
        assertEquals("bucket", ref.getBucketName().toString());
        assertNotNull(ref.getObjectName());

        assertEquals(1, s3.putCount);
        assertEquals("bucket", s3.putBucket);
        assertEquals(ref.getObjectName().toString(), s3.putKey);
    }

    @Test
    void storedKeyUsesPrefixAndDailyPartition() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "0");
        cfg.put(ProducerInterceptorToStorage.BUCKET_NAME_CONFIG, "bucket");
        cfg.put(ProducerInterceptorToStorage.OBJECT_PREFIX_CONFIG, "pre");
        cfg.put(ProducerInterceptorToStorage.PARTITION_GRANULARITY_CONFIG, "daily");
        ProducerInterceptorToStorage<Long, ObjectReference> i = configured(cfg, s3);

        i.onSend(new ProducerRecord<>("t", 1L, samplePayload()));

        assertTrue(s3.putKey.matches("pre/\\d{4}/\\d{2}/\\d{2}/[0-9a-f-]{36}"), "unexpected key: " + s3.putKey);
    }

    @Test
    void topicPlaceholderIsResolvedWithRegexTransform() {
        FakeAmazonS3 s3 = new FakeAmazonS3();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "0");
        cfg.put(ProducerInterceptorToStorage.BUCKET_NAME_CONFIG, "refbased-${topic}");
        cfg.put(ProducerInterceptorToStorage.OBJECT_PREFIX_CONFIG, "${topic}/data");
        // sanitize the topic into a valid bucket segment: '.'/'_' -> '-'
        cfg.put(ProducerInterceptorToStorage.TOPIC_TRANSFORM_REGEX_CONFIG, "[._]");
        cfg.put(ProducerInterceptorToStorage.TOPIC_TRANSFORM_REPLACEMENT_CONFIG, "-");
        ProducerInterceptorToStorage<Long, ObjectReference> i = configured(cfg, s3);

        ProducerRecord<?, ?> result = i.onSend(new ProducerRecord<>("my.Person_topic", 1L, samplePayload()));

        // topic transformed to "my-Person-topic" for naming, but the reference stays on the real topic
        assertEquals("my.Person_topic", result.topic());
        assertEquals("refbased-my-Person-topic", s3.putBucket);
        assertEquals("refbased-my-Person-topic", ((ObjectReference) result.value()).getBucketName().toString());
        assertTrue(s3.putKey.startsWith("my-Person-topic/data/"), "unexpected key: " + s3.putKey);
    }

    @Test
    void metricsCountInlineAndExternalized() {
        FakeAmazonS3 s3 = new FakeAmazonS3();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerInterceptorToStorage.SIZE_THRESHOLD_CONFIG, "1024");
        cfg.put(ProducerInterceptorToStorage.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorage<Long, ObjectReference> i = configured(cfg, s3);

        // tiny payload -> inline
        i.onSend(new ProducerRecord<>("t", 1L, samplePayload()));
        // padded payload -> externalized (objectName inflated well beyond 1 KiB)
        ObjectReference big = ObjectReference.newBuilder()
                .setTs(Instant.now()).setPayloadType("Person")
                .setBucketName("b").setObjectName(new String(new char[4096]).replace('\0', 'x'))
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
        cfg.put(ProducerInterceptorToStorage.BUCKET_NAME_CONFIG, "bucket");
        ProducerInterceptorToStorage<Long, ObjectReference> i = configured(cfg, s3);

        i.onSend(new ProducerRecord<>("mytopic", 7L, samplePayload()));

        Map<String, String> um = s3.putMetadata.getUserMetadata();
        assertEquals("mytopic", um.get("kafka-topic"));
        assertEquals("7", um.get("kafka-key"));
        assertEquals(ObjectReference.class.getName(), um.get("payload-class"));
        assertNotNull(um.get("payload-size-bytes"));
    }
}
