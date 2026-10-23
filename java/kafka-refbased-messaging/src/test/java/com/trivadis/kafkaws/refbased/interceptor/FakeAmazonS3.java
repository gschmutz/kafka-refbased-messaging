package com.trivadis.kafkaws.refbased.interceptor;

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal in-memory {@link com.amazonaws.services.s3.AmazonS3} test double. Extends
 * {@link AbstractAmazonS3} (which stubs the whole interface) and overrides only the few operations
 * the interceptors use, recording the calls so tests can assert on them.
 */
class FakeAmazonS3 extends AbstractAmazonS3 {

    boolean bucketExists = true;
    final List<String> createdBuckets = new ArrayList<>();

    int putCount;
    String putBucket;
    String putKey;
    ObjectMetadata putMetadata;

    int getCount;
    String getBucket;
    String getKey;
    S3Object getResponse;

    @Override
    public boolean doesBucketExistV2(String bucketName) {
        return bucketExists;
    }

    @Override
    public Bucket createBucket(String bucketName) {
        createdBuckets.add(bucketName);
        return new Bucket(bucketName);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) {
        this.putCount++;
        this.putBucket = bucketName;
        this.putKey = key;
        this.putMetadata = metadata;
        return new PutObjectResult();
    }

    @Override
    public S3Object getObject(String bucketName, String key) {
        this.getCount++;
        this.getBucket = bucketName;
        this.getKey = key;
        return getResponse;
    }
}
