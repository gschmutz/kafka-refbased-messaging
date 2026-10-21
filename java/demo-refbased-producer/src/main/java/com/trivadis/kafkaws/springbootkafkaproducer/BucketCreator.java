package com.trivadis.kafkaws.springbootkafkaproducer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class BucketCreator {

    @Value(value = "${bucket.person.name}")
    private String personBucket;

    @Value(value = "${bucket.product.name}")
    private String productBucket;

    @Autowired
    private AmazonS3 amazonS3;

    @Bean
    public void createBucket() {
        if (!amazonS3.doesBucketExistV2(personBucket))
            amazonS3.createBucket(personBucket);

        if (!amazonS3.doesBucketExistV2(productBucket))
            amazonS3.createBucket(productBucket);
    }
}
