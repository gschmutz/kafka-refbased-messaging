package com.trivadis.kafkaws.springbootkafkainterceptor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.github.javafaker.Faker;
import com.trivadis.demo.dto.BusinessEvent;
import com.trivadis.demo.dto.Person;
import com.trivadis.demo.dto.Product;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.time.Instant;
import java.util.UUID;

@SpringBootApplication
public class SpringBootKafkaProducerApplication implements CommandLineRunner {

	private static Logger LOG = LoggerFactory.getLogger(SpringBootKafkaProducerApplication.class);

	@Autowired
	private KafkaEventProducer kafkaEventProducer;

	@Autowired
	private AmazonS3 amazonS3;

	@Value(value = "${bucket.person.name}")
	private String personBucket;

	@Value(value = "${bucket.product.name}")
	private String productBucket;

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaProducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("EXECUTING : command line runner");

		if (args.length == 0) {
			runProducer(100, 10, 0);
		} else {
			runProducer(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Long.parseLong(args[2]));
		}

	}

	private S3Object writeToPersonS3(Person person) throws java.io.IOException {
		byte[] bytes = AvroUtil.serialize(person);
		ByteArrayInputStream stream = new ByteArrayInputStream(bytes);

		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(bytes.length);
		meta.setContentType("application/avro-binary");

		String objectName = String.valueOf(person.getId()) + ".avro";

		S3Object s3obj = new S3Object(personBucket, objectName);

		PutObjectResult result = amazonS3.putObject(personBucket, objectName, stream, meta);
		URL s3Url = amazonS3.getUrl(personBucket, objectName);

		return s3obj;
	}

	private S3Object writeToProductS3(Product product) throws java.io.IOException {
		byte[] bytes = AvroUtil.serialize(product);
		ByteArrayInputStream stream = new ByteArrayInputStream(bytes);

		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(bytes.length);
		meta.setContentType("application/avro-binary");

		String objectName = String.valueOf(product.getId()) + ".avro";

		S3Object s3obj = new S3Object(productBucket, objectName);

		PutObjectResult result = amazonS3.putObject(productBucket, objectName, stream, meta);
		URL s3Url = amazonS3.getUrl(productBucket, objectName);

		return s3obj;
	}

	private void runProducer(int sendMessageCount, int waitMsInBetween, long id) throws Exception {
		Long key = (id > 0) ? id : null;

		Faker faker = new Faker();

		for (int index = 0; index < sendMessageCount; index++) {

			UUID personId = UUID.randomUUID();
			Person person = Person.newBuilder().setId(personId.toString()).setFirstName(faker.name().firstName())
					.setMiddleName(faker.name().firstName())
					.setLastName(faker.name().lastName())
					.setStreet(faker.address().streetName())
					.setNumber(faker.address().streetAddressNumber())
					.setCity(faker.address().cityName())
					.setZipCode(faker.address().zipCode()).build();

			kafkaEventProducer.produce(index, key, person);

			// Simulate slow processing
			Thread.sleep(waitMsInBetween);

		}
	}
}


class S3Object {
	public String bucketName;
	public String objectName;

	public S3Object(String bucketName, String objectName) {
		this.bucketName = bucketName;
		this.objectName = objectName;
	}
}
