package com.trivadis.kafkaws.springbootkafkainterceptor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.github.javafaker.Faker;
import com.trivadis.demo.dto.ObjectReference;
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

	private void runProducer(int sendMessageCount, int waitMsInBetween, long id) throws Exception {
		Long key = (id > 0) ? id : null;

		Faker faker = new Faker();

		for (int index = 0; index < sendMessageCount; index++) {

			UUID personId = UUID.randomUUID();

			// make roughly every third message large enough to exceed the configured size
			// threshold, so the producer interceptor externalizes it to S3; the others stay
			// small and are sent inline on the topic
			boolean large = (index % 3 == 0);
			String street = faker.address().streetName();
			if (large) {
				street = street + " " + "X".repeat(2048);
			}

			Person person = Person.newBuilder().setId(personId.toString()).setFirstName(faker.name().firstName())
					.setMiddleName(faker.name().firstName())
					.setLastName(faker.name().lastName())
					.setStreet(street)
					.setNumber(faker.address().streetAddressNumber())
					.setCity(faker.address().cityName())
					.setZipCode(faker.address().zipCode()).build();

			LOG.info("[{}] producing {} person (street length={})", index, large ? "LARGE" : "small", street.length());
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
