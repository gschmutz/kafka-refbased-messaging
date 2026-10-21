# Demo project on concept of Reference-based Messaging with Apache Kafka

The limit of a Kafka message is 1 MB by default. This value can be increased (in Confluent Cloud it is set to 8 MB), but as Kafka is optimized for smaller messages, setting it to much higher value might not be the right thing to do. 

Therefore if the payload (the value) of a Kafka message is larger than the maximum, we need another solution. In this project we show various ways of refrenced-based messaging with Kafka (a.k.a. Claim Check Pattern), where the message in Kafka is just an event with Metadata, pointing (refrencing) the data which is stored outside of Kafka, for example in an object store (S3 or Azure Data Lake Storage, ...). 

## Using NiFi to manually work with Reference-based messages

### Prepare Avro Schemas

First build and register the necessary Avro Schemas

```bash
export DATAPLATFORM_IP=80.209.234.152

cd ./java/demo-meta
mvn clean package -Dmaven.test.skip=true

mvn schema-registry:register 
```

### Nifi Data Flow

A Data flow in Nifi receives a message from Kafka, retrieves the object via reference from S3 and then buffers the object for writing to S3.

Import the NiFi Template `/nifi/Generic_Pipeline_Process_Archive.xml` into Nifi and use it to create a new flow. 

Enable all services and then start all processors.


Start the simulator, which creates a Kafka message with a reference to the "large" object which is stored in an S3 bucket. 

```bash
export DATAPLATFORM_IP=80.209.234.152

cd ./java/demo-refbased-producer
mvn package -Dmaven.test.skip=true
mvn spring-boot:run
```

