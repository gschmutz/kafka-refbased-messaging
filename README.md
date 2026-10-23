# Reference-Based Messaging with Apache Kafka

The maximum size of a Kafka message is 1 MB by default. This value can be increased (in Confluent
Cloud it is set to 8 MB), but as Kafka is optimized for smaller messages, setting it to a much
higher value is often not the right thing to do.

So when the payload (the value) of a Kafka message is larger than the maximum, we need another
solution. This project demonstrates **Reference-Based Messaging** with Kafka (a.k.a. the **Claim
Check** pattern): the message that travels through Kafka is just a small reference holding metadata
that points to the actual data, which is stored outside of Kafka in an object store (S3, MinIO,
Azure Data Lake Storage, ...).

```
                    large payload
   Producer  ────────────────────────►  Object store (S3 / MinIO)
      │                                        ▲
      │ small reference (bucket + object)      │ fetch on consume
      ▼                                        │
   Kafka topic  ─────────────────────►  Consumer
```

## Solutions in this repository

There are two independent ways to apply the pattern here:

| Solution | Modules | How it works |
|----------|---------|--------------|
| **Interceptor-based (transparent)** | [`kafka-refbased-messaging`](java/kafka-refbased-messaging) library + [`kafka-refbased-messaging-demo`](java/kafka-refbased-messaging-demo) | Kafka producer/consumer **interceptors** do the externalization automatically. Application code produces and consumes the normal payload and is unaware that large messages are offloaded to object storage. |
| **Manual producer + Apache NiFi** | [`demo-refbased-producer`](java/demo-refbased-producer) | The application **explicitly** writes the payload to the object store and produces a reference message itself. A NiFi flow consumes the reference and retrieves the object. |

The remainder of this README focuses on the interceptor-based solution. The manual/NiFi approach is
described at the end.

---

## The library: `kafka-refbased-messaging`

A small, framework-agnostic library (plain Kafka client, no Spring) that provides the two
interceptors and the reference message schema.

- **`ProducerInterceptorToStorage`** – for records whose serialized value exceeds a configurable
  size threshold, stores the Avro payload in the object store and replaces the record value with a
  lightweight `ObjectReference`. Smaller records are left **inline** on the topic unchanged. The
  reference is published on the **same topic** as the original record.
- **`ConsumerInterceptorToStorage`** – inspects every record: if the value is an `ObjectReference`
  it fetches and deserializes the original payload from storage; otherwise the record is passed
  through unchanged. The application therefore always receives the materialized payload.
- **`ObjectReference`** – the Avro reference record (generated from `src/main/avdl`):

  | Field | Meaning |
  |-------|---------|
  | `ts` | time the reference was created (epoch millis) |
  | `payloadType` | record name of the externalized payload (e.g. `Person`) |
  | `bucketName` | bucket holding the externalized payload |
  | `objectName` | object key within the bucket |

### Same topic, two schemas

Because the reference is published on the original topic, the `ObjectReference` schema and the
original payload schema coexist on one topic. This requires a **record-aware subject naming
strategy** on the value serializer, so each record type is registered under its own Schema Registry
subject:

```
value.subject.name.strategy: io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
```

### Configuration

All settings are read from the normal Kafka producer/consumer configuration (every key has a
default, so the interceptors work out of the box):

| Key | Used by | Default | Description |
|-----|---------|---------|-------------|
| `interceptor.storage.size.threshold.bytes` | producer | `1048576` (1 MiB) | only payloads **larger** than this are externalized |
| `interceptor.storage.bucket.name` | producer | `large-object-bucket` | target bucket for stored objects; may contain the `${topic}` placeholder |
| `interceptor.storage.object.prefix` | producer | _(empty)_ | optional key prefix ("folder") the objects are stored under; may contain `${topic}` |
| `interceptor.storage.topic.transform.regex` | producer | _(empty)_ | optional regex applied to the topic name before it fills `${topic}` |
| `interceptor.storage.topic.transform.replacement` | producer | _(empty)_ | replacement for the regex (standard `replaceAll` semantics, e.g. `$1`) |
| `interceptor.storage.partition.granularity` | producer | `none` | time-based partitioning: `none`, `monthly`, `daily` or `hourly` |
| `interceptor.storage.service.endpoint` | both | `http://dataplatform:9005` | S3 / MinIO endpoint URL |
| `interceptor.storage.region` | both | `us-east-2` | signing region |
| `interceptor.storage.access.key` | both | `admin` | object-store access key |
| `interceptor.storage.secret.key` | both | `abc123abc123!` | object-store secret key |

#### Topic placeholder

The bucket name and the object prefix may contain the `${topic}` placeholder, which is resolved
**per record** to the topic the record is produced to — so you can route different topics to
different buckets or folders with a single configuration, e.g. `refbased-${topic}` or a prefix of
`${topic}/data`. (The reference itself is still published on the original topic; the placeholder only
affects storage naming.)

Because S3 bucket names are restricted (lowercase, no `.`/`_`, …), an optional regex can transform
the topic name before it fills the placeholder:

```yaml
interceptor.storage.bucket.name: refbased-${topic}
interceptor.storage.topic.transform.regex: "[._]"   # replace dots and underscores
interceptor.storage.topic.transform.replacement: "-"
# topic "my.Person_topic"  ->  bucket "refbased-my-Person-topic"
```

#### Object key layout & retention

The key under which each payload is stored is composed as:

```
<prefix>/<time-partition>/<uuid>
```

Both the prefix and the time partition are optional. The time partition is a **UTC** date path
derived from the configured granularity:

| Granularity | Path | Example key (prefix `person`) |
|-------------|------|-------------------------------|
| `none` | — | `person/3f2b…` |
| `monthly` | `yyyy/MM` | `person/2026/10/3f2b…` |
| `daily` | `yyyy/MM/dd` | `person/2026/10/23/3f2b…` |
| `hourly` | `yyyy/MM/dd/HH` | `person/2026/10/23/14/3f2b…` |

Partitioning by time lets you handle **data retention asynchronously**: configure an object-store
lifecycle / expiration rule on the date prefix (e.g. delete everything under `person/2026/09/…`
after 90 days) instead of deleting individual objects. The full key is always recorded in the
`ObjectReference`, so the consumer retrieves objects regardless of the layout.

#### Stored object metadata

Each externalized object carries the Kafka context available at produce time as S3 **user metadata**
(`x-amz-meta-*`):

| Metadata key | Notes |
|--------------|-------|
| `kafka-topic` | always present |
| `kafka-key` | present when the record has a key |
| `payload-class` | fully-qualified class of the externalized payload |
| `payload-size-bytes` | size of the stored Avro payload |
| `kafka-partition` | only if the producer set it explicitly |
| `kafka-timestamp` | only if the producer set it explicitly |

> The **offset is not stored**: a producer interceptor runs *before* the record is appended to the
> log, so no offset exists yet. For the same reason `partition` and `timestamp` are only present if
> the application set them explicitly — otherwise the broker assigns them after the send.

### Using it in your own application

1. Add the dependency:

   ```xml
   <dependency>
       <groupId>com.trivadis.kafkaws</groupId>
       <artifactId>kafka-refbased-messaging</artifactId>
       <version>1.1-SNAPSHOT</version>
   </dependency>
   ```

2. Register the interceptors and set the subject naming strategy on the producer, and register the
   consumer interceptor (see the demo [`application.yml`](java/kafka-refbased-messaging-demo/src/main/resources/application.yml)
   for a complete example).

> **Note:** the payload is stored as raw Avro binary and the consumer resolves the payload class
> from `payloadType`, so the payload classes must be on the consumer's classpath. In the current
> code the producer sets `payloadType` to `"Person"`, so as written it round-trips `Person`
> payloads.

### Build & install the library

```bash
cd ./java/kafka-refbased-messaging
mvn clean install -Dmaven.test.skip=true
```

---

## The demo: `kafka-refbased-messaging-demo`

A runnable Spring Boot application that exercises the interceptor-based solution end to end. It
both produces and consumes on the `person` topic.

- The **producer** generates `Person` records. To show both paths, roughly every third record is
  padded so it exceeds the size threshold and is **externalized to S3**, while the others stay
  **inline** on the topic. Each record is logged as `LARGE` or `small`.
- The **consumer** (`@KafkaListener`) logs the received records. Thanks to the consumer interceptor,
  it always sees a fully materialized `Person`, whether it came inline or from storage.

The interceptor configuration, the `TopicRecordNameStrategy`, and the size threshold live in
[`application.yml`](java/kafka-refbased-messaging-demo/src/main/resources/application.yml).

### Prerequisites

A running data platform providing **Kafka**, **Confluent Schema Registry**, and an **S3-compatible
object store** (MinIO). Point `DATAPLATFORM_IP` at its host; the demo expects Kafka on `9092/9093`,
Schema Registry on `8081`, and MinIO on `9005`.

```bash
export DATAPLATFORM_IP=<your-dataplatform-ip>
```

### Run it

The demo uses `auto.register.schemas: false`, so the Avro schemas must be registered first (this
also requires the library to be built, since the `ObjectReference` schema lives there).

```bash
export DATAPLATFORM_IP=<your-dataplatform-ip>

# 1. build & install the library (generates the ObjectReference schema)
cd ./java/kafka-refbased-messaging
mvn clean install -Dmaven.test.skip=true

# 2. build the demo and register its Avro subjects
cd ../kafka-refbased-messaging-demo
mvn clean package -Dmaven.test.skip=true
mvn schema-registry:register

# 3. run the demo (produces 100 records and consumes them)
mvn spring-boot:run
```

You can override the run parameters (`<messageCount> <waitMsBetween> <key>`), e.g.:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="50 100 0"
```

Watch the log: `LARGE` records are stored in the `person-ref-bucket` of the object store and sent as
an `ObjectReference`, while `small` records travel inline — and the consumer materializes both back
into `Person`.

---

## Alternative: manual producer + Apache NiFi (`demo-refbased-producer`)

This approach does not use interceptors: the application writes the payload to the object store and
produces the reference message itself, and a NiFi flow retrieves the object on the consuming side.

### Prepare the Avro schemas

Build the `kafka-refbased-messaging` library first (it owns the `ObjectReference` schema), then
build the producer and register the subjects:

```bash
export DATAPLATFORM_IP=<your-dataplatform-ip>

cd ./java/kafka-refbased-messaging
mvn clean install -Dmaven.test.skip=true

cd ../demo-refbased-producer
mvn clean package -Dmaven.test.skip=true
mvn schema-registry:register
```

### NiFi data flow

A data flow in NiFi receives a message from Kafka, retrieves the referenced object from S3, and then
buffers the object for writing back to S3.

Import the NiFi template `/nifi/Generic_Pipeline_Process_Archive.xml` into NiFi and use it to create
a new flow. Enable all services, then start all processors.

### Run the simulator

The simulator creates Kafka messages with a reference to the "large" object stored in an S3 bucket.

```bash
export DATAPLATFORM_IP=<your-dataplatform-ip>

cd ./java/demo-refbased-producer
mvn package -Dmaven.test.skip=true
mvn spring-boot:run
```

---

## Modules

| Module | Description |
|--------|-------------|
| [`java/kafka-refbased-messaging`](java/kafka-refbased-messaging) | Reusable interceptor library + `ObjectReference` schema |
| [`java/kafka-refbased-messaging-demo`](java/kafka-refbased-messaging-demo) | Spring Boot demo of the interceptor-based solution |
| [`java/demo-refbased-producer`](java/demo-refbased-producer) | Manual reference producer used with the NiFi flow |
