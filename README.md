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

## Two strategies for carrying the reference

Once the payload is in the object store, the small reference that still travels through Kafka can be
carried in **one of two ways**. This repository implements **both**, as two matched interceptor pairs
that share the same object-store logic (size threshold, bucket/prefix/partitioning, stored-object
metadata) and the same `interceptor.storage.*` configuration — they differ only in **how the
reference is represented on the topic**:

| | **1 · Value reference (placeholder message)** | **2 · Header reference** |
|--|----------------------------------------------|--------------------------|
| Reference travels as | the Kafka **record value** — an Avro `ClaimCheck` placeholder | Kafka **record headers** |
| Record value on the topic | the `ClaimCheck` (non-null) | `null` |
| Payload formats | Avro | **any** (Avro, JSON, String, `byte[]`, …) — serializer-agnostic |
| Subject naming strategy | **`TopicRecordNameStrategy`** (so the reference and payload schemas can coexist on the topic) | not applicable |
| Extra schema on the topic | yes — the `ClaimCheck` schema alongside the payload schema | none |
| Safe on compacted topics | yes | **no** — a `null` value is a tombstone |
| Interceptors | `ProducerInterceptorToStorage` / `ConsumerInterceptorToStorage` | `ProducerInterceptorToStorageHeaders` / `ConsumerInterceptorToStorageHeaders` |

```
 1 · value reference                         2 · header reference
   key | value=ClaimCheck{bucket,obj}     key | value=null
       |                                            | headers: refbased.reference=s3://bucket/key, …
```

In short: the **value-reference** strategy keeps a real, non-null, schema'd message on the topic (so
it is visible to any consumer and safe on compacted topics), at the cost of a second schema coexisting
on the topic and being tied to Avro. The **header-reference** strategy keeps the reference out of the
value entirely, which makes it work with any serializer and needs no extra schema — but the `null`
value rules out compacted topics.

Both strategies are provided by the interceptor library and are detailed below. The externalization
is done entirely by Kafka producer/consumer **interceptors**: application code produces and consumes
the normal payload and is unaware that large messages are offloaded to object storage.

---

## The library: `kafka-refbased-messaging`

A small, framework-agnostic library (plain Kafka client, no Spring) that provides **both** reference
strategies as two matched interceptor pairs, plus the Avro reference schema. Pick the pair that fits
your topic; both are configured through the same `interceptor.storage.*` keys (see
[Configuration](#configuration)).

### Strategy 1 · value reference (Avro placeholder)

The reference travels as the Kafka **record value**, an Avro `ClaimCheck` placeholder.

- **`ProducerInterceptorToStorage`** – for records whose serialized value exceeds a configurable
  size threshold, stores the Avro payload in the object store and replaces the record value with a
  lightweight `ClaimCheck`. Smaller records are left **inline** on the topic unchanged. The
  reference is published on the **same topic** as the original record.
- **`ConsumerInterceptorToStorage`** – inspects every record: if the value is an `ClaimCheck`
  it fetches and deserializes the original payload from storage; otherwise the record is passed
  through unchanged. The application therefore always receives the materialized payload.
- **`ClaimCheck`** – the Avro reference record (generated from `src/main/avdl`):

  | Field | Meaning |
  |-------|---------|
  | `ts` | time the reference was created (epoch millis) |
  | `payloadType` | record name of the externalized payload (e.g. `Person`) |
  | `payloadReference` | URI of the externalized payload, encoding bucket + object key (e.g. `s3://bucket/key`) |

**Same topic, two schemas.** Because the reference is published on the original topic, the
`ClaimCheck` schema and the original payload schema coexist on one topic. This requires a
**record-aware subject naming strategy** on the value serializer, so each record type is registered
under its own Schema Registry subject:

```
value.subject.name.strategy: io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
```

Use this strategy when consumers (or tooling) should still see a real, inspectable message for every
record, or when the topic is **log-compacted**.

### Strategy 2 · header reference (serializer-agnostic)

The reference travels in Kafka **record headers** and the record value is sent as `null`. The payload
is treated as **opaque**, so this pair works for **any** value type (Avro, JSON, String, `byte[]`, …).

- **`ProducerInterceptorToStorageHeaders`** – for records over the size threshold, it serializes the
  value with the producer's **configured `value.serializer`**, stores those bytes in the object
  store, then publishes the record with a **`null` value** and the reference in headers. Smaller
  records are left inline unchanged.
- **`ConsumerInterceptorToStorageHeaders`** – if a record carries the reference headers, it fetches
  the stored bytes and reconstructs the value with the consumer's **configured `value.deserializer`**;
  otherwise the record passes through unchanged.

Because the bytes are produced and consumed by the serializers the application is already configured
with, the interceptors contain no format-specific code. The reference is carried in these headers:

| Header | Meaning |
|--------|---------|
| `refbased.reference` | storage reference URI, e.g. `s3://bucket/key` (its presence marks a record as externalized) |
| `refbased.payloadType` | payload's Java class at produce time (informational only) |
| `refbased.ts` | time the reference was created (epoch millis) |

This strategy needs **no `ClaimCheck` schema, no Schema Registry subject strategy, and no second
schema on the topic** — the value on the wire is simply `null`. On the consumer, reconstruction goes
through `value.deserializer`, so for formats without type headers (e.g. JSON) pin a default type
(`spring.json.value.default.type`).

> **Caveat:** a `null` value is the log-compaction **tombstone** marker, so this strategy must not be
> used on a **compacted** topic (every externalized record would mark its key for deletion). On
> normal delete/time-retention topics it is safe.

The [`kafka-refbased-messaging-json-demo`](java/kafka-refbased-messaging-json-demo) module uses this
pair with JSON payloads.

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
`ClaimCheck`, so the consumer retrieves objects regardless of the layout.

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

### Monitoring

The interceptors of **both strategies** expose their behaviour as **JMX MBeans** (no extra
dependencies), registered per Kafka `client.id` under the same MBean names (a given producer/consumer
uses one strategy):

| MBean | Attributes |
|-------|------------|
| `com.trivadis.kafka.claimcheck:type=ProducerInterceptor,client-id=<id>` | `RecordsExternalized`, `RecordsInline`, `BytesExternalized`, `ExternalizationErrors` |
| `com.trivadis.kafka.claimcheck:type=ConsumerInterceptor,client-id=<id>` | `RecordsMaterialized`, `RecordsPassedThrough`, `BytesFetched`, `FetchErrors` |

You can inspect them with JConsole / VisualVM, or scrape them into Prometheus + Grafana via the
platform's JMX exporter — enable `jmx_monitoring_with_prometheus_enable` (and `PROMETHEUS_enable`
/ `GRAFANA_enable`) in [docker/config.yml](docker/config.yml). Registration is best-effort: a JMX
failure never interrupts message flow.

### Using it in your own application

1. Add the dependency:

   ```xml
   <dependency>
       <groupId>com.trivadis.kafkaws</groupId>
       <artifactId>kafka-refbased-messaging</artifactId>
       <version>1.1-SNAPSHOT</version>
   </dependency>
   ```

2. Register the matching producer/consumer interceptors for your chosen strategy via
   `interceptor.classes`:
   - **Strategy 1 (value reference):** `ProducerInterceptorToStorage` / `ConsumerInterceptorToStorage`,
     and set `value.subject.name.strategy: …TopicRecordNameStrategy` on the producer. See the demo
     [`application.yml`](java/kafka-refbased-messaging-demo/src/main/resources/application.yml).
   - **Strategy 2 (header reference):** `ProducerInterceptorToStorageHeaders` /
     `ConsumerInterceptorToStorageHeaders`, with whatever `value.serializer` / `value.deserializer`
     your payload uses. See the JSON demo
     [`application.yml`](java/kafka-refbased-messaging-json-demo/src/main/resources/application.yml).

> **Note (strategy 1):** the payload is stored as raw Avro binary and the consumer resolves the
> payload class from `payloadType`, so the payload classes must be on the consumer's classpath. In
> the current code the producer sets `payloadType` to `"Person"`, so as written it round-trips
> `Person` payloads. Strategy 2 instead reconstructs the value via the configured
> `value.deserializer`, so it carries no such Avro assumption.

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
also requires the library to be built, since the `ClaimCheck` schema lives there).

```bash
export DATAPLATFORM_IP=<your-dataplatform-ip>

# 1. build & install the library (generates the ClaimCheck schema)
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
an `ClaimCheck`, while `small` records travel inline — and the consumer materializes both back
into `Person`.

---

## The JSON demo: `kafka-refbased-messaging-json-demo`

A second runnable Spring Boot demo that exercises the **header-based, serializer-agnostic**
interceptors with **JSON** payloads instead of Avro. It is a copy of the demo above with three
differences:

- the payload is a plain POJO (`Person`) serialized with Spring Kafka's `JsonSerializer` /
  `JsonDeserializer` — **no Avro, no Schema Registry**;
- it wires `ProducerInterceptorToStorageHeaders` / `ConsumerInterceptorToStorageHeaders`, so the
  reference travels in headers and large records are sent with a `null` value;
- the consumer pins the target type via `spring.json.value.default.type`, since the stored JSON
  carries no type headers.

It produces and consumes on the `person-json` topic; as before, roughly every third (padded) record
exceeds the threshold and is externalized to the `refbased-json-bucket`, while the rest stay inline.

### Prerequisites

A running data platform providing **Kafka** and an **S3-compatible object store** (MinIO). Unlike the
Avro demo, **no Schema Registry is needed**. The demo expects Kafka on `9092/9093` and MinIO on
`9005`.

### Run it

```bash
export DATAPLATFORM_IP=<your-dataplatform-ip>

# 1. build & install the library (provides the interceptors)
cd ./java/kafka-refbased-messaging
mvn clean install -Dmaven.test.skip=true

# 2. run the JSON demo (no schema registration step required)
cd ../kafka-refbased-messaging-json-demo
mvn spring-boot:run
```

As with the Avro demo you can override `<messageCount> <waitMsBetween> <key>` via
`-Dspring-boot.run.arguments="50 100 0"`.


---

## Modules

| Module | Description |
|--------|-------------|
| [`java/kafka-refbased-messaging`](java/kafka-refbased-messaging) | Reusable interceptor library (Avro value-reference + serializer-agnostic header variants) + `ClaimCheck` schema |
| [`java/kafka-refbased-messaging-demo`](java/kafka-refbased-messaging-demo) | Spring Boot demo of the interceptor-based solution (Avro value-reference) |
| [`java/kafka-refbased-messaging-json-demo`](java/kafka-refbased-messaging-json-demo) | Spring Boot demo of the header-based interceptors with JSON payloads |
