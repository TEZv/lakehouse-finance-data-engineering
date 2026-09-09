# 📨 Kafka delivery and replay lab

Independent synthetic-data extension. This is a real Kafka producer/consumer example with a small SQLite sink, not yet a Kafka-to-Spark connector or a production streaming platform.

## What it demonstrates

- Kafka message keys (`event_id`), a topic, consumer groups and explicit offset commits.
- At-least-once consumption with sink-level replay protection keyed by topic/partition/offset.
- Separate business deduplication/version handling: newer versions replace state; old versions do not.
- Invalid payloads and conflicting equal versions are retained in a quarantine receipt.
- A database transaction covers both state and receipt. Kafka offsets are committed afterwards.

The sink is committed before Kafka. A crash between those commits can cause redelivery; a receipt prevents the same offset being applied twice. This is NOT an atomic transaction across Kafka and SQLite, nor a claim of end-to-end exactly-once processing.

## Run without installation or administrator access

Open [Kafka replay lab Actions](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/workflows/kafka-ci.yml) and inspect the latest run. A workflow definition alone is not execution evidence; only a successful run is.

The sink unit tests need Python's standard library only:

```sh
python -m unittest discover -s labs/kafka -v
```

For a personal machine with Docker already available:

```sh
docker run -d --name portfolio-kafka -p 127.0.0.1:9092:9092 apache/kafka:3.9.1
python -m pip install -r labs/kafka/requirements.txt
```

Wait for the broker to start, then:

```sh
python labs/kafka/broker_demo.py
```

The demo creates a uniquely named topic and five synthetic messages, then consumes them twice using different groups against one sink. Expected final state: E1 version 2, quantity 12; five receipts; one quarantined receipt. The temporary SQLite file is removed on exit. Stop only this disposable lab container when finished; its topic data is not retained:

```sh
docker rm -f portfolio-kafka
```

## Limitations and interview questions

One broker, one partition, one sequential consumer; no HA, TLS/SASL, performance benchmark, schema registry, rebalance test, or distributed sink. Receipt retention and topic recreation need a production policy. The fixture quantity is an integer learning contract, not a general financial decimal model. Quarantine is a SQLite table, not a Kafka dead-letter topic. The replay test uses a fresh group, not a forced process crash. Unit tests separately exercise replay and rollback on a storage failure.

Explain before claiming proficiency:

1. Why does Kafka producer idempotence not remove a duplicate business event sent twice?
2. Why must storage complete before offset commit?
3. What happens when a version-1 message follows version 2?
4. What would change for multiple partitions and consumers?
5. Why would committing offsets before storing data risk loss?

References: [Apache Kafka Docker quickstart](https://kafka.apache.org/39/getting-started/docker/), [Confluent Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html).
