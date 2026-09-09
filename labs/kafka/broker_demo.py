"""Bounded integration scenario against a real, local Kafka broker."""
import json
import tempfile
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from event_sink import connect, persist


def consume(topic, db, count):
    client = Consumer({
        "bootstrap.servers": "localhost:9092", "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    client.subscribe([topic])
    seen, deadline = 0, time.monotonic() + 60
    try:
        while seen < count and time.monotonic() < deadline:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            persist(db, message.topic(), message.partition(), message.offset(), message.value())
            # If the process dies here, Kafka can redeliver; the sink receipt makes replay safe.
            client.commit(message=message, asynchronous=False)
            seen += 1
    finally:
        client.close()
    assert seen == count, f"Expected {count} messages, got {seen}"


def main():
    topic = "portfolio-events-" + uuid.uuid4().hex
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    futures[topic].result(timeout=30)
    producer = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True})
    errors = []
    def delivered(error, message):
        if error:
            errors.append(str(error))
    events = [
        {"event_id": "E1", "version": 1, "quantity": 10},
        {"event_id": "E1", "version": 1, "quantity": 10},
        {"event_id": "E1", "version": 2, "quantity": 12},
        {"event_id": "E1", "version": 1, "quantity": 10},
        {"event_id": "E2", "version": 1, "quantity": -1},
    ]
    for event in events:
        producer.produce(topic, key=event["event_id"], value=json.dumps(event), on_delivery=delivered)
    assert producer.flush(30) == 0, "Undelivered messages"
    assert not errors, errors
    with tempfile.TemporaryDirectory(prefix="kafka-portfolio-") as directory:
        db = connect(Path(directory) / "sink.sqlite")
        try:
            consume(topic, db, len(events))
            # A fresh group deliberately replays every offset into the SAME durable sink.
            consume(topic, db, len(events))
            assert db.execute("SELECT * FROM events").fetchall() == [("E1", 2, 12)]
            assert db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 5
            assert db.execute("SELECT COUNT(*) FROM receipts WHERE outcome='quarantined'").fetchone()[0] == 1
            print("PASS: broker delivery, correction, stale version, quarantine, full replay")
        finally:
            db.close()


if __name__ == "__main__":
    main()
