"""Single-consumer learning sink. SQLite commit must precede Kafka offset commit."""
import json
import sqlite3


def connect(path):
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY, version INTEGER NOT NULL, quantity INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS receipts (
            topic TEXT, partition_id INTEGER, offset_id INTEGER,
            outcome TEXT NOT NULL, raw BLOB NOT NULL, reason TEXT,
            PRIMARY KEY(topic, partition_id, offset_id)
        );
    """)
    return db


def parse(raw):
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("Expected an object")
    if not isinstance(event.get("event_id"), str) or not event["event_id"].strip():
        raise ValueError("event_id must be nonempty text")
    for field in ("version", "quantity"):
        if type(event.get(field)) is not int or event[field] <= 0:
            raise ValueError(f"{field} must be a positive integer")
    return event


def persist(db, topic, partition, offset, raw):
    """Atomically store a receipt and latest state; replay is harmless in this sink."""
    key = (topic, partition, offset)
    with db:
        prior = db.execute(
            "SELECT outcome FROM receipts WHERE topic=? AND partition_id=? AND offset_id=?", key
        ).fetchone()
        if prior:
            return prior[0]
        reason = None
        try:
            event = parse(raw)
            prior_event = db.execute(
                "SELECT version, quantity FROM events WHERE event_id=?", (event["event_id"],)
            ).fetchone()
            if prior_event and prior_event[0] == event["version"] and prior_event[1] != event["quantity"]:
                raise ValueError("Conflicting payload for the same event version")
            outcome = "applied" if not prior_event or event["version"] > prior_event[0] else "ignored"
            db.execute("""
                INSERT INTO events VALUES (?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET version=excluded.version, quantity=excluded.quantity
                WHERE excluded.version > events.version
            """, (event["event_id"], event["version"], event["quantity"]))
        except (ValueError, UnicodeError) as exc:
            outcome, reason = "quarantined", str(exc)
        db.execute("INSERT INTO receipts VALUES (?, ?, ?, ?, ?, ?)", (*key, outcome, raw, reason))
    return outcome
