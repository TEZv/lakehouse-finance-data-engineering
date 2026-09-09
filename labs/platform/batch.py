"""Bounded batch adapter for the existing replay-safe Kafka learning sink."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "kafka"))
from event_sink import connect, persist


def run(source, output):
    source, output = Path(source), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    raw = source.read_bytes()
    records = json.loads(raw)
    if not isinstance(records, list):
        raise ValueError("Expected a JSON array")
    # Content identity is used for batch replay, not a simulated Kafka offset.
    batch_id = "batch:" + hashlib.sha256(raw).hexdigest()
    db = connect(output / "state.sqlite")
    try:
        for offset, event in enumerate(records):
            persist(db, batch_id, 0, offset, json.dumps(event).encode())
        states = [dict(zip(("event_id", "version", "quantity"), row)) for row in
                  db.execute("SELECT event_id, version, quantity FROM events ORDER BY event_id")]
        counts = dict(db.execute("SELECT outcome, COUNT(*) FROM receipts GROUP BY outcome"))
        # Atomic replacement keeps consumers from reading a partially written export.
        pending = output / "events.pending.json"
        pending.write_text(json.dumps(states, indent=2), encoding="utf-8")
        pending.replace(output / "events.json")
        result = {"events": len(states), "quantity": sum(x["quantity"] for x in states),
                  "receipts": sum(counts.values()), "quarantined": counts.get("quarantined", 0)}
        (output / "summary.json").write_text(json.dumps(result), encoding="utf-8")
        return result
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.source, args.output), sort_keys=True))
