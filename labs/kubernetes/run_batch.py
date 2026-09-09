import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "platform"))
from batch import run

if os.environ.get("INJECT_FAILURE") == "1":
    raise RuntimeError("Intentional failure: verify Job diagnostics, not success")
source = Path(__file__).resolve().parents[1] / "platform/events.json"
first = run(source, "/work/output")
second = run(source, "/work/output")
assert first == second == {"events": 2, "quantity": 16, "receipts": 6, "quarantined": 1}
print("PASS: Kubernetes batch and in-Pod replay " + json.dumps(second, sort_keys=True))
