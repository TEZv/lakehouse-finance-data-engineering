"""Export the shared synthetic batch as tab-delimited rows for Hive."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "labs/platform"))
from batch import run

output = Path(sys.argv[1]).resolve()
run(ROOT / "labs/platform/events.json", output)
records = json.loads((output / "events.json").read_text())
(output / "events.tsv").write_text("".join(
    f"{r['event_id']}\t{r['version']}\t{r['quantity']}\n" for r in records
), encoding="utf-8")
