"""Real dbt build + no-op rerun + per-event late version/stale replay assertions."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "labs/platform"))
from batch import run


def build(env):
    subprocess.run(["dbt", "build", "--project-dir", str(ROOT / "labs/dbt"),
                    "--profiles-dir", str(ROOT / "labs/dbt")], env=env, check=True)


def check(database, expected):
    with duckdb.connect(str(database), read_only=True) as db:
        assert db.execute("select * from mart_event_summary").fetchone() == expected


def main():
    with tempfile.TemporaryDirectory(prefix="dbt-proof-") as directory:
        output = Path(directory)
        run(ROOT / "labs/platform/events.json", output)
        source, database = output / "events.json", output / "analytics.duckdb"
        env = {**os.environ, "DBT_INPUT_PATH": str(source), "DBT_DATABASE_PATH": str(database)}
        build(env)
        check(database, (2, 16))
        build(env)
        check(database, (2, 16))
        # A late correction is selected by event version, not load time.
        source.write_text(json.dumps([
            {"event_id": "E1", "version": 3, "quantity": 15},
            {"event_id": "E2", "version": 1, "quantity": 4},
            {"event_id": "E4", "version": 1, "quantity": 2},
        ]), encoding="utf-8")
        build(env)
        check(database, (3, 21))
        rows = json.loads(source.read_text())
        rows[0] = {"event_id": "E1", "version": 1, "quantity": 10}
        source.write_text(json.dumps(rows), encoding="utf-8")
        build(env)
        check(database, (3, 21))
        subprocess.run(["dbt", "docs", "generate", "--project-dir", str(ROOT / "labs/dbt"),
                        "--profiles-dir", str(ROOT / "labs/dbt")], env=env, check=True)
        print("PASS: dbt build, tests, rerun, late correction, stale replay, docs")


if __name__ == "__main__":
    main()
