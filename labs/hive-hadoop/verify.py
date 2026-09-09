"""Compare Hive rows with the shared Python batch output and test external-table retention."""
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]


def query(sql):
    return subprocess.check_output([
        "docker", "exec", "portfolio-hive", "beeline", "-u", "jdbc:hive2://localhost:10000/",
        "-n", "hive", "--silent=true", "--showHeader=false", "--outputformat=tsv2", "-e", sql
    ], text=True)


deadline = time.monotonic() + 180
while True:
    try:
        query("SELECT 1;")
        break
    except subprocess.CalledProcessError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(3)
setup = (ROOT / "labs/hive-hadoop/setup.sql").read_text()
for _ in range(2):
    query(setup)
    result = query("SELECT event_id, version, quantity FROM portfolio.event_snapshot "
                   "WHERE batch_date='2026-01-02';")
    rows = sorted(line.strip() for line in result.splitlines() if line.startswith(("E1\t", "E2\t")))
    assert rows == ["E1\t2\t12", "E2\t1\t4"], repr(result)
    assert sum(int(row.split("\t")[2]) for row in rows) == 16
    partitions = query("SHOW PARTITIONS portfolio.event_snapshot;")
    assert "batch_date=2026-01-02" in partitions

query("DROP TABLE portfolio.event_snapshot;")
subprocess.run(["docker", "exec", "portfolio-hive", "hdfs", "dfs", "-test", "-e",
                "/portfolio/events/batch_date=2026-01-02/events.tsv"], check=True)
query(setup)
assert "E1\t2\t12" in query("SELECT event_id, version, quantity FROM portfolio.event_snapshot;")
print("PASS: HDFS NameNode/DataNode, Hive external partition, rerun, rows, retained external data")
