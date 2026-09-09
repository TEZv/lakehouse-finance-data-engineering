import json
from pathlib import Path
import os
import pendulum
from airflow.models import DagBag
from event_pipeline import dag, output_dir

bag = DagBag(dag_folder=str(Path(__file__).parent / "event_pipeline.py"), include_examples=False)
assert not bag.import_errors, bag.import_errors
assert dag.get_task("ingest").downstream_task_ids == {"dbt_build_and_test"}

for day in ("2026-01-02", "2026-01-03"):
    result = dag.test(execution_date=pendulum.parse(day, tz="UTC"),
                      run_conf={"inject_first_failure": True})
    assert result.state == "success", result.state
    tasks = result.get_task_instances()
    ingest = next(t for t in tasks if t.task_id == "ingest")
    assert ingest.try_number >= 2, "Transient failure did not trigger a retry"
    summary = json.loads((output_dir(day) / "summary.json").read_text())
    assert summary == {"events": 2, "quantity": 16, "receipts": 6, "quarantined": 1}

print("PASS: DAG import, dependency, actual retry recovery, two isolated logical dates, dbt build")
