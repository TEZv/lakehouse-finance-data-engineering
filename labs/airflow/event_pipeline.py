"""Local Airflow 2.10 compatibility lab; not a scheduler/service deployment."""
from datetime import timedelta
import os
from pathlib import Path
import subprocess
import sys

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "labs/platform"))
from batch import run


def output_dir(ds):
    return Path(os.environ["PLATFORM_OUTPUT_ROOT"]) / ds


def ingest(ds, ti, params, **context):
    if params.get("inject_first_failure") and ti.try_number == 1:
        raise AirflowException("Intentional transient failure before ingestion")
    result = run(ROOT / "labs/platform/events.json", output_dir(ds))
    assert result == {"events": 2, "quantity": 16, "receipts": 6, "quarantined": 1}
    return result


def transform(ds, **context):
    output = output_dir(ds)
    env = {**os.environ, "DBT_INPUT_PATH": str(output / "events.json"),
           "DBT_DATABASE_PATH": str(output / "analytics.duckdb")}
    subprocess.run([os.environ["DBT_EXECUTABLE"], "build", "--project-dir", str(ROOT / "labs/dbt"),
                    "--profiles-dir", str(ROOT / "labs/dbt")], check=True, env=env)


with DAG(
    "portfolio_event_pipeline", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily", catchup=False, max_active_runs=1,
    params={"inject_first_failure": False},
    default_args={"retries": 1, "retry_delay": timedelta(seconds=1)},
    tags=["portfolio", "synthetic"],
) as dag:
    ingest_task = PythonOperator(task_id="ingest", python_callable=ingest)
    dbt_task = PythonOperator(task_id="dbt_build_and_test", python_callable=transform)
    ingest_task >> dbt_task
