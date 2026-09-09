# ⏱️ Airflow batch orchestration

The DAG runs the shared Python batch and then a real dbt build/test. The dependency prevents analytics from running before ingestion succeeds. `ds` selects an isolated output directory; retries use the same directory and the sink handles batch replay. Input is a fixed synthetic fixture, not a date-filtered business feed.

## Verified scenario

`verify.py` checks DAG imports/dependencies, executes two logical dates through `dag.test()`, injects a first-attempt ingestion failure, and requires successful retry before dbt runs. The CI console contains task/retry logs; the evidence artifact contains output summaries. The test does not prove scheduler operation, automatic historical backfill, concurrent workers, or failure recovery of a whole Airflow service.

## Run on Linux / CI

Use Python 3.11 and two separate environments to avoid Airflow/dbt dependency conflicts. Exact executable commands are maintained in the `airflow` job of [platform-ci.yml](../../.github/workflows/platform-ci.yml).

- Install `apache-airflow==2.10.5` with its official Python 3.11 constraints.
- Install the dbt lab requirements in a second environment.
- Set `AIRFLOW_HOME` to a disposable lab directory and `AIRFLOW__CORE__LOAD_EXAMPLES=false`.
- Set `PLATFORM_OUTPUT_ROOT` to the lab output directory and `DBT_EXECUTABLE` to that second environment's dbt executable.
- Run `airflow db migrate`, then `python labs/airflow/verify.py` using the Airflow environment.

No webserver or public port is opened. This is a pinned Airflow 2 compatibility exercise, not advice to choose that version for new production infrastructure. SQLite metadata, SequentialExecutor and one-second retries are lab settings. `catchup=False` is explicit; changing it on a real scheduler requires a deliberate backfill plan.

Interview: What is the difference between task retry and pipeline idempotency? What if dbt fails after ingestion succeeds? Why separate data by logical date?

Sources: [Airflow installation](https://airflow.apache.org/docs/apache-airflow/2.10.5/installation/index.html), [DAG testing](https://airflow.apache.org/docs/apache-airflow/2.10.5/core-concepts/debug.html).
