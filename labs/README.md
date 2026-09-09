# 🧩 Data platform implementation labs

These modules extend one small synthetic event scenario. They are executable portfolio labs, not four production platforms or proof of commercial experience.

✅ All four platform jobs passed in [run 34409326771](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/runs/34409326771). [Per-job evidence and boundaries](../docs/PLATFORM_EXECUTION_EVIDENCE.md).

```text
Synthetic versioned events
  ├─ Kafka producer → broker → consumer → SQLite replay-safe state
  └─ Python batch adapter (same sink contract) → current-state JSON
       ├─ Airflow → dbt staging → incremental fact → summary + tests
       ├─ Kubernetes Job → batch execution + in-Pod replay
       └─ export → HDFS → Hive partitioned external table
```

The Kafka transport is tested separately; Airflow does NOT consume Kafka in this version. Hive and Kubernetes are alternative execution/storage exercises, not mandatory hops in one unnecessarily complex pipeline. The original PySpark/Delta batch remains separate.

| Module | Actual implementation | How execution is proved |
|---|---|---|
| [Kafka](kafka/README.md) | Producer/consumer, version checks, durable receipts, quarantine | Real broker CI plus five sink tests |
| [Shared batch](platform/batch.py) | Same sink, content-identified batches, atomic JSON export | Repeat run produces two events, quantity 16, six receipts, one quarantine |
| [dbt](dbt/README.md) | DuckDB staging, incremental fact, summary, schema and SQL tests | Four actual builds: initial, replay, correction/new event, stale version; generated docs |
| [Airflow](airflow/README.md) | Scheduled DAG definition, ingestion → dbt, retries, date-isolated outputs | `dag.test()` executes two logical dates and an intentionally failed first attempt followed by recovery |
| [Hive/Hadoop](hive-hadoop/README.md) | Real NameNode/DataNode; HiveServer2; external table and partition | HDFS data + Hive queries; repeated DDL; table drop retains external files |
| [Kubernetes](kubernetes/README.md) | Job, ConfigMap, resources, non-root read-only Pod | Real kind cluster: expected failed Job, successful Job, application replay |

## ▶️ No-admin route: GitHub Actions

1. Open [DE platform integration](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/workflows/platform-ci.yml).
2. Select a run for the relevant commit. Inspect **dbt**, **airflow**, **hive-hadoop**, and **kubernetes** separately.
3. A green job means its assertions ran; a YAML file or pending job alone does not.
4. Expand the execution step and find the final `PASS:` summary.
5. Download `dbt-evidence` (manifest, results, catalog, logs) or `airflow-evidence` (output summaries). Airflow task/retry logs are available in the job console; the first successful run did not emit separate task log files into the artifact.
6. To rerun manually as repository owner, choose **Run workflow** on `main`.

Everything uses disposable runner infrastructure and synthetic data. No cloud account, corporate installation, passwords, or paid cloud resources are required. GitHub Actions usage remains subject to the account's plan and policies.

## 🧪 Shared expected results

E1 arrives twice at version 1 with quantity 10, then at version 2 with quantity 12, then again at version 1. E2 has quantity 4. E3 has an invalid negative quantity.

- Current accepted events: E1=12 and E2=4.
- Total quantity: 16; event count: 2.
- Six transport/batch receipts, including duplicate/stale inputs; one quarantined receipt.
- The dbt correction scenario changes E1 to 15 and adds E4=2: total becomes 21 and stays 21 after an older E1 version is replayed.

## Boundaries

Six records are not a scale benchmark. SQLite is a single-process learning sink. DuckDB is not Snowflake. Airflow's test runner is not a running scheduler or HA deployment. A kind Job is not an operated cloud Kubernetes service. One HDFS DataNode with replication=1 has no node-loss redundancy. Pinned compatibility versions are chosen for reproducible isolated exercises, not as a production security baseline; reassess support and vulnerabilities before any real deployment.

See the [Ukrainian interview walkthrough](../docs/PLATFORM_INTERVIEW_UA.md) and [cross-portfolio coverage](https://github.com/TEZv/Data-Specialist-Portfolio/blob/main/docs/PLATFORM_COVERAGE.md).
