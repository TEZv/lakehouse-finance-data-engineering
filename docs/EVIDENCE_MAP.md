# 🔎 Evidence map

| Requirement theme | Evidence in this repository | Honest boundary |
|---|---|---|
| Python / PySpark | Reusable PySpark transformations under `src/lakehouse` | Independent project, not commercial tenure |
| Spark / distributed processing | Spark DataFrame APIs and local multi-threaded execution in CI | No cluster-scale performance claim |
| Delta Lake | Delta tables, schema evolution and `MERGE` | No production Delta table ownership claim |
| Data quality | Invalid-event quarantine and integration assertions | Synthetic data only |
| Data warehousing | Gold exposure/cash-flow model for BI/risk users | A compact portfolio model, not an enterprise DWH |
| Cloud / Databricks | Asset Bundle job definition and deployment runbook | No workspace deployment claimed until executed |
| CI/CD | Public GitHub Actions PySpark + Delta integration test | CI proves the code path, not a customer delivery |

## Technology coverage strategy

The core pipeline and the platform labs are separate, inspectable artifacts. Their boundaries matter: a passing lab is evidence of a bounded implementation, not evidence of commercial production ownership.

| Group | Current evidence | Next step |
|---|---|---|
| SQL and data modelling | MS SQL Server portfolio | Azure SQL controlled deployment |
| Lakehouse / Big Data | This PySpark + Delta project | Databricks Free Edition job run |
| Orchestration / streaming | [Airflow → dbt batch lab](../labs/airflow-dbt/) and [Kafka replay lab](../labs/kafka/) | Add Structured Streaming only when it is a coherent extension, not a badge |
| Storage / execution platform | [Hive/HDFS lab](../labs/hive-hadoop/) and [restricted Kubernetes Job lab](../labs/kubernetes/) | One controlled cloud deployment with redacted evidence |
| Cloud / platform | Terraform Azure SQL + GitHub Actions | One controlled cloud deployment with redacted evidence |

Kafka, Airflow, Hive/HDFS and Kubernetes have inspectable bounded labs. Snowflake, BigQuery, ClickHouse and Scala are not claimed here: add any of them only when a coherent target role repeatedly requests it and there is an inspectable artifact.
