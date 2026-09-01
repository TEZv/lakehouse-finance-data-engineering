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

The vacancy material points to four groups. This portfolio intentionally covers the lakehouse group now; the other groups remain separately visible rather than being cosmetically “claimed”.

| Group | Current evidence | Next step |
|---|---|---|
| SQL and data modelling | MS SQL Server portfolio | Azure SQL controlled deployment |
| Lakehouse / Big Data | This PySpark + Delta project | Databricks Free Edition job run |
| Orchestration / streaming | Design boundary only | Airflow batch orchestration, then Kafka/Structured Streaming if target jobs need it |
| Cloud / platform | Terraform Azure SQL + GitHub Actions | One controlled cloud deployment with redacted evidence |

Do not add Kafka, Airflow, Snowflake, BigQuery, ClickHouse, Kubernetes or Scala merely as badges. Add each only when a coherent target role repeatedly requests it and there is an inspectable artifact.
