# 🏞️ Lakehouse Data Engineering Portfolio

[![PySpark and Delta CI](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/workflows/pyspark-ci.yml/badge.svg)](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/workflows/pyspark-ci.yml)

A reproducible, synthetic-data **PySpark + Delta Lake** project showing how a financial-events feed moves through a Databricks-compatible Bronze / Silver / Gold lakehouse.

> Portfolio status: independent lab project. The code is tested locally and in GitHub Actions; no Databricks workspace deployment or commercial Databricks experience is claimed until a controlled deployment is completed.

## 🧭 Why this project exists

This is the lakehouse companion to the [MS SQL Server Data Engineering Portfolio](https://github.com/TEZv/mssql-data-engineering-portfolio), not a duplicate of it.

| SQL Server portfolio | This lakehouse portfolio |
|---|---|
| Relational database development, maintenance, transactions | Distributed transformations with PySpark and Delta Lake |
| Stored procedures, dimensional models, SQL Server operations | Bronze / Silver / Gold layers, schema evolution, Delta `MERGE` |
| Azure SQL + Terraform delivery target | Databricks-compatible code and job definition |

## 🔎 What a reviewer can verify

- **Bronze:** append-only raw event ingestion with source metadata and Delta schema evolution;
- **Silver:** typed transformation, deduplication, invalid-record quarantine and idempotent `MERGE` upserts;
- **Gold:** daily account/instrument exposure and cash-flow aggregates ready for BI or risk analysis;
- **Data quality:** explicit invariants for accepted, quarantined and deduplicated records;
- **Reliability:** a rerun does not duplicate Silver records; late corrections update the existing event;
- **Delivery:** GitHub Actions executes the same PySpark + Delta tests in a clean environment;
- **Databricks handoff:** a Databricks Asset Bundle job definition is included for a future workspace deployment.

The architecture follows the Bronze → Silver → Gold pattern that Databricks documents as the medallion architecture: raw, refined, then business-ready data. [Databricks documentation](https://docs.databricks.com/aws/en/lakehouse/medallion)

## 🧩 Architecture

```text
synthetic REST-style JSON events
             |
             v
   Bronze Delta table (raw + metadata)
             |
             v
 Silver Delta table (typed, deduplicated) ----> quarantine Delta table
             |
             v
 Gold Delta table (daily exposure + cash flow)
             |
             v
       BI / risk / operations consumers
```

## 🗂️ Repository structure

```text
src/lakehouse/                 # reusable PySpark pipeline code
resources/landing/             # synthetic initial and late-arriving source batches
tests/                         # integration assertions against local Delta Lake
databricks.yml                 # Asset Bundle entrypoint
resources/databricks/          # future Databricks job definition
.github/workflows/             # clean-environment CI
docs/                          # interview notes, evidence map and deployment boundary
```

## ▶️ Run locally

You need Python 3.10+ and Java 8/11/17. No database server and no administrator rights are required.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PYTHONPATH = "src"
pytest -q
```

To run the pipeline manually:

```powershell
$env:PYTHONPATH = "src"
python -m lakehouse.pipeline --input resources/landing/initial --lakehouse data/lakehouse
python -m lakehouse.pipeline --input resources/landing/late-arriving --lakehouse data/lakehouse
```

The output path is intentionally ignored by Git. GitHub Actions provides the public reproducible runtime proof when local Java/PySpark setup is unavailable.

## ✅ Honest positioning

Accurate today:

- “Built and CI-validated a Databricks-compatible PySpark and Delta Lake lakehouse project.”
- “Implemented medallion layers, schema evolution, Delta `MERGE`, data-quality checks and idempotent reruns.”

Not accurate yet:

- “Deployed Databricks in production.”
- “Operated Databricks jobs commercially.”

See [evidence map](docs/EVIDENCE_MAP.md), [Databricks deployment runbook](docs/DATABRICKS_DEPLOYMENT_RUNBOOK.md), and [interview story](docs/INTERVIEW_STORY.md).
