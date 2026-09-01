# 🎙️ Interview story

## 90-second version

“I built this project to demonstrate lakehouse patterns beyond relational SQL. A synthetic trade-events feed lands in an immutable Bronze Delta table. Silver applies typing, validation, quarantine and an idempotent Delta `MERGE`, so a late correction updates the event rather than duplicating it. Gold calculates daily account and instrument exposure for BI or risk consumers. The whole flow is exercised in a clean GitHub Actions environment using PySpark and Delta Lake. It is an independent project, not commercial Databricks delivery; the next proof gate is a run in my personal Databricks workspace.”

## Show, do not merely tell

1. Start at the green CI badge.
2. Open `pipeline.py` and point to the validation predicate and `MERGE` condition.
3. Open the late-arriving JSON fixture: it introduces `trade_venue` and corrects `T-1001`.
4. Show the assertion proving that the corrected record is updated and that Silver remains idempotent.
5. Explain why quarantine is retained for audit rather than silently dropped.

## Practical-task choices

- If asked for a batch pipeline, adapt this repository.
- If asked for streaming, preserve the same Bronze/Silver/Gold contracts and replace the batch reader with `readStream` only when the task truly requires it.
- If asked for an orchestrator, add Airflow around this pipeline instead of pretending that a notebook schedule is the same thing.
