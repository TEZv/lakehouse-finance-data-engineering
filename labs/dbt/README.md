# 🧮 dbt analytics layer

Input: `events.json`, an array with unique `event_id`, positive integer `version` and `quantity`. The shared batch exports current state, not raw duplicate events.

Models: `stg_events` casts the contract; `fct_events` keeps one current row per event using `delete+insert` and a per-event version comparison; `mart_event_summary` reports count and total quantity. No time window, currency conversion or financial risk calculation is implied.

## Run

Use a separate Python 3.11 environment, from the repository root:

```sh
python -m pip install -r labs/dbt/requirements.txt
python labs/dbt/verify.py
```

The verifier creates its own temporary database and input files. It runs initial build (2,16), no-op replay (2,16), correction/new record (3,21), and stale-version replay (3,21). It runs dbt tests on each build and generates documentation at the end. Generated `target` and logs are ignored by Git and uploaded as CI artifacts.

## Limitations

No CDC delete/tombstone support. Input is a complete current snapshot for the relationship test. Equal-version conflicts must be rejected upstream; this model does not resolve them. JSON staging is rescanned, so incremental target writes do not imply incremental source reads. No cloud warehouse execution or dimensional business model is claimed; the fact grain here is deliberately small and explicit.

Interview: Why a per-key version check instead of one global watermark? Why does `unique_key` still need a uniqueness test? What should a deleted source event do?

Sources: [dbt-duckdb implementation](https://github.com/duckdb/dbt-duckdb), [DuckDB's dbt introduction](https://duckdb.org/2025/04/04/dbt-duckdb). [Run evidence](../../.github/workflows/platform-ci.yml).
