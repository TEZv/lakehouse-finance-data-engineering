# ✅ Platform execution evidence

Implementation commit: `b6db693`. Evidence was inspected on 2026-09-10 (Europe/Kyiv); GitHub timestamps for this run are on 2026-09-09 UTC.

[Successful integration run 34409326771](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/runs/34409326771)

| Job | Verified outcome |
|---|---|
| [Airflow](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/runs/34409326771/job/102659798991) | DAG import, dependency, intentional first-attempt failure, actual UP_FOR_RETRY → SUCCESS, two logical dates, dbt build |
| [dbt](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/runs/34409326771/job/102659799115) | Initial and repeat builds, newer correction/new event, older-version replay, SQL/schema tests and generated docs |
| [Hive/Hadoop](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/runs/34409326771/job/102659799159) | One live HDFS DataNode, Hive external partition query, repeat registration, external data retained after dropping/recreating the lab table |
| [Kubernetes](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/runs/34409326771/job/102659799162) | Real kind cluster, expected failing Job, successful non-root Job, batch replay assertions |

[Earlier Kafka broker run 34408679491](https://github.com/TEZv/lakehouse-finance-data-engineering/actions/runs/34408679491) verified producer/consumer delivery and full replay. Locally, the shared batch test and five existing Kafka sink tests also passed.

`dbt-evidence` contains generated dbt artifacts/logs. `airflow-evidence` contains two date-specific summaries in this run; Airflow task/retry logs are in the job console. Artifact and log retention is controlled by GitHub; rerun the workflow if these expire. These links prove the named run/commit, not arbitrary future changes.

No paid cloud resources, employer data, production servers or local administrator installations were used. Refer to [module limitations](../labs/README.md) before using these results in an application.
