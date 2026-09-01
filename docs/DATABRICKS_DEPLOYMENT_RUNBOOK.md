# ☁️ Databricks deployment runbook

## Purpose

Run this only after the local/GitHub CI is green. The goal is a small, controlled proof that the same project can run in a personal Databricks workspace — not to claim commercial deployment.

## Recommended route

Databricks Free Edition is designed for students, educators and people learning or experimenting with the platform. Create a personal workspace, never use an employer workspace, dataset or credentials. [Official Free Edition guide](https://docs.databricks.com/aws/en/getting-started/free-edition)

1. Create a personal workspace and enable MFA if offered.
2. Create a Git folder or upload only this public repository; do not upload work files.
3. Run the initial JSON batch and inspect Bronze, Silver, quarantine and Gold tables.
4. Run the late-arriving batch; show that `T-1001` updates and gets `trade_venue`.
5. Save redacted screenshots of the job run, table history and the Gold query result.
6. Delete the test data/job if the workspace is not intended for continued practice.

## What to retain as proof

- link to this exact commit;
- screenshot of a successful Databricks job run;
- screenshot of Gold output and Delta history, with account IDs redacted if replaced by real examples;
- a short README note: “Executed in a personal learning workspace on YYYY-MM-DD.”

## What not to claim

- no production workload;
- no employer environment;
- no volume, performance or cost claim beyond what was actually observed.
