from pathlib import Path

import pytest

from lakehouse.pipeline import build_spark, paths, run_pipeline


@pytest.fixture(scope="session")
def spark():
    session = build_spark("lakehouse-portfolio-tests")
    yield session
    session.stop()


def test_medallion_pipeline_handles_schema_evolution_and_late_corrections(spark, tmp_path: Path):
    root = Path(__file__).parents[1]
    lakehouse = tmp_path / "lakehouse"

    first_run = run_pipeline(spark, root / "resources" / "landing" / "initial", lakehouse)
    second_run = run_pipeline(spark, root / "resources" / "landing" / "late-arriving", lakehouse)
    lake_paths = paths(lakehouse)

    silver = spark.read.format("delta").load(lake_paths["silver"])
    quarantine = spark.read.format("delta").load(lake_paths["quarantine"])
    gold = spark.read.format("delta").load(lake_paths["gold"])

    assert first_run["bronze_rows_in_batch"] == 4
    assert first_run["quarantined_rows_in_run"] == 1
    assert second_run["bronze_rows_in_batch"] == 2
    assert silver.count() == 4
    assert quarantine.count() == 2  # historical invalid record is retained for audit on each ingestion cycle
    assert silver.where("event_id = 'T-1001'").select("quantity").first()[0] == 12
    assert silver.where("event_id = 'T-1001'").select("trade_venue").first()[0] == "XEUR"
    assert gold.where("account_id = 'ACC-01' AND instrument = 'EUROSTOXX50'").select("net_quantity").first()[0] == 8


def test_rerun_is_idempotent_in_silver(spark, tmp_path: Path):
    root = Path(__file__).parents[1]
    lakehouse = tmp_path / "lakehouse"
    source = root / "resources" / "landing" / "initial"

    run_pipeline(spark, source, lakehouse)
    run_pipeline(spark, source, lakehouse)
    silver = spark.read.format("delta").load(paths(lakehouse)["silver"])

    assert silver.count() == 3
