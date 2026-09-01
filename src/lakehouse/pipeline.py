from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def build_spark(app_name: str = "lakehouse-finance-portfolio") -> SparkSession:
    """Create a local Spark session with the Delta extensions used by Databricks."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def paths(lakehouse: str | Path) -> dict[str, str]:
    root = Path(lakehouse)
    return {
        "bronze": str(root / "bronze" / "trade_events"),
        "silver": str(root / "silver" / "trade_events"),
        "quarantine": str(root / "quarantine" / "trade_events"),
        "gold": str(root / "gold" / "daily_account_exposure"),
    }


def ingest_bronze(spark: SparkSession, input_dir: str | Path, bronze_path: str) -> int:
    raw = (
        spark.read.option("multiLine", "true").json(str(Path(input_dir) / "*.json"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    raw.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
    return raw.count()


def clean_events(bronze: DataFrame) -> DataFrame:
    # A Bronze table can predate an optional source-field addition. Preserve a
    # stable Silver contract by materializing the field as NULL until it arrives.
    trade_venue = (
        F.col("trade_venue")
        if "trade_venue" in bronze.columns
        else F.lit(None).cast("string")
    )
    return (
        bronze.select(
            "event_id",
            "account_id",
            "instrument",
            "side",
            F.to_timestamp("event_ts").alias("event_ts"),
            F.to_date("event_ts").alias("trade_date"),
            F.col("quantity").cast("decimal(18,4)").alias("quantity"),
            F.col("price").cast("decimal(18,4)").alias("price"),
            "currency",
            F.to_timestamp("source_updated_at").alias("source_updated_at"),
            trade_venue.alias("trade_venue"),
            "_source_file",
            "_ingested_at",
        )
        .withColumn("notional", F.col("quantity") * F.col("price"))
    )


def valid_condition() -> object:
    return (
        F.col("event_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("instrument").isNotNull()
        & F.col("trade_date").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("price").isNotNull()
        & (F.col("price") > 0)
        & F.col("side").isin("BUY", "SELL")
        & F.col("currency").isNotNull()
    )


def merge_silver(spark: SparkSession, accepted: DataFrame, silver_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, silver_path):
        accepted.write.format("delta").mode("overwrite").save(silver_path)
        return

    target = DeltaTable.forPath(spark, silver_path)
    (
        target.alias("target")
        .merge(accepted.alias("source"), "target.event_id = source.event_id")
        .whenMatchedUpdate(
            condition="source.source_updated_at >= target.source_updated_at",
            set={column: f"source.{column}" for column in accepted.columns},
        )
        .whenNotMatchedInsert(values={column: f"source.{column}" for column in accepted.columns})
        .execute()
    )


def transform_silver(spark: SparkSession, bronze_path: str, silver_path: str, quarantine_path: str) -> tuple[int, int]:
    cleaned = clean_events(spark.read.format("delta").load(bronze_path))
    invalid = cleaned.where(~valid_condition()).withColumn(
        "quarantine_reason", F.lit("INVALID_REQUIRED_FIELD_OR_MEASURE")
    )
    invalid.write.format("delta").mode("append").save(quarantine_path)

    latest_per_event = (
        cleaned.where(valid_condition())
        .withColumn(
            "_row_number",
            F.row_number().over(Window.partitionBy("event_id").orderBy(F.col("source_updated_at").desc(), F.col("_ingested_at").desc())),
        )
        .where(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    merge_silver(spark, latest_per_event, silver_path)
    return latest_per_event.count(), invalid.count()


def build_gold(spark: SparkSession, silver_path: str, gold_path: str) -> int:
    gold = (
        spark.read.format("delta").load(silver_path)
        .groupBy("trade_date", "account_id", "instrument", "currency")
        .agg(
            F.sum(F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(-F.col("quantity"))).alias("net_quantity"),
            F.sum(F.when(F.col("side") == "BUY", -F.col("notional")).otherwise(F.col("notional"))).alias("net_cash_flow"),
            F.sum("notional").alias("gross_notional"),
            F.count("*").alias("trade_count"),
        )
    )
    gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)
    return gold.count()


def run_pipeline(spark: SparkSession, input_dir: str | Path, lakehouse: str | Path) -> dict[str, int]:
    lake_paths = paths(lakehouse)
    bronze_rows = ingest_bronze(spark, input_dir, lake_paths["bronze"])
    accepted_rows, quarantined_rows = transform_silver(
        spark, lake_paths["bronze"], lake_paths["silver"], lake_paths["quarantine"]
    )
    gold_rows = build_gold(spark, lake_paths["silver"], lake_paths["gold"])
    return {
        "bronze_rows_in_batch": bronze_rows,
        "accepted_distinct_events": accepted_rows,
        "quarantined_rows_in_run": quarantined_rows,
        "gold_rows": gold_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the synthetic finance lakehouse pipeline")
    parser.add_argument("--input", required=True, help="Directory containing JSON event batches")
    parser.add_argument("--lakehouse", default="data/lakehouse", help="Output directory for Delta tables")
    parser.add_argument("--reset", action="store_true", help="Delete only the specified local lakehouse output before running")
    args = parser.parse_args()

    if args.reset:
        shutil.rmtree(args.lakehouse, ignore_errors=True)

    spark = build_spark()
    try:
        print(run_pipeline(spark, args.input, args.lakehouse))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
