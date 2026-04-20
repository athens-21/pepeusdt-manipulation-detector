"""
Bronze → Silver transformation for PEPEUSDT trades.

Reads raw parquet from bronze layer, applies schema casting, data quality checks,
derives enrichment columns, and writes clean parquet to the silver layer.

Usage:
    spark-submit bronze_to_silver.py --ds 2026-04-13
"""

import argparse
import sys

sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, LongType, BooleanType, DateType

from dq.quality_checks import run_all_checks


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze → Silver: PEPEUSDT trades")
    parser.add_argument("--ds", required=True, help="Processing date YYYY-MM-DD")
    return parser.parse_args()


def main():
    args = parse_args()
    ds = args.ds

    print(f"[bronze_to_silver] Starting for ds={ds}")

    spark = (
        SparkSession.builder
        .appName(f"bronze_to_silver_{ds}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")


    # ------------------------------------------------------------------
    # 1. Read bronze parquet
    # ------------------------------------------------------------------
    bronze_path = f"/data/bronze/trades/trade_date={ds}/data.parquet"
    print(f"[bronze_to_silver] Reading bronze data from: {bronze_path}")

    df = spark.read.parquet(bronze_path)
    print(f"[bronze_to_silver] Raw bronze row count: {df.count()}")

    # ------------------------------------------------------------------
    # 2. Cast columns to correct types
    # ------------------------------------------------------------------
    df = (
        df
        .withColumn("price",          F.col("price").cast(DoubleType()))
        .withColumn("qty",            F.col("qty").cast(DoubleType()))
        .withColumn("quote_qty",      F.col("quote_qty").cast(DoubleType()))
        .withColumn("time",           F.col("time").cast(LongType()))
        .withColumn("is_buyer_maker", F.col("is_buyer_maker").cast(BooleanType()))
    )

    # ------------------------------------------------------------------
    # 3. Convert epoch microseconds → TimestampType
    # ------------------------------------------------------------------
    df = df.withColumn(
        "trade_time",
        F.to_timestamp(F.col("time") / 1_000_000)
    )

    # ------------------------------------------------------------------
    # 4. Data Quality checks (pre-filter)
    # ------------------------------------------------------------------
    print("[bronze_to_silver] Running data quality checks ...")
    run_all_checks(df, ds=ds)

    # ------------------------------------------------------------------
    # 5. Filter out invalid rows (log counts at each step)
    # ------------------------------------------------------------------
    before_price_filter = df.count()
    df = df.filter(F.col("price") > 0)
    after_price_filter = df.count()
    removed_price = before_price_filter - after_price_filter
    print(f"[bronze_to_silver] Rows removed due to price <= 0: {removed_price}")

    before_qty_filter = df.count()
    df = df.filter(F.col("qty") > 0)
    after_qty_filter = df.count()
    removed_qty = before_qty_filter - after_qty_filter
    print(f"[bronze_to_silver] Rows removed due to qty <= 0: {removed_qty}")

    before_ts_filter = df.count()
    df = df.filter(F.col("trade_time").cast("date") == F.lit(ds).cast("date"))
    after_ts_filter = df.count()
    removed_ts = before_ts_filter - after_ts_filter
    print(f"[bronze_to_silver] Rows removed due to out-of-range timestamp: {removed_ts}")

    before_dedup = df.count()
    df = df.dropDuplicates(["trade_id"])
    after_dedup = df.count()
    removed_dups = before_dedup - after_dedup
    print(f"[bronze_to_silver] Rows removed due to duplicate trade_id: {removed_dups}")

    # ------------------------------------------------------------------
    # 6. Add derived columns
    # ------------------------------------------------------------------
    df = (
        df
        .withColumn("trade_hour",     F.hour(F.col("trade_time")))
        .withColumn("trade_minute",   F.minute(F.col("trade_time")))
        .withColumn("is_sell",        ~F.col("is_buyer_maker"))
        .withColumn("usd_value",      F.col("qty") * F.col("price"))
        .withColumn("processed_date", F.lit(ds).cast(DateType()))
    )

    # ------------------------------------------------------------------
    # 7. Write to silver layer (partitioned by processed_date)
    # ------------------------------------------------------------------
    silver_path = f"/data/silver/trades/processed_date={ds}"
    print(f"[bronze_to_silver] Writing silver data to: {silver_path}")

    df.write.mode("overwrite").parquet(silver_path)

    final_count = df.count()
    print(f"[bronze_to_silver] Done. Final silver row count: {final_count}")

    spark.stop()


if __name__ == "__main__":
    main()
