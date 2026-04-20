"""
Pump & Dump detection on 5-minute OHLCV candles.

Algorithm:
  1. Identify PUMP candles: price change >= 10% AND volume >= 2.5x rolling average
  2. For each pump candle look forward 12 candles (60 min)
  3. Confirm DUMP: minimum close in the next 60 min drops <= -7% from pump peak

Usage:
    spark-submit detect_pump_dump.py --ds 2026-04-13
"""

import argparse
import sys

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


def parse_args():
    parser = argparse.ArgumentParser(description="Detect Pump & Dump events")
    parser.add_argument("--ds", required=True, help="Processing date YYYY-MM-DD")
    return parser.parse_args()


def main():
    args = parse_args()
    ds = args.ds

    print(f"[detect_pump_dump] Starting for ds={ds}")

    spark = (
        SparkSession.builder
        .appName(f"detect_pump_dump_{ds}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Read OHLCV gold data
    # ------------------------------------------------------------------
    ohlcv_path = f"/data/gold/ohlcv_5min/trade_date={ds}"
    print(f"[detect_pump_dump] Reading OHLCV from: {ohlcv_path}")

    df = spark.read.parquet(ohlcv_path).orderBy("window_start")
    print(f"[detect_pump_dump] OHLCV candle count: {df.count()}")

    # ------------------------------------------------------------------
    # 2. Window functions ordered by window_start
    # ------------------------------------------------------------------
    w_ordered = Window.orderBy("window_start")

    # Previous close (lag 1)
    df = df.withColumn("prev_close", F.lag("close", 1).over(w_ordered))

    # Percent change from previous close
    df = df.withColumn(
        "pct_change",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
            (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100
        ).otherwise(F.lit(None).cast("double"))
    )

    # ------------------------------------------------------------------
    # 3. 7-period rolling average volume (±3 rows around current row)
    # ------------------------------------------------------------------
    w_rolling = (
        Window.orderBy("window_start")
        .rowsBetween(-3, 3)
    )
    df = df.withColumn("avg_volume", F.avg("volume_usdt").over(w_rolling))

    # Materialise with row_number for forward-looking join
    df = df.withColumn("row_num", F.row_number().over(w_ordered))
    df.cache()

    # ------------------------------------------------------------------
    # 4. Detect PUMP candles
    # ------------------------------------------------------------------
    pump_candles = df.filter(
        (F.col("pct_change") >= 10)
        & (F.col("volume_usdt") >= F.col("avg_volume") * 2.5)
    )
    pump_count = pump_candles.count()
    print(f"[detect_pump_dump] Pump candles identified: {pump_count}")

    if pump_count == 0:
        print(f"[detect_pump_dump] No pump candles found for {ds}. Writing empty output.")
        # Write empty parquet with consistent schema
        spark.createDataFrame([], schema=_output_schema()).write.mode("overwrite").parquet(
            f"/data/gold/pump_dump_signals/trade_date={ds}"
        )
        spark.stop()
        return

    # ------------------------------------------------------------------
    # 5. For each pump, look forward 12 candles to find the dump
    #    Join pump candles with all candles where row_num is in (pump+1, pump+12)
    # ------------------------------------------------------------------
    pump_alias = pump_candles.select(
        F.col("row_num").alias("pump_row"),
        F.col("window_start").alias("pump_window_start"),
        F.col("window_end").alias("pump_window_end"),
        F.col("prev_close").alias("price_at_pump_start"),
        F.col("close").alias("price_at_peak"),
        F.col("pct_change").alias("pump_pct"),
        F.col("volume_usdt").alias("volume_usdt_during_pump"),
    )

    future_alias = df.select(
        F.col("row_num").alias("future_row"),
        F.col("close").alias("future_close"),
    )

    # Cross-join within the 12-candle forward window
    events = pump_alias.join(
        future_alias,
        (future_alias["future_row"] > pump_alias["pump_row"])
        & (future_alias["future_row"] <= pump_alias["pump_row"] + 12)
    )

    # Minimum close in the look-ahead window per pump candle
    events = events.groupBy(
        "pump_row", "pump_window_start", "pump_window_end",
        "price_at_pump_start", "price_at_peak", "pump_pct", "volume_usdt_during_pump"
    ).agg(
        F.min("future_close").alias("price_after_dump")
    )

    # ------------------------------------------------------------------
    # 6. Calculate dump % and filter: dump_pct <= -7
    # ------------------------------------------------------------------
    events = events.withColumn(
        "dump_pct",
        (F.col("price_after_dump") - F.col("price_at_peak")) / F.col("price_at_peak") * 100
    )

    events = events.filter(F.col("dump_pct") <= -7)

    # ------------------------------------------------------------------
    # 7. Add derived columns
    # ------------------------------------------------------------------
    events = (
        events
        .withColumn("estimated_profit_pct", F.col("pump_pct"))
        .withColumn(
            "severity",
            F.when(F.col("pump_pct") >= 20, F.lit("HIGH"))
             .when(F.col("pump_pct") >= 10, F.lit("MEDIUM"))
             .otherwise(F.lit("LOW"))
        )
        .withColumn("trade_date", F.lit(ds).cast("date"))
        .withColumn("processed_date", F.lit(ds).cast("date"))
        .drop("pump_row")
    )

    # ------------------------------------------------------------------
    # 8. Write to gold layer
    # ------------------------------------------------------------------
    gold_path = f"/data/gold/pump_dump_signals/trade_date={ds}"
    print(f"[detect_pump_dump] Writing signals to: {gold_path}")

    events.write.mode("overwrite").parquet(gold_path)

    final_count = events.count()
    print(f"[detect_pump_dump] Done. Pump & Dump events detected: {final_count}")

    spark.stop()


def _output_schema():
    from pyspark.sql.types import (
        StructType, StructField, DateType, TimestampType,
        DoubleType, StringType
    )
    return StructType([
        StructField("pump_window_start",       TimestampType(), True),
        StructField("pump_window_end",         TimestampType(), True),
        StructField("price_at_pump_start",     DoubleType(),    True),
        StructField("price_at_peak",           DoubleType(),    True),
        StructField("price_after_dump",        DoubleType(),    True),
        StructField("pump_pct",                DoubleType(),    True),
        StructField("dump_pct",                DoubleType(),    True),
        StructField("volume_usdt_during_pump", DoubleType(),    True),
        StructField("estimated_profit_pct",    DoubleType(),    True),
        StructField("severity",                StringType(),    True),
        StructField("trade_date",              DateType(),      True),
        StructField("processed_date",          DateType(),      True),
    ])


if __name__ == "__main__":
    main()
