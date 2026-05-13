"""
Pump & Dump detection on 5-minute OHLCV candles.

Algorithm (rolling peak/trough):
  1. For each candle compute rise_from_30m_low:
       rolling_low = min(close) over past 6 candles (30 min)
       rise_pct    = (close - rolling_low) / rolling_low * 100
  2. PUMP confirmed when rise_pct >= PUMP_THRESHOLD (1.5%)
     AND volume_usdt >= 2x rolling 30-min average volume
  3. For each pump peak look forward 12 candles (60 min)
  4. DUMP confirmed when min(future_close) drops <= -DUMP_THRESHOLD (-1.0%) from peak

Thresholds calibrated against PEPE 5-min candle data where
typical p90 rise-from-30m-low is ~0.9% and max observed is ~3%.

Usage:
    spark-submit detect_pump_dump.py --ds 2026-04-13
"""

import argparse

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

PUMP_THRESHOLD = 1.5   # % rise from rolling 30m low
DUMP_THRESHOLD = -1.0  # % drop from peak within next 60 min
LOOKBACK_CANDLES = 6   # 6 × 5 min = 30 min rolling window
LOOKAHEAD_CANDLES = 12 # 12 × 5 min = 60 min forward window


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
    # 2. Rolling 30-min low and average volume (past 6 candles)
    # ------------------------------------------------------------------
    w_lookback = (
        Window.orderBy("window_start")
        .rowsBetween(-LOOKBACK_CANDLES, -1)
    )

    df = (
        df
        .withColumn("rolling_low_30m",    F.min("close").over(w_lookback))
        .withColumn("rolling_avg_vol_30m", F.avg("volume_usdt").over(w_lookback))
    )

    # ------------------------------------------------------------------
    # 3. Compute rise_from_30m_low
    # ------------------------------------------------------------------
    df = df.withColumn(
        "rise_from_low_pct",
        F.when(
            F.col("rolling_low_30m").isNotNull() & (F.col("rolling_low_30m") > 0),
            (F.col("close") - F.col("rolling_low_30m")) / F.col("rolling_low_30m") * 100
        ).otherwise(F.lit(None).cast("double"))
    )

    # Materialise with row_number for forward-looking join
    w_ordered = Window.orderBy("window_start")
    df = df.withColumn("row_num", F.row_number().over(w_ordered))
    df.cache()

    # ------------------------------------------------------------------
    # 4. Identify pump peaks
    # ------------------------------------------------------------------
    pump_candles = df.filter(
        (F.col("rise_from_low_pct") >= PUMP_THRESHOLD)
        & (F.col("volume_usdt") >= F.col("rolling_avg_vol_30m") * 2.0)
    )
    pump_count = pump_candles.count()
    print(f"[detect_pump_dump] Pump peaks identified: {pump_count}")

    if pump_count == 0:
        print(f"[detect_pump_dump] No pump peaks found for {ds}. Writing empty output.")
        spark.createDataFrame([], schema=_output_schema()).write.mode("overwrite").parquet(
            f"/data/gold/pump_dump_signals/trade_date={ds}"
        )
        spark.stop()
        return

    # ------------------------------------------------------------------
    # 5. For each pump peak, look forward LOOKAHEAD_CANDLES to find dump
    # ------------------------------------------------------------------
    pump_alias = pump_candles.select(
        F.col("row_num").alias("pump_row"),
        F.col("window_start").alias("pump_window_start"),
        F.col("window_end").alias("pump_window_end"),
        F.col("rolling_low_30m").alias("price_at_pump_start"),
        F.col("close").alias("price_at_peak"),
        F.col("rise_from_low_pct").alias("pump_pct"),
        F.col("volume_usdt").alias("volume_usdt_during_pump"),
    )

    future_alias = df.select(
        F.col("row_num").alias("future_row"),
        F.col("close").alias("future_close"),
    )

    events = pump_alias.join(
        future_alias,
        (future_alias["future_row"] > pump_alias["pump_row"])
        & (future_alias["future_row"] <= pump_alias["pump_row"] + LOOKAHEAD_CANDLES)
    )

    events = events.groupBy(
        "pump_row", "pump_window_start", "pump_window_end",
        "price_at_pump_start", "price_at_peak", "pump_pct", "volume_usdt_during_pump"
    ).agg(
        F.min("future_close").alias("price_after_dump")
    )

    # ------------------------------------------------------------------
    # 6. Calculate dump_pct and filter
    # ------------------------------------------------------------------
    events = events.withColumn(
        "dump_pct",
        (F.col("price_after_dump") - F.col("price_at_peak")) / F.col("price_at_peak") * 100
    )

    events = events.filter(F.col("dump_pct") <= DUMP_THRESHOLD)

    # ------------------------------------------------------------------
    # 7. Severity based on pump_pct relative to PEPE's typical range
    # ------------------------------------------------------------------
    events = (
        events
        .withColumn("estimated_profit_pct", F.col("pump_pct"))
        .withColumn(
            "severity",
            F.when(F.col("pump_pct") >= 2.5, F.lit("HIGH"))
             .when(F.col("pump_pct") >= 1.5, F.lit("MEDIUM"))
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
