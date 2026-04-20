"""
Compute 5-minute OHLCV candles from Silver trade data.

Reads cleaned trades from the silver layer and aggregates into 5-minute
OHLCV windows, written to the gold layer.

Usage:
    spark-submit compute_ohlcv.py --ds 2026-04-13
"""

import argparse
import sys

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Compute 5-min OHLCV candles")
    parser.add_argument("--ds", required=True, help="Processing date YYYY-MM-DD")
    return parser.parse_args()


def main():
    args = parse_args()
    ds = args.ds

    print(f"[compute_ohlcv] Starting for ds={ds}")

    spark = (
        SparkSession.builder
        .appName(f"compute_ohlcv_{ds}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Read silver trades
    # ------------------------------------------------------------------
    silver_path = f"/data/silver/trades/processed_date={ds}"
    print(f"[compute_ohlcv] Reading silver data from: {silver_path}")

    df = spark.read.parquet(silver_path)
    print(f"[compute_ohlcv] Silver row count: {df.count()}")

    # ------------------------------------------------------------------
    # 2. Build 5-minute time windows
    # ------------------------------------------------------------------
    windowed = df.groupBy(
        F.window(F.col("trade_time"), "5 minutes")
    )

    # ------------------------------------------------------------------
    # 3. Aggregate OHLCV per window
    #    open/close require ordering by trade_time inside each window
    # ------------------------------------------------------------------
    ohlcv = windowed.agg(
        F.first(F.col("price")).alias("open"),          # approximate open
        F.max(F.col("price")).alias("high"),
        F.min(F.col("price")).alias("low"),
        F.last(F.col("price")).alias("close"),           # approximate close
        F.sum(F.col("qty")).alias("volume_pepe"),
        F.sum(F.col("usd_value")).alias("volume_usdt"),
        F.count(F.col("trade_id")).alias("trade_count"),
        F.count(
            F.when(F.col("is_sell") == False, F.col("trade_id"))
        ).alias("buyer_initiated_count"),
        F.count(
            F.when(F.col("is_sell") == True, F.col("trade_id"))
        ).alias("seller_initiated_count"),
    )

    # ------------------------------------------------------------------
    # 4. Flatten the window struct and add buy_sell_ratio
    # ------------------------------------------------------------------
    ohlcv = (
        ohlcv
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn(
            "buy_sell_ratio",
            F.col("buyer_initiated_count") / F.col("trade_count")
        )
        .orderBy("window_start")
    )

    # ------------------------------------------------------------------
    # 5. Write to gold layer
    # ------------------------------------------------------------------
    gold_path = f"/data/gold/ohlcv_5min/trade_date={ds}"
    print(f"[compute_ohlcv] Writing gold OHLCV to: {gold_path}")

    ohlcv.write.mode("overwrite").parquet(gold_path)

    final_count = ohlcv.count()
    print(f"[compute_ohlcv] Done. Candle count: {final_count} (5-min windows for {ds})")

    spark.stop()


if __name__ == "__main__":
    main()
