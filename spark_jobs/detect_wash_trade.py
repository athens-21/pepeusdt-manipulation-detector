"""
Wash Trade detection on Silver trade data.

Algorithm (optimised — avoids full cross-join on millions of rows):
  1. Bucket trades into 1-second time buckets
  2. Join buys and sells only within the same time bucket
  3. Apply tight filters: time_diff < 1000ms, price_diff < 0.1%, qty_similarity > 90%
  4. Score each pair and keep wash_score >= 0.8

Usage:
    spark-submit detect_wash_trade.py --ds 2026-04-13
"""

import argparse
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Detect Wash Trading pairs")
    parser.add_argument("--ds", required=True, help="Processing date YYYY-MM-DD")
    return parser.parse_args()


def main():
    args = parse_args()
    ds = args.ds

    print(f"[detect_wash_trade] Starting for ds={ds}")

    spark = (
        SparkSession.builder
        .appName(f"detect_wash_trade_{ds}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Read silver trades
    # ------------------------------------------------------------------
    silver_path = f"/data/silver/trades/processed_date={ds}"
    print(f"[detect_wash_trade] Reading silver data from: {silver_path}")

    df = spark.read.parquet(silver_path)
    total_rows = df.count()
    print(f"[detect_wash_trade] Total trades: {total_rows}")

    # ------------------------------------------------------------------
    # 2. Add 1-second time bucket
    #    time is epoch-milliseconds; divide by 1000 and floor to get second bucket
    # ------------------------------------------------------------------
    df = df.withColumn(
        "time_bucket",
        (F.unix_timestamp(F.col("trade_time")) ).cast("long")
    )

    # ------------------------------------------------------------------
    # 3. Separate into buys and sells
    # ------------------------------------------------------------------
    buys = df.filter(F.col("is_sell") == False).select(
        F.col("trade_id").alias("buy_trade_id"),
        F.col("price").alias("buy_price"),
        F.col("qty").alias("buy_qty"),
        F.col("time").alias("buy_time"),
        F.col("time_bucket").alias("buy_bucket"),
    )

    sells = df.filter(F.col("is_sell") == True).select(
        F.col("trade_id").alias("sell_trade_id"),
        F.col("price").alias("sell_price"),
        F.col("qty").alias("sell_qty"),
        F.col("time").alias("sell_time"),
        F.col("time_bucket").alias("sell_bucket"),
    )

    buy_count  = buys.count()
    sell_count = sells.count()
    print(f"[detect_wash_trade] Buys: {buy_count}, Sells: {sell_count}")

    # ------------------------------------------------------------------
    # 4. Join on same time bucket (restricts search space dramatically)
    # ------------------------------------------------------------------
    pairs = buys.join(sells, buys["buy_bucket"] == sells["sell_bucket"])

    # ------------------------------------------------------------------
    # 5. Apply wash trade filters
    # ------------------------------------------------------------------
    pairs = pairs.withColumn(
        "time_diff_ms",
        F.abs(F.col("buy_time") - F.col("sell_time"))
    ).withColumn(
        "price_diff_pct",
        F.abs(F.col("buy_price") - F.col("sell_price")) / F.col("buy_price") * 100
    ).withColumn(
        "qty_similarity_pct",
        (F.lit(1) - F.abs(F.col("buy_qty") - F.col("sell_qty")) / F.col("buy_qty")) * 100
    )

    pairs = pairs.filter(
        (F.col("time_diff_ms") < 1000)
        & (F.col("price_diff_pct") < 0.1)
        & (F.col("qty_similarity_pct") > 90)
    )

    filtered_count = pairs.count()
    print(f"[detect_wash_trade] Pairs after filter: {filtered_count}")

    # ------------------------------------------------------------------
    # 6. Calculate wash_score (weighted composite)
    # ------------------------------------------------------------------
    pairs = pairs.withColumn(
        "time_score",
        F.lit(1) - (F.col("time_diff_ms") / 1000)
    ).withColumn(
        "price_score",
        F.lit(1) - (F.col("price_diff_pct") / 0.1)
    ).withColumn(
        "qty_score",
        F.col("qty_similarity_pct") / 100
    ).withColumn(
        "wash_score",
        F.col("time_score")  * 0.40
        + F.col("price_score") * 0.35
        + F.col("qty_score")   * 0.25
    )

    # ------------------------------------------------------------------
    # 7. Keep only high-confidence pairs (score >= 0.8)
    # ------------------------------------------------------------------
    pairs = pairs.filter(F.col("wash_score") >= 0.8)

    # ------------------------------------------------------------------
    # 8. Select final output columns
    # ------------------------------------------------------------------
    output = pairs.select(
        F.lit(ds).cast("date").alias("trade_date"),
        F.col("buy_trade_id"),
        F.col("sell_trade_id"),
        F.col("time_diff_ms"),
        F.col("price_diff_pct"),
        F.col("qty_similarity_pct"),
        F.col("wash_score"),
        F.lit(ds).cast("date").alias("processed_date"),
    )

    # ------------------------------------------------------------------
    # 9. Write to gold layer
    # ------------------------------------------------------------------
    gold_path = f"/data/gold/wash_trade_signals/trade_date={ds}"
    print(f"[detect_wash_trade] Writing signals to: {gold_path}")

    output.write.mode("overwrite").parquet(gold_path)

    total_pairs     = output.count()
    high_confidence = output.filter(F.col("wash_score") >= 0.95).count()

    print(f"[detect_wash_trade] Done.")
    print(f"[detect_wash_trade] Total wash trade pairs found:       {total_pairs}")
    print(f"[detect_wash_trade] High-confidence pairs (score>=0.95): {high_confidence}")

    spark.stop()


if __name__ == "__main__":
    main()
