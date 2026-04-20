"""
Data Quality checks for the pepe-pipeline Silver layer.
All functions accept a PySpark DataFrame and return metrics or raise on critical failures.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


def check_row_count(df: DataFrame, table_name: str, min_rows: int = 100_000) -> bool:
    """
    Validate that the DataFrame has a meaningful number of rows.

    Returns True if count >= min_rows.
    Logs a WARNING if count < min_rows but count > 0.
    Raises ValueError if the DataFrame is empty (count == 0).
    """
    count = df.count()
    if count == 0:
        raise ValueError(
            f"[DQ CRITICAL] {table_name}: DataFrame is EMPTY — no rows to process."
        )
    if count < min_rows:
        logger.warning(
            "[DQ WARNING] %s: row count %d is below expected minimum of %d.",
            table_name, count, min_rows,
        )
        return False
    logger.info("[DQ OK] %s: row count %d >= %d.", table_name, count, min_rows)
    return True


def check_price_validity(df: DataFrame, price_col: str = "price") -> tuple[int, int]:
    """
    Check for non-positive prices.

    Returns (total_rows, invalid_rows_count) where invalid = price <= 0 or NULL.
    """
    total = df.count()
    invalid = df.filter(F.col(price_col).isNull() | (F.col(price_col) <= 0)).count()
    if invalid > 0:
        logger.warning(
            "[DQ WARNING] price_validity: %d / %d rows have price <= 0 or NULL.",
            invalid, total,
        )
    else:
        logger.info("[DQ OK] price_validity: all %d rows have valid prices.", total)
    return total, invalid


def check_qty_validity(df: DataFrame, qty_col: str = "qty") -> tuple[int, int]:
    """
    Check for non-positive quantities.

    Returns (total_rows, invalid_rows_count) where invalid = qty <= 0 or NULL.
    """
    total = df.count()
    invalid = df.filter(F.col(qty_col).isNull() | (F.col(qty_col) <= 0)).count()
    if invalid > 0:
        logger.warning(
            "[DQ WARNING] qty_validity: %d / %d rows have qty <= 0 or NULL.",
            invalid, total,
        )
    else:
        logger.info("[DQ OK] qty_validity: all %d rows have valid quantities.", total)
    return total, invalid


def check_timestamp_range(df: DataFrame, ts_col: str, ds: str) -> tuple[int, int]:
    """
    Verify that all timestamps fall within the expected calendar date (ds).

    ds: 'YYYY-MM-DD' string representing the processing date.
    Returns (total_rows, out_of_range_rows).
    """
    total = df.count()
    day_start = F.lit(ds).cast("date")
    day_end = F.date_add(day_start, 1)

    out_of_range = df.filter(
        F.col(ts_col).isNull()
        | (F.col(ts_col).cast("date") < day_start)
        | (F.col(ts_col).cast("date") >= day_end)
    ).count()

    if out_of_range > 0:
        logger.warning(
            "[DQ WARNING] timestamp_range: %d / %d rows are outside date %s.",
            out_of_range, total, ds,
        )
    else:
        logger.info(
            "[DQ OK] timestamp_range: all %d rows fall within %s.", total, ds
        )
    return total, out_of_range


def check_duplicates(df: DataFrame, key_col: str = "trade_id") -> int:
    """
    Count duplicate rows by key column.

    Returns the number of duplicate rows (total occurrences beyond the first).
    """
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    duplicates = total - distinct
    if duplicates > 0:
        logger.warning(
            "[DQ WARNING] duplicates: %d duplicate %s values found in %d rows.",
            duplicates, key_col, total,
        )
    else:
        logger.info("[DQ OK] duplicates: no duplicate %s values in %d rows.", key_col, total)
    return duplicates


def run_all_checks(df: DataFrame, ds: str) -> dict:
    """
    Run the full DQ suite and return a summary dictionary.

    Raises ValueError if any critical check fails:
      - Empty DataFrame
      - > 1% invalid prices
      - > 1% invalid quantities
      - > 5% out-of-range timestamps
    """
    print(f"[DQ] Starting data quality checks for ds={ds} ...")

    summary = {}

    # --- Row count ---
    try:
        summary["row_count_ok"] = check_row_count(df, table_name=f"trades_{ds}")
    except ValueError as exc:
        raise ValueError(str(exc)) from exc

    total_rows = df.count()
    summary["total_rows"] = total_rows

    # --- Price validity ---
    _, invalid_prices = check_price_validity(df)
    summary["invalid_prices"] = invalid_prices
    if invalid_prices / total_rows > 0.01:
        raise ValueError(
            f"[DQ CRITICAL] Price invalidity rate {invalid_prices / total_rows:.2%} exceeds 1% threshold."
        )

    # --- Qty validity ---
    _, invalid_qty = check_qty_validity(df)
    summary["invalid_qty"] = invalid_qty
    if invalid_qty / total_rows > 0.01:
        raise ValueError(
            f"[DQ CRITICAL] Qty invalidity rate {invalid_qty / total_rows:.2%} exceeds 1% threshold."
        )

    # --- Timestamp range ---
    _, out_of_range = check_timestamp_range(df, ts_col="trade_time", ds=ds)
    summary["out_of_range_timestamps"] = out_of_range
    if out_of_range / total_rows > 0.05:
        raise ValueError(
            f"[DQ CRITICAL] Timestamp out-of-range rate {out_of_range / total_rows:.2%} exceeds 5% threshold."
        )

    # --- Duplicates ---
    duplicates = check_duplicates(df, key_col="trade_id")
    summary["duplicate_trade_ids"] = duplicates

    print(f"[DQ] All checks complete. Summary: {summary}")
    return summary
