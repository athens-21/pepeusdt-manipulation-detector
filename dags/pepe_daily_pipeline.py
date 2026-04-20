"""
pepe_daily_pipeline — Airflow DAG

Orchestrates the full PEPEUSDT market manipulation detection pipeline:
  Landing → Bronze → Silver → Gold (OHLCV + Pump&Dump + WashTrade) → MySQL DW

Schedule: daily at 01:00 UTC (data for previous day should be available)
"""

from __future__ import annotations

import os
import zipfile
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ---------------------------------------------------------------------------
# DAG-level defaults
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}

DATA_ROOT      = "/opt/airflow/data"
SPARK_JOBS_DIR = "/spark_jobs"


# ---------------------------------------------------------------------------
# Helper: resolve ds from conf or context
# ---------------------------------------------------------------------------
def _get_ds(context: dict) -> str:
    conf = context.get("dag_run").conf or {}
    return conf.get("ds") or context["ds"]


# ---------------------------------------------------------------------------
# Helper: base paths
# ---------------------------------------------------------------------------
def _landing_path(ds: str) -> str:
    return f"{DATA_ROOT}/landing/PEPEUSDT-trades-{ds}.zip"

def _bronze_path(ds: str) -> str:
    return f"{DATA_ROOT}/bronze/trades/trade_date={ds}/data.parquet"

def _silver_path(ds: str) -> str:
    return f"{DATA_ROOT}/silver/trades/processed_date={ds}"

def _gold_ohlcv_path(ds: str) -> str:
    return f"{DATA_ROOT}/gold/ohlcv_5min/trade_date={ds}"

def _gold_pump_dump_path(ds: str) -> str:
    return f"{DATA_ROOT}/gold/pump_dump_signals/trade_date={ds}"

def _gold_wash_trade_path(ds: str) -> str:
    return f"{DATA_ROOT}/gold/wash_trade_signals/trade_date={ds}"


# ---------------------------------------------------------------------------
# Task: check_landing_file
# ---------------------------------------------------------------------------
def check_landing_file(**context) -> str:
    ds = _get_ds(context)
    path = _landing_path(ds)
    print(f"[check_landing_file] Checking for: {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Landing file not found: {path}\n"
            f"Please download it from:\n"
            f"  https://data.binance.vision/?prefix=data/spot/daily/trades/PEPEUSDT/\n"
            f"and place it in the data/landing/ directory."
        )
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"[check_landing_file] Found {path} ({size_mb:.1f} MB)")
    return path


# ---------------------------------------------------------------------------
# Task: ingest_to_bronze
# ---------------------------------------------------------------------------
BRONZE_COLUMNS = [
    "trade_id",
    "price",
    "qty",
    "quote_qty",
    "time",
    "is_buyer_maker",
    "is_best_match",
]

def ingest_to_bronze(**context) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq

    ds = _get_ds(context)
    landing = _landing_path(ds)
    bronze  = _bronze_path(ds)

    print(f"[ingest_to_bronze] Unzipping {landing}")

    with zipfile.ZipFile(landing, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV found inside {landing}")
        csv_name = csv_names[0]
        print(f"[ingest_to_bronze] Extracting {csv_name} ...")
        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, header=None, names=BRONZE_COLUMNS, index_col=False)

    row_count = len(df)
    print(f"[ingest_to_bronze] Loaded {row_count:,} rows from CSV")

    df["_source_file"] = os.path.basename(landing)
    df["_ingested_at"] = pd.Timestamp.utcnow()
    df["trade_date"]   = ds

    bronze_dir = os.path.dirname(bronze)
    os.makedirs(bronze_dir, exist_ok=True)

    pq.write_table(pa.Table.from_pandas(df), bronze)

    print(f"[ingest_to_bronze] Written {row_count:,} rows to {bronze}")
    context["ti"].xcom_push(key="bronze_path", value=bronze)
    return bronze


# ---------------------------------------------------------------------------
# Task: load_gold_to_mysql
# ---------------------------------------------------------------------------
def load_gold_to_mysql(**context) -> None:
    from sqlalchemy import create_engine, text

    ds = _get_ds(context)

    mysql_host = Variable.get("MYSQL_HOST", default_var="mysql")
    mysql_user = Variable.get("MYSQL_USER", default_var="pepe_user")
    mysql_pass = Variable.get("MYSQL_PASS", default_var="dwpass123")
    mysql_db   = Variable.get("MYSQL_DB",   default_var="pepe_dw")

    engine = create_engine(
        f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}",
        pool_pre_ping=True,
    )

    # ---- dim_date upsert ----
    date_obj    = datetime.strptime(ds, "%Y-%m-%d").date()
    date_id     = int(ds.replace("-", ""))
    day_of_week = date_obj.weekday()
    is_weekend  = day_of_week >= 5

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT IGNORE INTO dim_date
                  (date_id, full_date, year, month, day_of_month, day_of_week, is_weekend)
                VALUES
                  (:date_id, :full_date, :year, :month, :dom, :dow, :is_weekend)
            """),
            {
                "date_id":    date_id,
                "full_date":  ds,
                "year":       date_obj.year,
                "month":      date_obj.month,
                "dom":        date_obj.day,
                "dow":        day_of_week,
                "is_weekend": is_weekend,
            }
        )

    _load_silver_trades(engine, ds)
    _load_pump_dump(engine, ds)
    _load_wash_trades(engine, ds)

    print(f"[load_gold_to_mysql] All tables loaded successfully for ds={ds}")


def _load_silver_trades(engine, ds: str) -> None:
    silver_path = _silver_path(ds)
    print(f"[load_gold_to_mysql] Loading fact_trades from {silver_path}")

    df = pd.read_parquet(silver_path)
    df["date_id"] = int(ds.replace("-", ""))

    cols = [
        "trade_id", "date_id", "price", "qty", "quote_qty",
        "trade_time", "is_buyer_maker", "is_sell", "usd_value",
        "trade_hour", "processed_date",
    ]
    df = df[cols]

    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM fact_trades WHERE processed_date = :ds"),
            {"ds": ds}
        )

    df.to_sql("fact_trades", engine, if_exists="append", index=False, chunksize=5000)
    print(f"[load_gold_to_mysql] fact_trades: inserted {len(df):,} rows for {ds}")


def _load_pump_dump(engine, ds: str) -> None:
    from sqlalchemy import text
    gold_path = _gold_pump_dump_path(ds)
    print(f"[load_gold_to_mysql] Loading fact_pump_dump_events from {gold_path}")

    try:
        df = pd.read_parquet(gold_path)
    except Exception as exc:
        print(f"[load_gold_to_mysql] No pump_dump data at {gold_path}: {exc}")
        return

    if df.empty:
        print(f"[load_gold_to_mysql] fact_pump_dump_events: 0 rows for {ds} (no events detected)")
        return

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM fact_pump_dump_events WHERE trade_date = :ds"),
            {"ds": ds}
        )

    df.to_sql("fact_pump_dump_events", engine, if_exists="append", index=False)
    print(f"[load_gold_to_mysql] fact_pump_dump_events: inserted {len(df):,} rows for {ds}")


def _load_wash_trades(engine, ds: str) -> None:
    from sqlalchemy import text
    gold_path = _gold_wash_trade_path(ds)
    print(f"[load_gold_to_mysql] Loading fact_wash_trade_pairs from {gold_path}")

    try:
        df = pd.read_parquet(gold_path)
    except Exception as exc:
        print(f"[load_gold_to_mysql] No wash_trade data at {gold_path}: {exc}")
        return

    if df.empty:
        print(f"[load_gold_to_mysql] fact_wash_trade_pairs: 0 rows for {ds} (no events detected)")
        return

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM fact_wash_trade_pairs WHERE trade_date = :ds"),
            {"ds": ds}
        )

    df.to_sql("fact_wash_trade_pairs", engine, if_exists="append", index=False)
    print(f"[load_gold_to_mysql] fact_wash_trade_pairs: inserted {len(df):,} rows for {ds}")


# ---------------------------------------------------------------------------
# Task: validate_dw_counts
# ---------------------------------------------------------------------------
def validate_dw_counts(**context) -> None:
    from sqlalchemy import create_engine, text

    ds = _get_ds(context)

    mysql_host = Variable.get("MYSQL_HOST", default_var="mysql")
    mysql_user = Variable.get("MYSQL_USER", default_var="pepe_user")
    mysql_pass = Variable.get("MYSQL_PASS", default_var="dwpass123")
    mysql_db   = Variable.get("MYSQL_DB",   default_var="pepe_dw")

    engine = create_engine(
        f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}",
        pool_pre_ping=True,
    )

    checks = {
        "fact_trades":           "SELECT COUNT(*) FROM fact_trades WHERE processed_date = :ds",
        "fact_pump_dump_events": "SELECT COUNT(*) FROM fact_pump_dump_events WHERE trade_date = :ds",
        "fact_wash_trade_pairs": "SELECT COUNT(*) FROM fact_wash_trade_pairs WHERE trade_date = :ds",
    }

    failed_tables = []
    with engine.connect() as conn:
        for table, query in checks.items():
            count = conn.execute(text(query), {"ds": ds}).scalar()
            print(f"[validate_dw_counts] {table}: {count:,} rows for {ds}")
            if table == "fact_trades" and count == 0:
                failed_tables.append(table)

    if failed_tables:
        raise ValueError(
            f"[validate_dw_counts] CRITICAL: the following tables have 0 rows for {ds}: "
            + ", ".join(failed_tables)
        )

    print(f"[validate_dw_counts] All DW counts validated for {ds}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="pepe_daily_pipeline",
    description="PEPEUSDT market manipulation detection pipeline",
    start_date=datetime(2026, 4, 12),
    schedule="0 1 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "ds": Param(
            "2026-04-13",
            type="string",
            description="Date to process (YYYY-MM-DD)",
        )
    },
    tags=["pepe", "crypto", "market-manipulation"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_check_landing = PythonOperator(
        task_id="check_landing_file",
        python_callable=check_landing_file,
    )

    t_ingest_bronze = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=ingest_to_bronze,
    )

    _spark_master = "spark://spark-master:7077"
    _spark_conf   = {
        "spark.master": _spark_master,
        "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python3.11",
    }
    _ds_arg       = '{{ dag_run.conf.get("ds") or ds }}'

    t_bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{SPARK_JOBS_DIR}/bronze_to_silver.py",
        application_args=["--ds", _ds_arg],
        conn_id="spark_default",
        conf=_spark_conf,
        name="bronze_to_silver_{{ ds }}",
    )

    t_detect_pump_dump = SparkSubmitOperator(
        task_id="detect_pump_dump",
        application=f"{SPARK_JOBS_DIR}/detect_pump_dump.py",
        application_args=["--ds", _ds_arg],
        conn_id="spark_default",
        conf=_spark_conf,
        name="detect_pump_dump_{{ ds }}",
    )

    t_detect_wash_trade = SparkSubmitOperator(
        task_id="detect_wash_trade",
        application=f"{SPARK_JOBS_DIR}/detect_wash_trade.py",
        application_args=["--ds", _ds_arg],
        conn_id="spark_default",
        conf=_spark_conf,
        name="detect_wash_trade_{{ ds }}",
    )

    t_compute_ohlcv = SparkSubmitOperator(
        task_id="compute_ohlcv",
        application=f"{SPARK_JOBS_DIR}/compute_ohlcv.py",
        application_args=["--ds", _ds_arg],
        conn_id="spark_default",
        conf=_spark_conf,
        name="compute_ohlcv_{{ ds }}",
    )

    t_load_mysql = PythonOperator(
        task_id="load_gold_to_mysql",
        python_callable=load_gold_to_mysql,
    )

    t_validate = PythonOperator(
        task_id="validate_dw_counts",
        python_callable=validate_dw_counts,
    )

    (
        start
        >> t_check_landing
        >> t_ingest_bronze
        >> t_bronze_to_silver
        >> [t_detect_pump_dump, t_detect_wash_trade, t_compute_ohlcv]
        >> t_load_mysql
        >> t_validate
        >> end
    )
