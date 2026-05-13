"""
pepe_run_all — Airflow DAG

Scans the landing directory for all available PEPEUSDT zip files and
triggers pepe_daily_pipeline for each date found.  Run this once to
backfill everything without manually selecting dates.

Trigger manually from the Airflow UI (no schedule).
"""

from __future__ import annotations

import glob
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DATA_ROOT = "/opt/airflow/data"


def scan_landing_files(**context) -> list[str]:
    pattern = os.path.join(DATA_ROOT, "landing", "PEPEUSDT-trades-*.zip")
    paths = sorted(glob.glob(pattern))

    if not paths:
        raise FileNotFoundError(
            f"No landing files found matching: {pattern}\n"
            "Download from https://data.binance.vision/?prefix=data/spot/daily/trades/PEPEUSDT/"
        )

    dates = []
    for path in paths:
        filename = os.path.basename(path)
        date = filename.replace("PEPEUSDT-trades-", "").replace(".zip", "")
        dates.append(date)

    print(f"[pepe_run_all] Found {len(dates)} landing file(s): {dates}")
    context["ti"].xcom_push(key="dates", value=dates)
    return dates


def trigger_all_dates(**context) -> None:
    import json
    import subprocess

    dates: list[str] = context["ti"].xcom_pull(
        task_ids="scan_landing_files", key="dates"
    )

    for ds in dates:
        run_id = f"batch__{ds}__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        print(f"[pepe_run_all] Triggering pepe_daily_pipeline for ds={ds}")
        subprocess.run(
            [
                "airflow", "dags", "trigger",
                "pepe_daily_pipeline",
                "--run-id", run_id,
                "--conf", json.dumps({"ds": ds}),
            ],
            check=True,
        )

    print(f"[pepe_run_all] Done — triggered {len(dates)} pipeline run(s)")


with DAG(
    dag_id="pepe_run_all",
    description="Trigger pepe_daily_pipeline for every landing file found",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 0,
    },
    tags=["pepe", "batch", "utility"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_scan = PythonOperator(
        task_id="scan_landing_files",
        python_callable=scan_landing_files,
    )

    t_trigger = PythonOperator(
        task_id="trigger_all_dates",
        python_callable=trigger_all_dates,
    )

    start >> t_scan >> t_trigger >> end
