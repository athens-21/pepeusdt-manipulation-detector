# pepe-pipeline

A production-grade data pipeline for detecting market manipulation in **PEPEUSDT** cryptocurrency trades. Ingests raw Binance Vision trade data, processes it through a Medallion architecture (Bronze → Silver → Gold), and loads manipulation signals into a MySQL data warehouse for analysis.

Detects two manipulation patterns: **Pump & Dump** events (price spikes ≥10% followed by a sharp drop within 60 minutes) and **Wash Trading** (suspicious buy/sell pairs occurring within milliseconds at near-identical prices and quantities).

---

## Pipeline

```mermaid
flowchart TD
    A(["📥 Binance Vision\nPEPEUSDT-trades-YYYY-MM-DD.zip"]):::source

    subgraph landing["Landing Zone"]
        B["data/landing/\n*.zip"]:::landing
    end

    subgraph airflow["Apache Airflow DAG — pepe_daily_pipeline"]
        direction TB

        T1["✅ check_landing_file\nPythonOperator"]:::py
        T2["🔶 ingest_to_bronze\nPythonOperator\nPandas + PyArrow"]:::py
        T3["⚡ bronze_to_silver\nSparkSubmitOperator\nType-cast · DQ · Filter · Enrich"]:::spark

        subgraph parallel["Parallel Spark Jobs"]
            direction LR
            T4["⚡ compute_ohlcv\n5-min candles"]:::spark
            T5["⚡ detect_pump_dump\nPrice spike ≥10%\n+ Volume ≥2.5×"]:::spark
            T6["⚡ detect_wash_trade\ntime_diff < 1s\nwash_score ≥ 0.8"]:::spark
        end

        T7["🐍 load_gold_to_mysql\nSQLAlchemy · Pandas"]:::py
        T8["✅ validate_dw_counts\nRow count assertions"]:::py

        T1 --> T2 --> T3 --> T4 & T5 & T6 --> T7 --> T8
    end

    subgraph bronze["🥉 Bronze Layer"]
        BR["data/bronze/trades/\ntrade_date=YYYY-MM-DD/\ndata.parquet\n—\nRaw · Immutable"]:::bronze
    end

    subgraph silver["🥈 Silver Layer"]
        SL["data/silver/trades/\nprocessed_date=YYYY-MM-DD/\npart-*.snappy.parquet\n—\nTyped · DQ-checked · Enriched"]:::silver
    end

    subgraph gold["🥇 Gold Layer"]
        G1["ohlcv_5min/\ntrade_date=*/"]:::gold
        G2["pump_dump_signals/\ntrade_date=*/"]:::gold
        G3["wash_trade_signals/\ntrade_date=*/"]:::gold
    end

    subgraph dw["MySQL Data Warehouse — pepe_dw"]
        direction LR
        DIM["dim_date"]:::dim
        FT["fact_trades"]:::fact
        FP["fact_pump_dump_events"]:::fact
        FW["fact_wash_trade_pairs"]:::fact
        DIM --- FT
    end

    A --> B --> T1
    T2 -. writes .-> BR
    T3 -. reads .-> BR
    T3 -. writes .-> SL
    T4 & T5 & T6 -. reads .-> SL
    T4 -. writes .-> G1
    T5 -. writes .-> G2
    T6 -. writes .-> G3
    T7 -. reads .-> G1 & G2 & G3
    T7 -. writes .-> FT & FP & FW

    classDef source    fill:#1a1a2e,color:#e0e0e0,stroke:#4a4a8a
    classDef landing   fill:#2d2d44,color:#e0e0e0,stroke:#6a6a9a
    classDef py        fill:#3b5998,color:#fff,stroke:#2a4070
    classDef spark     fill:#e25822,color:#fff,stroke:#b04010
    classDef bronze    fill:#8B6914,color:#fff,stroke:#6b4f0f
    classDef silver    fill:#607080,color:#fff,stroke:#405060
    classDef gold      fill:#b8860b,color:#fff,stroke:#8b6508
    classDef dim       fill:#2e7d32,color:#fff,stroke:#1b5e20
    classDef fact      fill:#1565c0,color:#fff,stroke:#0d47a1
```

---

## Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Orchestration | Apache Airflow | 2.8.1 |
| Distributed Processing | Apache Spark (PySpark) | 3.5.0 |
| Ingestion / Load | Pandas + PyArrow + SQLAlchemy | — |
| Data Warehouse | MySQL | 8.0 |
| Airflow Metadata DB | PostgreSQL | 15 |
| Containerisation | Docker Compose | v2 |
| Storage Format | Apache Parquet (Snappy) | — |

---

## Architecture

### Medallion Layers

| Layer | Format | Location | Purpose |
|-------|--------|----------|---------|
| Bronze | Parquet | `data/bronze/trades/trade_date=*/` | Raw ingest, immutable |
| Silver | Parquet | `data/silver/trades/processed_date=*/` | Cleaned, typed, DQ-validated, enriched |
| Gold | Parquet | `data/gold/*/trade_date=*/` | Detection signals + OHLCV candles |
| DW | MySQL | `pepe_dw` database | Star schema for reporting |

### Services

```
postgres          ← Airflow metadata DB
airflow-init      ← DB migration + create admin user (runs once)
airflow-webserver ← UI  :8080
airflow-scheduler ← Task execution (LocalExecutor)
spark-master      ← Standalone cluster coordinator  :7077  UI :8090
spark-worker      ← Executor (2 cores, 4 GB RAM)
mysql             ← Data warehouse  :3306
```

---

## Quick Start

### Prerequisites
- Docker Desktop ≥ 24
- Docker Compose v2

### 1. Clone and configure

```bash
git clone <repo-url>
cd pepe-pipeline
cp .env.example .env
```

Edit `.env` — set strong passwords and generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# paste output as FERNET_KEY in .env
```

### 2. Build and start all services

```bash
docker compose build   # ~5 min first time — compiles Python 3.11 for Spark
docker compose up -d
```

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8080 (admin / admin) |
| Spark Master UI | http://localhost:8090 |
| MySQL | localhost:3306 |

### 3. Set Airflow Variables

In the Airflow UI → Admin → Variables, add:

| Key | Value |
|-----|-------|
| `MYSQL_HOST` | `mysql` |
| `MYSQL_USER` | `pepe_user` |
| `MYSQL_PASS` | *(MYSQL_PASSWORD from .env)* |
| `MYSQL_DB` | `pepe_dw` |

The `spark_default` connection is created automatically by `airflow-init`.

---

## Adding Data

1. Download from [Binance Vision](https://data.binance.vision/?prefix=data/spot/daily/trades/PEPEUSDT/):
   ```
   PEPEUSDT-trades-YYYY-MM-DD.zip
   ```
2. Place in `data/landing/`:
   ```bash
   cp ~/Downloads/PEPEUSDT-trades-2026-04-13.zip data/landing/
   ```

---

## Running the Pipeline

### Airflow UI

1. Open http://localhost:8080
2. Toggle `pepe_daily_pipeline` **ON**
3. Click **Trigger DAG w/ config** → `{"ds": "2026-04-13"}`

### CLI

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger pepe_daily_pipeline --conf '{"ds": "2026-04-13"}'
```

### Check results

```bash
# Task states
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run pepe_daily_pipeline <run_id>

# Row counts in MySQL
docker compose exec mysql \
  mysql -u pepe_user -p pepe_dw \
  -e "SELECT COUNT(*) FROM fact_trades WHERE processed_date = '2026-04-13';"
```

---

## Detection Methodology

### Pump & Dump

A 5-minute OHLCV candle is flagged as a **pump** when:
- Price change ≥ **+10%** vs previous candle
- Volume ≥ **2.5×** the 7-period rolling average volume

Confirmed as a **dump** if the minimum close in the next **12 candles (60 min)** drops ≤ **−7%** from the pump peak.

| Severity | Condition |
|----------|-----------|
| `HIGH` | pump ≥ 20% |
| `MEDIUM` | pump 10–19% |
| `LOW` | pump < 10% |

### Wash Trading

A buy/sell pair is flagged when all three hold simultaneously:

| Filter | Threshold |
|--------|-----------|
| `time_diff_ms` | < 1 000 ms |
| `price_diff_pct` | < 0.1 % |
| `qty_similarity_pct` | > 90 % |

Scored as a weighted composite:

```
wash_score = time_score  × 0.40
           + price_score × 0.35
           + qty_score   × 0.25
```

Only pairs with `wash_score ≥ 0.80` are written to the DW.

**Performance:** trades are bucketed into 1-second windows and joined only within the same bucket — avoids an O(n²) cross-join on millions of rows.

---

## MySQL Data Warehouse Schema

```
dim_date ─────────────── fact_trades          (FK: date_id)
dim_time_window          fact_pump_dump_events
                         fact_wash_trade_pairs
```

See [`sql/init.sql`](sql/init.sql) for full DDL.

---

## Project Structure

```
pepe-pipeline/
├── Dockerfile.airflow          # Airflow image: Python 3.11 + Java 17 + providers
├── Dockerfile.spark            # Spark image: Python 3.11 compiled from source
├── docker-compose.yml          # All services
├── .env.example                # Environment variable template
├── dags/
│   └── pepe_daily_pipeline.py  # Airflow DAG
├── spark_jobs/
│   ├── bronze_to_silver.py     # Schema casting + DQ + enrichment
│   ├── compute_ohlcv.py        # 5-min OHLCV candles
│   ├── detect_pump_dump.py     # Pump & Dump detection
│   └── detect_wash_trade.py    # Wash Trade detection
├── dq/
│   └── quality_checks.py       # Reusable PySpark DQ functions
├── sql/
│   └── init.sql                # MySQL star schema DDL + dim_date seed
├── DOCUMENTATION.md            # In-depth project documentation (Thai)
└── data/
    ├── landing/                # Place downloaded ZIPs here
    ├── bronze/                 # Auto-populated by pipeline
    ├── silver/                 # Auto-populated by pipeline
    └── gold/                   # Auto-populated by pipeline
```

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| `PYTHON_VERSION_MISMATCH` | Ensure `PYSPARK_PYTHON=/usr/local/bin/python3.11` is set in Airflow env |
| `PATH_NOT_FOUND` in Spark job | Verify `./data:/data` volume is mounted in both Airflow and Spark services |
| Airflow UI shows "Could not read served logs" | Log display issue only — check actual state via `airflow tasks states-for-dag-run` |
| Spark worker not registering | `docker compose logs spark-worker` |
