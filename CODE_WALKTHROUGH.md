# Code Walkthrough — เดิน code ตามลำดับ Pipeline

ไฟล์นี้อธิบาย code ทีละบรรทัด เรียงตามลำดับที่ pipeline ทำงานจริง
ตั้งแต่ trigger DAG → landing → bronze → silver → gold → MySQL

---

## ลำดับการทำงานทั้งหมด

```
pepe_run_all.py          ← trigger ทุกวันพร้อมกัน
      │
      └─ pepe_daily_pipeline.py  ← ควบคุมลำดับ task ต่อไปนี้
              │
              ├── 1. check_landing_file     (Python)
              ├── 2. ingest_to_bronze       (Python)
              ├── 3. bronze_to_silver       (Spark)
              ├── 4. compute_ohlcv          (Spark)
              ├── 5a. detect_pump_dump      (Spark) ─┐ parallel
              ├── 5b. detect_wash_trade     (Spark) ─┘
              ├── 6. load_gold_to_mysql     (Python)
              └── 7. validate_dw_counts     (Python)
```

---

## Step 0 — Trigger: `dags/pepe_run_all.py`

ไฟล์นี้ไม่ได้ process data แต่เป็น utility สำหรับ scan หา ZIP แล้ว trigger pipeline ทีละวัน

### บรรทัด 23

```python
DATA_ROOT = "/opt/airflow/data"
```

path ของ data/ ภายใน Docker container (mount มาจาก `./data` บน host)

### บรรทัด 26–44: ฟังก์ชัน `scan_landing_files`

```python
pattern = os.path.join(DATA_ROOT, "landing", "PEPEUSDT-trades-*.zip")
paths = sorted(glob.glob(pattern))
```

**บรรทัด 27** — สร้าง pattern สำหรับ glob: `/opt/airflow/data/landing/PEPEUSDT-trades-*.zip`
ดาว `*` match กับวันที่ใดก็ได้

**บรรทัด 28** — `glob.glob` หาไฟล์ทั้งหมดที่ match pattern แล้ว `sorted` เรียงตาม date string

**บรรทัด 36–40** — loop แกะชื่อ date ออกจากชื่อไฟล์:

```python
filename = os.path.basename(path)       # "PEPEUSDT-trades-2026-04-13.zip"
date = filename.replace("PEPEUSDT-trades-", "").replace(".zip", "")  # "2026-04-13"
```

**บรรทัด 43** — `xcom_push` เก็บ list ของ dates ไว้ใน Airflow XCom เพื่อส่งต่อให้ task ถัดไป

### บรรทัด 47–68: ฟังก์ชัน `trigger_all_dates`

**บรรทัด 51–53** — `xcom_pull` ดึง dates list ที่ task ก่อนหน้าเก็บไว้

**บรรทัด 55–66** — loop สร้าง run_id ไม่ซ้ำกัน แล้วเรียก Airflow CLI:

```python
subprocess.run([
    "airflow", "dags", "trigger",
    "pepe_daily_pipeline",
    "--run-id", run_id,
    "--conf", json.dumps({"ds": ds}),   # ส่งวันที่เข้าไปใน pipeline
], check=True)
```

`--conf {"ds": "2026-04-13"}` คือวิธีบอก pipeline ว่าต้องประมวลผลวันไหน

### บรรทัด 71–98: DAG definition

**บรรทัด 75** — `schedule=None` หมายความว่าไม่รันอัตโนมัติ ต้อง trigger เองเสมอ

**บรรทัด 98** — กำหนดลำดับ task:

```python
start >> t_scan >> t_trigger >> end
```

---

## Step 1 — check_landing_file: `dags/pepe_daily_pipeline.py`

### บรรทัด 1–30: imports และ constants

```python
DATA_ROOT      = "/opt/airflow/data"
SPARK_JOBS_DIR = "/spark_jobs"
```

**บรรทัด 25–26** — กำหนด path หลัก 2 ค่า ใช้ตลอดทั้งไฟล์

### บรรทัด 32–44: helper functions สร้าง path

```python
def _landing_path(ds): return f"{DATA_ROOT}/landing/PEPEUSDT-trades-{ds}.zip"
def _bronze_path(ds):  return f"{DATA_ROOT}/bronze/trades/trade_date={ds}/data.parquet"
def _silver_path(ds):  return f"{DATA_ROOT}/silver/trades/processed_date={ds}"
```

**บรรทัด 38–49** — แต่ละฟังก์ชันรับ `ds` (date string) แล้วคืน path เต็มของแต่ละ layer
เขียนแยกไว้เพื่อให้แก้ path ที่เดียวแล้วมีผลทุกที่

### บรรทัด 52–70: ฟังก์ชัน `check_landing_file`

```python
def check_landing_file(**context) -> str:
    ds = _get_ds(context)
    path = _landing_path(ds)
    if not os.path.exists(path):
        raise FileNotFoundError(...)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    return path
```

**บรรทัด 55** — `_get_ds(context)` อ่าน `ds` จาก DAG run config ที่ส่งมาตอน trigger
**บรรทัด 58** — `os.path.exists` เช็คว่าไฟล์ ZIP อยู่จริงในระบบไฟล์หรือไม่
**บรรทัด 59–63** — ถ้าไม่มีไฟล์ raise error ทันที พร้อมบอก URL ให้ไปโหลด
**บรรทัด 64** — ถ้ามีไฟล์ คำนวณขนาดและ print ออกมาใน log

---

## Step 2 — ingest_to_bronze: `dags/pepe_daily_pipeline.py`

### บรรทัด 82–98: column names และฟังก์ชัน

```python
BRONZE_COLUMNS = [
    "trade_id", "price", "qty", "quote_qty",
    "time", "is_buyer_maker", "is_best_match",
]
```

**บรรทัด 82–88** — กำหนดชื่อ column เองเพราะ CSV จาก Binance ไม่มี header row

### บรรทัด 100–134: ฟังก์ชัน `ingest_to_bronze`

**บรรทัด 111–118: เปิด ZIP และอ่าน CSV**

```python
with zipfile.ZipFile(landing, "r") as zf:
    csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
    csv_name = csv_names[0]
    with zf.open(csv_name) as csv_file:
        df = pd.read_csv(csv_file, header=None, names=BRONZE_COLUMNS, index_col=False)
```

**บรรทัด 112** — `zf.namelist()` list ไฟล์ทั้งหมดใน ZIP แล้ว filter เอาแค่ `.csv`
**บรรทัด 118** — `header=None` เพราะ CSV ไม่มีหัวตาราง, `names=BRONZE_COLUMNS` ใส่ชื่อเอง

**บรรทัด 123–125: เพิ่ม metadata columns**

```python
df["_source_file"] = os.path.basename(landing)    # ชื่อไฟล์ ZIP ต้นทาง
df["_ingested_at"] = pd.Timestamp.utcnow()         # เวลาที่ ingest
df["trade_date"]   = ds                            # วันที่ประมวลผล
```

ทั้ง 3 column นี้ไม่ได้มาจาก Binance แต่เพิ่มเพื่อ traceability — รู้ว่า row นี้มาจากไฟล์ไหน ingest เมื่อไหร่

**บรรทัด 127–128: สร้าง folder**

```python
bronze_dir = os.path.dirname(bronze)
os.makedirs(bronze_dir, exist_ok=True)
```

สร้าง directory `data/bronze/trades/trade_date=2026-04-13/` ถ้ายังไม่มี
`exist_ok=True` ไม่ error ถ้า folder มีอยู่แล้ว

**บรรทัด 130: แปลง CSV → Parquet**

```python
pq.write_table(pa.Table.from_pandas(df), bronze)
```

- `pa.Table.from_pandas(df)` — แปลง pandas DataFrame เป็น PyArrow Table
- `pq.write_table(...)` — เขียนออกเป็นไฟล์ `.parquet`

Parquet ดีกว่า CSV เพราะบีบอัดได้, อ่านเร็วกว่า, และเก็บ schema ไว้ด้วย

**บรรทัด 133** — `xcom_push` เก็บ path ของ bronze parquet ไว้ใน XCom ให้ task ถัดไปใช้ได้

---

## Step 3 — bronze_to_silver: `spark_jobs/bronze_to_silver.py`

Airflow เรียก script นี้ผ่าน `spark-submit` ซึ่งรันบน Spark cluster

### บรรทัด 23–26: รับ argument

```python
parser.add_argument("--ds", required=True, help="Processing date YYYY-MM-DD")
```

**บรรทัด 25** — รับ `--ds` เป็น argument บังคับ เช่น `spark-submit bronze_to_silver.py --ds 2026-04-13`

### บรรทัด 35–40: สร้าง Spark Session

```python
spark = SparkSession.builder.appName(f"bronze_to_silver_{ds}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
```

**บรรทัด 36–38** — สร้าง Spark Session เชื่อมต่อกับ Spark master ที่ตั้งไว้ใน config
**บรรทัด 40** — ลด log level เป็น WARN เพื่อไม่ให้ INFO log ท่วม output

### บรรทัด 46–50: อ่าน Bronze

```python
bronze_path = f"/data/bronze/trades/trade_date={ds}/data.parquet"
df = spark.read.parquet(bronze_path)
```

**บรรทัด 46** — path ใช้ `/data/` ไม่ใช่ `/opt/airflow/data/` เพราะ Spark worker mount volume ต่างกัน
**บรรทัด 49** — `spark.read.parquet` อ่าน Parquet เข้ามาเป็น Spark DataFrame (distributed)

### บรรทัด 55–62: Cast Types

```python
df = (
    df
    .withColumn("price",          F.col("price").cast(DoubleType()))
    .withColumn("qty",            F.col("qty").cast(DoubleType()))
    .withColumn("quote_qty",      F.col("quote_qty").cast(DoubleType()))
    .withColumn("time",           F.col("time").cast(LongType()))
    .withColumn("is_buyer_maker", F.col("is_buyer_maker").cast(BooleanType()))
)
```

`withColumn` ใน Spark คือการสร้าง column ใหม่ทับ column เดิม (immutable transformation)
ต้อง cast เพราะ Parquet จาก pandas อาจ infer type ไม่ตรงกับที่ต้องการ เช่น `time` อาจเป็น float แทน long

### บรรทัด 67–70: แปลง Timestamp

```python
df = df.withColumn(
    "trade_time",
    F.to_timestamp(F.col("time") / 1_000_000)
)
```

**บรรทัด 69** — `time` เป็น epoch **microseconds** (เช่น 1776038400509350)
หาร 1,000,000 → ได้ epoch **seconds** (1776038400.50935)
`to_timestamp()` แปลงเป็น DateTime อ่านได้ (2026-04-13 00:00:00.509)

### บรรทัด 75–76: Data Quality Checks

```python
run_all_checks(df, ds=ds)
```

เรียก `dq/quality_checks.py` ดูรายละเอียดใน Step 3a ด้านล่าง

### บรรทัด 81–103: Filter ข้อมูลเสีย

**บรรทัด 82–85:**

```python
before_price_filter = df.count()
df = df.filter(F.col("price") > 0)
after_price_filter = df.count()
removed_price = before_price_filter - after_price_filter
```

นับจำนวน rows ก่อนและหลัง filter เพื่อ log ว่าลบออกกี่ row — ทำแบบนี้ 4 รอบสำหรับ:
- price ≤ 0
- qty ≤ 0
- timestamp นอกวัน (`trade_time.cast("date") != ds`)
- duplicate trade_id (`dropDuplicates(["trade_id"])`)

### บรรทัด 108–115: เพิ่ม Derived Columns

```python
df = (
    df
    .withColumn("trade_hour",     F.hour(F.col("trade_time")))       # ชั่วโมง 0-23
    .withColumn("trade_minute",   F.minute(F.col("trade_time")))     # นาที 0-59
    .withColumn("is_sell",        F.col("is_buyer_maker"))            # True = sell pressure
    .withColumn("usd_value",      F.col("qty") * F.col("price"))     # มูลค่า USDT
    .withColumn("processed_date", F.lit(ds).cast(DateType()))        # วันที่ process
)
```

**บรรทัด 112** — `is_sell = is_buyer_maker` เพราะใน Binance ถ้า `is_buyer_maker = True` แปลว่า buyer เปิด order ก่อน = เป็นฝั่ง maker = ฝั่ง sell เป็น taker
**บรรทัด 113** — `usd_value` คำนวณมูลค่าจริงเป็น USDT ใช้ใน OHLCV volume

### บรรทัด 123: เขียน Silver

```python
df.write.mode("overwrite").parquet(silver_path)
```

`mode("overwrite")` ทำให้ run ซ้ำได้โดยไม่ duplicate — ถ้ามีข้อมูลวันนั้นอยู่แล้วจะเขียนทับ

---

## Step 3a — Data Quality: `dq/quality_checks.py`

ถูกเรียกจาก `bronze_to_silver.py` บรรทัด 76

### บรรทัด 18–37: `check_row_count`

```python
def check_row_count(df, table_name, min_rows=100_000):
    count = df.count()
    if count == 0:
        raise ValueError(f"[DQ CRITICAL] {table_name}: DataFrame is EMPTY")
    if count < min_rows:
        logger.warning(...)
```

**บรรทัด 24** — ถ้าไม่มีข้อมูลเลย raise error หยุด pipeline ทันที
**บรรทัด 26** — ถ้าน้อยกว่า 100,000 rows แค่ warning (บางวัน PEPE อาจมี volume น้อย)

### บรรทัด 40–57: `check_price_validity`

```python
invalid = df.filter(F.col(price_col).isNull() | (F.col(price_col) <= 0)).count()
```

**บรรทัด 44** — นับ rows ที่ price เป็น NULL หรือ ≤ 0 ซึ่งไม่มีความหมายทางการเงิน

### บรรทัด 86–130: `run_all_checks`

```python
if invalid_prices / total_rows > 0.01:
    raise ValueError("[DQ CRITICAL] Price invalidity rate exceeds 1%")
```

**บรรทัด 104–107** — ถ้า invalid price มากกว่า 1% ของทั้งหมด ถือว่าข้อมูลเสีย หยุด pipeline
ทำเช่นเดียวกันกับ qty (1%) และ timestamp out-of-range (5%)

---

## Step 4 — compute_ohlcv: `spark_jobs/compute_ohlcv.py`

### บรรทัด 46–50: อ่าน Silver

```python
silver_path = f"/data/silver/trades/processed_date={ds}"
df = spark.read.parquet(silver_path)
```

อ่าน silver parquet ที่ผ่าน cleaning แล้วมาจาก step 3

### บรรทัด 56–60: สร้าง 5-minute Windows

```python
windowed = df.groupBy(
    F.window(F.col("trade_time"), "5 minutes")
)
```

**บรรทัด 57** — `F.window()` ของ Spark จัด trades แต่ละ row เข้า bucket 5 นาทีอัตโนมัติ
เช่น trades ระหว่าง 00:00:00–00:04:59 รวมอยู่ใน window เดียวกัน

### บรรทัด 63–74: Aggregate OHLCV

```python
ohlcv = windowed.agg(
    F.first(F.col("price")).alias("open"),
    F.max(F.col("price")).alias("high"),
    F.min(F.col("price")).alias("low"),
    F.last(F.col("price")).alias("close"),
    F.sum(F.col("qty")).alias("volume_pepe"),
    F.sum(F.col("usd_value")).alias("volume_usdt"),
    F.count(F.col("trade_id")).alias("trade_count"),
    F.count(F.when(F.col("is_sell") == False, ...)).alias("buyer_initiated_count"),
    F.count(F.when(F.col("is_sell") == True, ...)).alias("seller_initiated_count"),
)
```

**บรรทัด 64** — `first(price)` = ราคา transaction แรกใน window (Open)
**บรรทัด 65** — `max(price)` = ราคาสูงสุด (High)
**บรรทัด 66** — `min(price)` = ราคาต่ำสุด (Low)
**บรรทัด 67** — `last(price)` = ราคา transaction สุดท้าย (Close)
**บรรทัด 73–74** — แยกนับ buy กับ sell แยกกันโดยใช้ `F.when()` เป็น conditional count

### บรรทัด 77–86: Flatten Window struct และเพิ่ม buy_sell_ratio

```python
ohlcv = (
    ohlcv
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
    .withColumn("buy_sell_ratio", F.col("buyer_initiated_count") / F.col("trade_count"))
)
```

**บรรทัด 79–80** — `F.window()` คืน struct `{start, end}` ต้องแตกออกเป็น 2 column แยก
**บรรทัด 84** — `buy_sell_ratio > 0.5` = buy มากกว่า sell ใน window นั้น

---

## Step 5a — detect_pump_dump: `spark_jobs/detect_pump_dump.py`

### บรรทัด 18–20: Thresholds

```python
PUMP_THRESHOLD    = 1.5   # % rise จาก rolling 30m low
DUMP_THRESHOLD    = -1.0  # % drop จาก peak ใน 60m ถัดไป
LOOKBACK_CANDLES  = 6     # 6 × 5min = 30 min
LOOKAHEAD_CANDLES = 12    # 12 × 5min = 60 min
```

ค่าเหล่านี้ calibrate มาจากข้อมูลจริงของ PEPE ที่ p90 ของการขยับปกติอยู่ที่ ~0.9% ต่อ candle

### บรรทัด 57–66: Rolling 30-min Low

```python
w_lookback = (
    Window.orderBy("window_start")
    .rowsBetween(-LOOKBACK_CANDLES, -1)
)
df = df.withColumn("rolling_low_30m",    F.min("close").over(w_lookback))
df = df.withColumn("rolling_avg_vol_30m", F.avg("volume_usdt").over(w_lookback))
```

**บรรทัด 58–60** — `rowsBetween(-6, -1)` หมายถึงมองย้อนหลัง 6 rows (= 30 นาที) ไม่รวม row ปัจจุบัน
**บรรทัด 62** — หา `min(close)` ใน 6 candle ที่ผ่านมา = จุดต่ำสุดล่าสุด
**บรรทัด 63** — หา `avg(volume_usdt)` ใน 6 candle ที่ผ่านมา = volume ปกติ

### บรรทัด 68–75: คำนวณ rise_from_low_pct

```python
df = df.withColumn(
    "rise_from_low_pct",
    F.when(
        F.col("rolling_low_30m").isNotNull() & (F.col("rolling_low_30m") > 0),
        (F.col("close") - F.col("rolling_low_30m")) / F.col("rolling_low_30m") * 100
    ).otherwise(F.lit(None).cast("double"))
)
```

**บรรทัด 71** — `F.when(...).otherwise(None)` คือ if-else ใน Spark
ถ้า rolling_low มีค่า → คำนวณ % เพิ่มจาก low
ถ้าไม่มี (candle แรกๆ ที่ไม่มี 6 candle ก่อนหน้า) → ใส่ NULL

### บรรทัด 81–86: หา Pump Peaks

```python
pump_candles = df.filter(
    (F.col("rise_from_low_pct") >= PUMP_THRESHOLD)
    & (F.col("volume_usdt") >= F.col("rolling_avg_vol_30m") * 2.0)
)
```

**บรรทัด 82** — ราคาขึ้นจาก 30m low อย่างน้อย 1.5%
**บรรทัด 83** — volume candle นั้นต้องมากกว่า average 2 เท่า (มีแรงซื้อผิดปกติ)
ทั้งสองเงื่อนไขต้องผ่านพร้อมกัน

### บรรทัด 97–118: Join กับ Future Candles

```python
pump_alias  = pump_candles.select(
    F.col("row_num").alias("pump_row"), ...)

future_alias = df.select(
    F.col("row_num").alias("future_row"),
    F.col("close").alias("future_close"))

events = pump_alias.join(
    future_alias,
    (future_alias["future_row"] > pump_alias["pump_row"])
    & (future_alias["future_row"] <= pump_alias["pump_row"] + LOOKAHEAD_CANDLES)
)
```

**บรรทัด 115–116** — join เงื่อนไข: เอา rows ที่อยู่ "หลัง pump" และ "ไม่เกิน 12 candles ถัดไป"
ได้ทุก candle ใน 60 นาทีหลัง pump แต่ละจุด

### บรรทัด 120–129: หา Dump

```python
events = events.groupBy(...).agg(
    F.min("future_close").alias("price_after_dump")
)

events = events.withColumn(
    "dump_pct",
    (F.col("price_after_dump") - F.col("price_at_peak")) / F.col("price_at_peak") * 100
)
events = events.filter(F.col("dump_pct") <= DUMP_THRESHOLD)
```

**บรรทัด 121** — `min(future_close)` หาราคาต่ำสุดใน 60 นาทีหลัง pump
**บรรทัด 125** — คำนวณว่าราคาลงจาก peak กี่ % (จะได้ค่าติดลบ)
**บรรทัด 128** — เก็บเฉพาะ events ที่ dump มากกว่า -1.0%

### บรรทัด 132–144: กำหนด Severity

```python
.withColumn(
    "severity",
    F.when(F.col("pump_pct") >= 2.5, F.lit("HIGH"))
     .when(F.col("pump_pct") >= 1.5, F.lit("MEDIUM"))
     .otherwise(F.lit("LOW"))
)
```

**บรรทัด 134–137** — `F.when().when().otherwise()` คือ if-elif-else ใน Spark
pump ≥ 2.5% = HIGH, pump ≥ 1.5% = MEDIUM

---

## Step 5b — detect_wash_trade: `spark_jobs/detect_wash_trade.py`

### บรรทัด 54–57: เพิ่ม Time Bucket

```python
df = df.withColumn(
    "time_bucket",
    (F.unix_timestamp(F.col("trade_time"))).cast("long")
)
```

**บรรทัด 56** — `unix_timestamp()` แปลง DateTime → epoch seconds (ทศนิยมถูกตัดทิ้ง)
ทุก trade ที่เกิดใน **วินาทีเดียวกัน** จะมี `time_bucket` เดียวกัน

### บรรทัด 62–76: แยก Buys และ Sells

```python
buys = df.filter(F.col("is_sell") == False).select(
    F.col("trade_id").alias("buy_trade_id"),
    F.col("price").alias("buy_price"),
    F.col("qty").alias("buy_qty"),
    F.col("time").alias("buy_time"),
    F.col("time_bucket").alias("buy_bucket"),
)
```

สร้าง DataFrame 2 ชุด — `buys` และ `sells` — พร้อม rename columns เพื่อไม่ชน column name ตอน join

### บรรทัด 85: Join ใน Bucket เดียวกัน

```python
pairs = buys.join(sells, buys["buy_bucket"] == sells["sell_bucket"])
```

join เฉพาะ buy–sell ที่อยู่ใน second เดียวกัน แทนที่จะ join ทั้งหมด
ลด complexity จาก O(n²) เป็น O(k²) โดย k = trades ใน 1 second (น้อยกว่ามาก)

### บรรทัด 90–105: กรอง 3 เงื่อนไข

```python
pairs = pairs.withColumn(
    "time_diff_ms",
    (F.abs(F.col("buy_time") - F.col("sell_time")) / 1000).cast("long")
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
```

**บรรทัด 92** — `buy_time` และ `sell_time` เป็น microseconds ดังนั้น หาร 1000 → milliseconds
**บรรทัด 95** — price_diff คิดเป็น % เทียบกับ buy price
**บรรทัด 98** — qty_similarity = 100% ถ้าจำนวนเท่ากันพอดี, ลดลงตามความต่าง

### บรรทัด 113–127: คำนวณ wash_score

```python
pairs = pairs.withColumn(
    "time_score",  F.lit(1) - (F.col("time_diff_ms") / 1000)
).withColumn(
    "price_score", F.lit(1) - (F.col("price_diff_pct") / 0.1)
).withColumn(
    "qty_score",   F.col("qty_similarity_pct") / 100
).withColumn(
    "wash_score",
    F.col("time_score")  * 0.40
    + F.col("price_score") * 0.35
    + F.col("qty_score")   * 0.25
)
```

**บรรทัด 115** — `time_score`: ถ้า time_diff = 0ms → score = 1.0, ถ้า 999ms → score = 0.001
**บรรทัด 118** — `price_score`: ถ้า diff = 0% → score = 1.0, ถ้า 0.099% → score ≈ 0.01
**บรรทัด 124–127** — weighted sum: time สำคัญสุด (40%), price รองมา (35%), qty น้อยสุด (25%)

### บรรทัด 132: Filter score

```python
pairs = pairs.filter(F.col("wash_score") >= 0.8)
```

เก็บเฉพาะคู่ที่มีความน่าสงสัยสูง (score ≥ 0.8 จาก 1.0)

---

## Step 6 — load_gold_to_mysql: `dags/pepe_daily_pipeline.py`

### บรรทัด 140–154: เชื่อมต่อ MySQL

```python
engine = create_engine(
    f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}",
    pool_pre_ping=True,
)
```

**บรรทัด 151–154** — SQLAlchemy engine เชื่อมต่อ MySQL
`pool_pre_ping=True` ทดสอบ connection ก่อนใช้งาน ป้องกัน stale connection error

### บรรทัด 156–179: Upsert dim_date

```python
conn.execute(text("""
    INSERT IGNORE INTO dim_date (date_id, full_date, year, month, ...)
    VALUES (:date_id, :full_date, :year, ...)
"""), {...})
```

**บรรทัด 165** — `INSERT IGNORE` ถ้าวันนั้นมีอยู่แล้วใน dim_date จะ skip ไม่ error

### บรรทัด 188–211: โหลด fact_trades

```python
with engine.begin() as conn:
    conn.execute(
        text("DELETE FROM fact_trades WHERE processed_date = :ds"),
        {"ds": ds}
    )
df.to_sql("fact_trades", engine, if_exists="append", index=False, chunksize=5000)
```

**บรรทัด 204–208** — DELETE ก่อน แล้ว INSERT ใหม่ = idempotent (run ซ้ำกี่ครั้งก็ได้ผลเหมือนกัน)
**บรรทัด 210** — `chunksize=5000` ส่ง rows เป็น batch ละ 5,000 ไม่ให้ query ใหญ่เกินไป

### บรรทัด 214–237 และ 240–263: โหลด pump_dump และ wash_trade

```python
try:
    df = pd.read_parquet(gold_path)
except Exception as exc:
    return  # ถ้าไม่มีไฟล์ skip เฉยๆ

if df.empty:
    return  # ถ้าไม่มี event ก็ skip ไม่ error
```

**บรรทัด 220–224** — ถ้าไม่มีไฟล์ (วันนั้นยังไม่ถูก process) → return เฉยๆ
**บรรทัด 226–228** — ถ้า parquet ว่าง (ไม่มี event ตรวจพบ) → return เฉยๆ ไม่ถือว่าผิดพลาด

---

## Step 7 — validate_dw_counts: `dags/pepe_daily_pipeline.py`

### บรรทัด 284–296: ตรวจ row counts

```python
checks = {
    "fact_trades":           "SELECT COUNT(*) FROM fact_trades WHERE processed_date = :ds",
    "fact_pump_dump_events": "SELECT COUNT(*) FROM fact_pump_dump_events WHERE trade_date = :ds",
    "fact_wash_trade_pairs": "SELECT COUNT(*) FROM fact_wash_trade_pairs WHERE trade_date = :ds",
}

for table, query in checks.items():
    count = conn.execute(text(query), {"ds": ds}).scalar()
    if table == "fact_trades" and count == 0:
        failed_tables.append(table)
```

**บรรทัด 284–288** — ทำ dictionary mapping table name → SQL query
**บรรทัด 293** — `.scalar()` ดึงค่าตัวเลขเดียวออกมาจาก COUNT(*)
**บรรทัด 295** — เช็คเฉพาะ `fact_trades` เพราะ pump/wash อาจมี 0 rows ได้ถ้าวันนั้นไม่มี event

### บรรทัด 298–304: Fail ถ้า fact_trades ว่าง

```python
if failed_tables:
    raise ValueError(
        f"[validate_dw_counts] CRITICAL: the following tables have 0 rows for {ds}: "
        + ", ".join(failed_tables)
    )
```

ถ้า fact_trades มี 0 rows แปลว่า load ล้มเหลว → raise error → Airflow จะ retry task นี้อัตโนมัติ

---

## Step สุดท้าย — DAG Flow definition: `dags/pepe_daily_pipeline.py` บรรทัด 397–407

```python
(
    start
    >> t_check_landing
    >> t_ingest_bronze
    >> t_bronze_to_silver
    >> t_compute_ohlcv
    >> [t_detect_pump_dump, t_detect_wash_trade]   # รันคู่กัน (parallel)
    >> t_load_mysql
    >> t_validate
    >> end
)
```

**บรรทัด 403** — `[t_detect_pump_dump, t_detect_wash_trade]` ใน list = รัน parallel พร้อมกัน
ทั้งสองอ่าน silver layer คนละ partition ไม่ blocking กัน ประหยัดเวลา
**บรรทัด 404** — `t_load_mysql` รอให้ทั้งสอง task ใน list เสร็จก่อนจึงเริ่มทำงาน

---

## Spark Memory Config: `dags/pepe_daily_pipeline.py` บรรทัด 342–348

```python
_spark_conf = {
    "spark.master": "spark://spark-master:7077",
    "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python3.11",
    "spark.driver.memory": "512m",
    "spark.executor.memory": "800m",
    "spark.executor.cores": "1",
}
```

**บรรทัด 345–347** — จำกัด memory ไว้เพราะ Docker บนเครื่องนี้มี RAM จำกัด (3.8 GB รวมทุก container)
ถ้าไม่ตั้งค่า Spark จะขอ memory เกิน → OS kill process ด้วย error code -9

config นี้ใช้กับทุก SparkSubmitOperator ทั้ง 4 ตัว (bronze_to_silver, compute_ohlcv, detect_pump_dump, detect_wash_trade)

---

## ทำไมถึงเลือกแบบนี้? — เหตุผลเบื้องหลังทุก Design Decision

---

### pepe_run_all.py — ทำไมต้องมี DAG แยกสำหรับ trigger?

**ปัญหา:** pipeline ต้อง process หลายวันพร้อมกัน (backfill 8 วัน)
ถ้าใช้ DAG เดียวและ trigger ด้วยมือทีละวัน = ต้องกด 8 ครั้ง

**ทางเลือกที่ไม่เลือก:**
- `schedule="@daily"` + `catchup=True` — Airflow จะ run ย้อนหลังอัตโนมัติ แต่ควบคุมลำดับและ error handling ได้ยากกว่า
- Loop ใน DAG เดียว — Airflow ไม่ได้ออกแบบมาให้ DAG หนึ่ง process หลาย date ใน run เดียว

**ที่เลือก:** `pepe_run_all.py` scan ZIP แล้ว trigger `pepe_daily_pipeline` ทีละวัน ทำให้แต่ละวันมี run_id แยก, log แยก, retry แยกกันได้อิสระ

---

### pepe_daily_pipeline.py — ทำไม `schedule=None`?

```python
schedule=None
```

Pipeline นี้ไม่มี schedule เพราะ **raw data ต้องดาวน์โหลดจาก Binance เองก่อน** ถึงจะรันได้ ถ้าตั้ง `schedule="@daily"` Airflow จะ trigger อัตโนมัติแม้ไฟล์ยังไม่มี → task แรกจะ fail ทุกวัน

---

### pepe_daily_pipeline.py — ทำไม `retries=2, retry_delay=10min`?

```python
"retries": 2,
"retry_delay": timedelta(minutes=10),
```

Spark jobs บน Docker อาจ fail เพราะ resource contention (RAM เต็มชั่วคราว) ไม่ใช่ bug ใน code — retry อัตโนมัติช่วยให้ผ่านได้โดยไม่ต้องแทรกแซงด้วยมือ
10 นาทีเพียงพอให้ container คืน memory ก่อน retry

---

### pepe_daily_pipeline.py — ทำไม `max_active_runs=1`?

```python
max_active_runs=1,
```

Spark worker มี memory จำกัด (800m) ถ้ารัน 2 วันพร้อมกัน Spark จะแย่ง memory กัน → OOM → ทั้งคู่ fail
จำกัดไว้ที่ 1 active run = รันทีละวัน เสร็จแล้วค่อยรันวันถัดไป

---

### pepe_daily_pipeline.py — ทำไม helper functions `_landing_path`, `_bronze_path`, ...?

```python
def _landing_path(ds): return f"{DATA_ROOT}/landing/PEPEUSDT-trades-{ds}.zip"
def _bronze_path(ds):  return f"{DATA_ROOT}/bronze/trades/trade_date={ds}/data.parquet"
```

Path ของแต่ละ layer ถูกใช้ใน **หลาย task** (check → ingest → load → validate)
ถ้าเขียน path inline ทุกที่ → แก้ชื่อครั้งเดียวต้องไล่แก้ทุก task
รวม path ไว้ที่เดียว → แก้ที่เดียวมีผลทุกที่

---

### pepe_daily_pipeline.py — ทำไม `PythonOperator` สำหรับ landing/bronze แต่ `SparkSubmitOperator` สำหรับ silver/gold?

| Task | Operator | เหตุผล |
|---|---|---|
| check_landing, ingest_to_bronze, load_mysql, validate | `PythonOperator` | งานเล็ก: เช็คไฟล์, อ่าน CSV, write MySQL — ไม่ต้องการ distributed compute |
| bronze_to_silver, compute_ohlcv, detect_* | `SparkSubmitOperator` | งานหนัก: process ข้อมูลล้าน rows, rolling window, cross-join — ต้องการ Spark |

การใช้ Spark กับงานเล็กจะเสีย overhead ในการสร้าง SparkSession โดยไม่จำเป็น

---

### pepe_daily_pipeline.py — ทำไม `pool_pre_ping=True`?

```python
engine = create_engine(..., pool_pre_ping=True)
```

SQLAlchemy มี connection pool — connection ที่ idle นาน ๆ อาจถูก MySQL ปิดฝั่ง server แต่ pool ยังคิดว่า connection ยังอยู่
`pool_pre_ping=True` ping connection ก่อนใช้งานทุกครั้ง ถ้าตาย → สร้างใหม่อัตโนมัติ
หากไม่ใส่ → `MySQL server has gone away` error ที่หาสาเหตุยาก

---

### pepe_daily_pipeline.py — ทำไม `INSERT IGNORE` สำหรับ dim_date แต่ `DELETE + INSERT` สำหรับ fact tables?

**dim_date:**
```python
INSERT IGNORE INTO dim_date ...
```
วันที่ไม่เปลี่ยน — 2026-04-13 จะเป็น วันอาทิตย์เสมอ ถ้ามีอยู่แล้ว skip ได้เลย

**fact tables:**
```python
DELETE FROM fact_trades WHERE processed_date = :ds
# แล้วค่อย INSERT ใหม่
```
ข้อมูล trade อาจแก้ไขได้ถ้าพบ bug แล้ว re-process — DELETE ก่อนทำให้ run ซ้ำได้โดยไม่ duplicate
เรียกว่า **idempotent** = รันกี่ครั้งผลลัพธ์เหมือนเดิมเสมอ

---

### pepe_daily_pipeline.py — ทำไม `chunksize=5000` ใน `to_sql`?

```python
df.to_sql("fact_trades", engine, if_exists="append", index=False, chunksize=5000)
```

fact_trades มีข้อมูล ~1 ล้าน rows ต่อวัน ถ้า INSERT ทั้งหมดใน query เดียว → MySQL packet size เกิน limit (`max_allowed_packet`)
5,000 rows ต่อ batch = query ขนาดพอดี ไม่ crash

---

### pepe_daily_pipeline.py — ทำไม validate แค่ `fact_trades` แต่ไม่ validate pump/wash?

```python
if table == "fact_trades" and count == 0:
    failed_tables.append(table)
```

`fact_trades` ต้องมีข้อมูลเสมอ — ถ้า 0 rows แปลว่า load ล้มเหลวแน่นอน
แต่ `fact_pump_dump_events` และ `fact_wash_trade_pairs` อาจมี 0 rows ได้ถ้าวันนั้นไม่มี event — ไม่ถือเป็น error

---

### bronze_to_silver.py — ทำไมต้อง Cast Types ทั้งที่ parquet มี schema อยู่แล้ว?

```python
.withColumn("price", F.col("price").cast(DoubleType()))
.withColumn("time",  F.col("time").cast(LongType()))
```

pandas `read_csv` + `write_parquet` อาจ infer type ผิด เช่น:
- `time` (เลข 16 หลัก) → pandas อาจ infer เป็น `float64` แทน `int64` → เกิด precision loss
- `is_buyer_maker` อ่านเป็น string `"True"/"False"` → ต้อง cast เป็น `BooleanType` เอง

Cast ใน Spark step นี้คือ **contract** ว่า silver layer มี schema ที่เชื่อถือได้แน่นอน

---

### bronze_to_silver.py — ทำไมหาร 1,000,000 ก่อน `to_timestamp`?

```python
F.to_timestamp(F.col("time") / 1_000_000)
```

Binance ส่ง `time` หน่วย **microseconds** (1 วินาที = 1,000,000 microseconds)
`to_timestamp()` รับหน่วย **seconds** → ต้องหารก่อน
ถ้าไม่หาร → ได้ปี ค.ศ. ประมาณ 58,000 (ไม่ใช่ 2026)

---

### bronze_to_silver.py — ทำไมรัน DQ checks **ก่อน** filter ข้อมูลเสีย?

```python
run_all_checks(df, ds=ds)   # บรรทัด 76
# แล้วค่อย filter...
df = df.filter(F.col("price") > 0)  # บรรทัด 82
```

DQ check วัดคุณภาพของ **raw bronze data** — ถ้า filter ก่อนแล้วค่อย check จะไม่รู้ว่าต้นทางเสียแค่ไหน
ถ้า bronze มี invalid price > 1% → หยุดทันที ไม่ต้องเสียเวลา process ต่อ

---

### bronze_to_silver.py — ทำไมนับ rows ก่อนและหลัง filter ทุกขั้น?

```python
before_price_filter = df.count()
df = df.filter(...)
after_price_filter = df.count()
print(f"Rows removed: {before_price_filter - after_price_filter}")
```

เพื่อ **observability** — ถ้า pipeline fail หรือผลลัพธ์ผิด สามารถดู log แล้วรู้ทันทีว่า data หายไปที่ step ไหนกี่ rows

---

### compute_ohlcv.py — ทำไม 5 นาที? ไม่ใช่ 1 นาทีหรือ 1 ชั่วโมง?

```python
F.window(F.col("trade_time"), "5 minutes")
```

- **1 นาที** — noise เยอะ สัญญาณ pump/dump ไม่ชัด
- **5 นาที** — มาตรฐานของ technical analysis crypto ส่วนใหญ่, ชัดพอที่จะเห็น pattern แต่ไม่ smooth จนซ่อน event
- **1 ชั่วโมง** — หยาบเกินไป หาเวลาที่ pump เกิดขึ้นไม่ได้

---

### compute_ohlcv.py — ทำไม `first`/`last` สำหรับ open/close แทน order by timestamp?

```python
F.first(F.col("price")).alias("open"),
F.last(F.col("price")).alias("close"),
```

`F.first`/`F.last` ใน Spark ไม่รับประกันลำดับ (non-deterministic) แต่ยอมรับได้เพราะ:
- PEPE มี trades หลายแสน rows ต่อวัน → การ orderBy ใน window ทุก candle = expensive มาก
- ความต่างของ open/close จาก trade แรก/สุดท้ายใน window 5 นาที มีผลน้อยมากในทางปฏิบัติ
- ใช้ OHLCV เพื่อ detect pump pattern ไม่ใช่ trading จริง precision ระดับนี้เพียงพอ

---

### compute_ohlcv.py — ทำไมต้องมี `buy_sell_ratio`?

```python
F.col("buyer_initiated_count") / F.col("trade_count")
```

เป็น signal เสริมสำหรับ pump detection:
- pump ที่เกิดจาก manipulation → มักมี `buy_sell_ratio` สูงผิดปกติ (buy pressure ล้นตลาด)
- ใช้ร่วมกับ volume และ price rise เพื่อยืนยัน signal

---

### detect_pump_dump.py — ทำไม rolling window approach แทนการ compare กับ daily average?

หา pump จาก **จุดต่ำสุดล่าสุด 30 นาที** ไม่ใช่ daily average เพราะ:
- daily average ไม่จับ intraday pump ที่เกิดและจบภายในไม่กี่ชั่วโมงได้
- pump มักเริ่มจาก local low → spike ขึ้น → dump กลับ
- rolling 30m low จับ pattern นี้ได้ตรงกว่า

---

### detect_pump_dump.py — ทำไม `row_number` + join แทน Window function ดู lookahead?

```python
df = df.withColumn("row_num", F.row_number().over(w_ordered))
# แล้วค่อย join กับ future_alias
events = pump_alias.join(future_alias,
    (future_alias["future_row"] > pump_alias["pump_row"])
    & (future_alias["future_row"] <= pump_alias["pump_row"] + LOOKAHEAD_CANDLES)
)
```

Spark Window function (`rowsBetween`) ดู **อดีต** ได้ดี แต่ดู **อนาคต** (lookahead) ทำได้ยาก
การใช้ `row_number` + join เป็นวิธีมาตรฐานใน Spark สำหรับ forward-looking operations

---

### detect_wash_trade.py — ทำไม time bucket ก่อน join? ไม่ join โดยตรง?

```python
# แทนที่จะ:
pairs = buys.join(sells)  # cross join O(n²) = หายนะ

# ใช้:
pairs = buys.join(sells, buys["buy_bucket"] == sells["sell_bucket"])
```

PEPE มี trades ~1 ล้าน rows/วัน cross join = 1 ล้าน × 1 ล้าน = 1 **ล้านล้าน** คู่ → OOM แน่นอน
bucket join จับคู่เฉพาะ trades ใน **second เดียวกัน** → ลด search space เหลือ k² โดย k = trades ต่อ 1 second (น้อยกว่ามาก)

---

### detect_wash_trade.py — ทำไม threshold `time_diff < 1000ms, price_diff < 0.1%, qty_similarity > 90%`?

| เงื่อนไข | ค่า | เหตุผล |
|---|---|---|
| `time_diff < 1000ms` | < 1 วินาที | wash trade ต้องเกิดเกือบพร้อมกัน — ต่างกันเกิน 1 วินาทีอาจเป็น coincidence |
| `price_diff < 0.1%` | < 0.1% | ราคาต้องเกือบเท่ากัน — ถ้าต่างมากกว่านี้คือ market movement ปกติ |
| `qty_similarity > 90%` | > 90% | ปริมาณต้องใกล้เคียงกัน — wash trade มักใช้ amount เดิม |

ทั้ง 3 เงื่อนไขต้องผ่านพร้อมกัน เพื่อกรอง false positive ออกให้มากที่สุด

---

### detect_wash_trade.py — ทำไม weight time 40%, price 35%, qty 25%?

```python
"wash_score",
F.col("time_score")  * 0.40
+ F.col("price_score") * 0.35
+ F.col("qty_score")   * 0.25
```

- **Time (40%)** — สำคัญที่สุด: wash trade จริงต้องเกิดแทบพร้อมกัน
- **Price (35%)** — สำคัญมาก: ราคาเหมือนกันบ่งชี้เป็น coordinated order
- **Qty (25%)** — สำคัญน้อยที่สุด: บางครั้ง wash trader แยก order เป็นหลาย lot ขนาดต่างกันเล็กน้อย

weight เหล่านี้สะท้อนว่า **เวลาและราคาคือหลักฐานหลัก** ของ wash trade

---

### dq/quality_checks.py — ทำไมแยกเป็นไฟล์ต่างหาก?

ถ้าเขียน DQ check ไว้ใน `bronze_to_silver.py` โดยตรง:
- ทดสอบแยกไม่ได้
- นำไปใช้กับ pipeline อื่นในอนาคตไม่ได้

การแยกเป็น `dq/quality_checks.py` = **reusable module** ที่ import ได้จากทุก Spark job

---

### dq/quality_checks.py — ทำไม threshold 1% สำหรับ price/qty แต่ 5% สำหรับ timestamp?

```python
if invalid_prices / total_rows > 0.01:   # 1%
    raise ValueError(...)
if out_of_range / total_rows > 0.05:     # 5%
    raise ValueError(...)
```

- **price/qty invalid 1%** — ข้อมูลราคา/ปริมาณเสียเกิน 1% = data source มีปัญหาร้ายแรง ควรหยุด
- **timestamp out-of-range 5%** — Binance daily file บางครั้งมี trades ที่ timestamp ข้ามเที่ยงคืน UTC เล็กน้อย (timezone edge case) → 5% ยืดหยุ่นพอที่จะ tolerate กรณีนี้

---

### sql/init.sql — ทำไมใช้ Star Schema (dim + fact)?

ทางเลือก: เก็บทุกอย่างใน table เดียวใหญ่ ๆ

**ปัญหาของ flat table:** `trade_date` ซ้ำใน every row → query เช่น "วันไหนเป็น weekend" ต้องแปลงใน query ทุกครั้ง

**Star schema:**
- `dim_date` เก็บ attributes ของวัน (weekend/weekday, month, year) ไว้ที่เดียว
- `fact_trades` join กับ `dim_date` → query ทำได้ง่ายและเร็ว

---

### sql/init.sql — ทำไม `DECIMAL(20, 10)` สำหรับราคา ไม่ใช้ `FLOAT`?

```sql
price DECIMAL(20, 10) NOT NULL,
```

ราคา PEPE = `0.00000347` — ถ้าใช้ `FLOAT`:
- IEEE 754 floating point มี precision error: `0.00000347` อาจกลายเป็น `0.000003469999998...`
- ใน financial data ความถูกต้องของตัวเลขสำคัญมาก

`DECIMAL` เก็บตัวเลขแบบ **exact** ไม่มี rounding error

---

### sql/init.sql — ทำไม index บน `trade_time`, `trade_date`, `wash_score`?

```sql
INDEX idx_trade_time (trade_time),
INDEX idx_wash_score (wash_score),
```

query ที่ใช้บ่อยที่สุด:
- ดู trades ช่วงเวลาหนึ่ง → filter `WHERE trade_time BETWEEN ...` → index ช่วย
- กรอง wash trade confidence สูง → filter `WHERE wash_score >= 0.9` → index ช่วย

ไม่มี index → MySQL scan ทุก row ทุกครั้ง → ช้าเมื่อข้อมูลโต

---

### sql/init.sql — ทำไม seed `dim_date` ถึงปี 2029 ตั้งแต่ต้น?

```sql
WHERE DATE('2026-01-01') + INTERVAL seq DAY <= DATE('2029-12-31')
```

`dim_date` ต้องมีข้อมูลอยู่ก่อน **ก่อน** ที่ `fact_trades` จะ insert ได้ (เพราะมี foreign key)
การ pre-seed ถึงปี 2029 = ไม่ต้องมานึกถึงเรื่องนี้อีกหลายปี และไม่ต้อง insert dim_date ทีละวันใน pipeline
