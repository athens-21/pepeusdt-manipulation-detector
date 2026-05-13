# Architecture: PEPEUSDT Pipeline — Data Flow ทุก Layer

ไฟล์นี้อธิบาย data flow ตั้งแต่ต้นทางที่ Binance ไปจนถึงปลายทางที่ MySQL
อธิบายว่าแต่ละ layer ได้รับข้อมูลอะไร ทำอะไรกับมัน และส่งอะไรต่อไป

---

## ภาพรวม Flow

```
Binance Vision (website)
        │  download ZIP file
        ▼
data/landing/              [ZIP files]
        │  unzip + read CSV → write Parquet
        ▼
data/bronze/               [Raw Parquet, no cleaning]
        │  Spark: cast types, DQ checks, filter, enrich
        ▼
data/silver/               [Clean Parquet, enriched columns]
        │  Spark: group by 5-min window
        ▼
data/gold/ohlcv_5min/      [OHLCV candles]
        │  Spark: rolling peak/trough detection
        ▼
data/gold/pump_dump_signals/    [Pump & Dump events]
        │  Spark: time-bucket join + scoring
        ▼
data/gold/wash_trade_signals/   [Wash Trade pairs]
        │  Python: read parquet → INSERT MySQL
        ▼
MySQL pepe_dw              [Data Warehouse, queryable]
```

---

## Layer 1 — Binance (ต้นทาง)

**ที่มา:** https://data.binance.vision/?prefix=data/spot/daily/trades/PEPEUSDT/

Binance เปิดให้ดาวน์โหลด historical trade data ฟรีทุกวัน
แต่ละวันเป็นไฟล์ ZIP 1 ไฟล์ ข้างในมี CSV 1 ไฟล์

**รูปแบบชื่อไฟล์:**

```
PEPEUSDT-trades-YYYY-MM-DD.zip
  └── PEPEUSDT-trades-YYYY-MM-DD.csv
```

**CSV ไม่มี header** มี 7 columns เรียงตามนี้:

| ลำดับ column | ชื่อ | ตัวอย่าง | ความหมาย |
|---|---|---|---|
| 0 | trade_id | 549900448 | ID ของ transaction ไม่ซ้ำกัน |
| 1 | price | 0.000003 | ราคา PEPE ต่อ 1 หน่วย เป็น USDT |
| 2 | qty | 57636887.0 | จำนวน PEPE ที่ซื้อขาย |
| 3 | quote_qty | 199.999998 | มูลค่ารวมเป็น USDT (price × qty) |
| 4 | time | 1776038400509350 | timestamp เป็น epoch microseconds |
| 5 | is_buyer_maker | False | True = seller เป็นฝ่ายเปิด order ก่อน |
| 6 | is_best_match | True | จับคู่ที่ราคาดีที่สุด (เกือบ True ทุก row) |

**ทำไม timestamp เป็น microseconds:** Binance ใช้ precision ระดับ microsecond (1/1,000,000 วินาที)
ต้องหาร 1,000,000 เพื่อแปลงเป็น seconds ก่อนแปลงเป็น datetime

---

## Layer 2 — Landing (`data/landing/`)

**ไฟล์ที่รับผิดชอบ:** `dags/pepe_daily_pipeline.py` → task `check_landing_file`

**วัตถุประสงค์:** เป็น drop zone สำหรับรับไฟล์ ZIP จาก Binance ก่อนที่ pipeline จะเริ่มทำงาน

**สิ่งที่อยู่ใน folder:**

```
data/landing/
├── PEPEUSDT-trades-2026-04-13.zip
├── PEPEUSDT-trades-2026-04-14.zip
├── PEPEUSDT-trades-2026-04-15.zip
├── PEPEUSDT-trades-2026-04-29.zip
├── PEPEUSDT-trades-2026-05-09.zip
├── PEPEUSDT-trades-2026-05-10.zip
├── PEPEUSDT-trades-2026-05-11.zip
└── PEPEUSDT-trades-2026-05-12.zip
```

**task `check_landing_file` ทำอะไร:**

1. รับ `ds` (date string เช่น `2026-04-13`) มาจาก DAG config
2. ต่อ path เป็น `data/landing/PEPEUSDT-trades-2026-04-13.zip`
3. เช็คว่าไฟล์นั้น exist หรือไม่
4. ถ้าไม่มี → raise `FileNotFoundError` และหยุด pipeline ทันที พร้อมบอก URL ให้ไปโหลด
5. ถ้ามี → print ขนาดไฟล์และ pass ต่อ

**ทำไม layer นี้ถึงต้องมี:** เพื่อให้ pipeline fail fast ถ้าไฟล์ยังไม่ถูกวางไว้ แทนที่จะรอไปจนถึง step อื่นแล้วค่อย error ซึ่งเสียเวลากว่า

---

## Layer 3 — Bronze (`data/bronze/`)

**ไฟล์ที่รับผิดชอบ:** `dags/pepe_daily_pipeline.py` → task `ingest_to_bronze`

**วัตถุประสงค์:** แปลง ZIP → CSV → Parquet โดยไม่แก้ไขข้อมูลใดๆ เก็บสภาพดิบที่สุด

**โครงสร้าง folder:**

```
data/bronze/trades/
└── trade_date=2026-04-13/
    └── data.parquet
```

**สิ่งที่ task `ingest_to_bronze` ทำ:**

1. เปิด ZIP file ด้วย Python standard library `zipfile`
2. หา CSV ข้างใน (ชื่อไม่แน่นอน ใช้ glob หา `.csv`)
3. อ่าน CSV ด้วย pandas กำหนด column names เองเพราะไม่มี header:

```python
BRONZE_COLUMNS = [
    "trade_id", "price", "qty", "quote_qty",
    "time", "is_buyer_maker", "is_best_match"
]
```

4. เพิ่ม metadata columns:
   - `_source_file` — ชื่อไฟล์ ZIP ที่มาจาก เพื่อ traceability
   - `_ingested_at` — เวลา UTC ที่ ingest
   - `trade_date` — วันที่ (partition key)
5. เขียนเป็น Parquet ด้วย PyArrow

**Schema ของ Bronze Parquet:**

| Column | Type | หมายเหตุ |
|---|---|---|
| trade_id | int64 | ID transaction |
| price | float64 | ราคา USDT |
| qty | float64 | จำนวน PEPE |
| quote_qty | float64 | มูลค่า USDT |
| time | int64 | epoch microseconds ยังไม่แปลง |
| is_buyer_maker | bool | |
| is_best_match | bool | |
| _source_file | string | ชื่อไฟล์ต้นทาง |
| _ingested_at | datetime[UTC] | เวลา ingest |
| trade_date | string | เช่น "2026-04-13" |

**ตัวอย่าง row จริง:**

```
trade_id    : 549900448
price       : 0.000003
qty         : 57636887.0
quote_qty   : 199.999998
time        : 1776038400509350   ← microseconds ยังไม่แปลง
is_buyer_maker: False
_source_file: PEPEUSDT-trades-2026-04-13.zip
trade_date  : 2026-04-13
```

**ทำไมต้องเก็บ Bronze แยก:** เพื่อมี "raw copy" ที่ไม่เคยถูกแก้ไข ถ้า silver หรือ gold layer มีปัญหาในอนาคต สามารถ re-process ใหม่จาก bronze ได้เลยโดยไม่ต้องดาวน์โหลดซ้ำ

---

## Layer 4 — Silver (`data/silver/`)

**ไฟล์ที่รับผิดชอบ:** `spark_jobs/bronze_to_silver.py` + `dq/quality_checks.py`

**วัตถุประสงค์:** ทำความสะอาดข้อมูล, กรองข้อมูลเสีย, เพิ่ม derived columns ที่ downstream ทุกคนจะใช้

**โครงสร้าง folder:**

```
data/silver/trades/
└── processed_date=2026-04-13/
    └── part-00000-....snappy.parquet
```

### ขั้นตอนที่ 1: Cast Types

Bronze เก็บทุกอย่างไว้แบบ CSV อาจมี type ผิด ต้อง cast ให้ถูก:

```
price          → DoubleType
qty            → DoubleType
quote_qty      → DoubleType
time           → LongType (เตรียมแปลง timestamp)
is_buyer_maker → BooleanType
```

### ขั้นตอนที่ 2: แปลง Timestamp

```python
trade_time = to_timestamp(time / 1_000_000)
```

- `time` ใน bronze คือ epoch **microseconds** (1776038400509350)
- หาร 1,000,000 → epoch **seconds** (1776038400.50935)
- แปลงเป็น DateTime → `2026-04-13 00:00:00.509350`

### ขั้นตอนที่ 3: Data Quality Checks (`dq/quality_checks.py`)

รัน 5 checks ก่อน filter:

| Check | เกณฑ์ | ผลถ้า fail |
|---|---|---|
| row_count | rows = 0 | pipeline หยุด error |
| price_validity | invalid price > 1% ของทั้งหมด | pipeline หยุด error |
| qty_validity | invalid qty > 1% ของทั้งหมด | pipeline หยุด error |
| timestamp_range | timestamp นอกวัน > 5% | pipeline หยุด error |
| duplicates | มี duplicate trade_id | log warning แต่ไม่หยุด |

### ขั้นตอนที่ 4: Filter ข้อมูลเสียออก

กรองตามลำดับ และ log ว่าแต่ละ step ลบออกกี่ row:

```
1. price > 0          → ลบ row ที่ราคาเป็น 0 หรือ negative
2. qty > 0            → ลบ row ที่จำนวนเป็น 0 หรือ negative
3. trade_time == date → ลบ row ที่ timestamp ไม่ตรงกับวันที่ประมวลผล
4. dropDuplicates     → ลบ duplicate trade_id
```

### ขั้นตอนที่ 5: เพิ่ม Derived Columns

| Column ใหม่ | สูตร | ใช้ทำอะไร |
|---|---|---|
| `trade_time` | time / 1,000,000 → DateTime | ใช้ใน OHLCV window และ wash trade |
| `trade_hour` | hour(trade_time) | วิเคราะห์ pattern รายชั่วโมง |
| `trade_minute` | minute(trade_time) | |
| `is_sell` | = is_buyer_maker | True = seller เปิด order ก่อน (sell pressure) |
| `usd_value` | qty × price | มูลค่าจริงเป็น USDT สำหรับ fact_trades |
| `processed_date` | ds (date string) | partition และ idempotent reload |

**Schema ของ Silver Parquet:**

| Column | Type | หมายเหตุ |
|---|---|---|
| trade_id | int64 | |
| price | float64 | |
| qty | float64 | |
| quote_qty | float64 | |
| time | int64 | epoch microseconds (ยังเก็บไว้ใช้ใน wash trade) |
| is_buyer_maker | bool | |
| trade_time | datetime64 | แปลงแล้ว อ่านได้ |
| trade_hour | int32 | |
| trade_minute | int32 | |
| is_sell | bool | = is_buyer_maker |
| usd_value | float64 | qty × price |
| processed_date | string | |

**ตัวอย่าง row จริง:**

```
trade_id       : 549900448
price          : 0.000003
qty            : 57636887.0
trade_time     : 2026-04-13 00:00:00.509350   ← อ่านได้แล้ว
trade_hour     : 0
is_sell        : False                         ← buyer initiated
usd_value      : 199.999998                    ← มูลค่า USDT
processed_date : 2026-04-13
```

**ทำไมต้อง silver แยกจาก bronze:**
- Bronze = raw ไม่แตะ, audit trail
- Silver = clean, พร้อมใช้ ให้ Spark job ทุกตัวอ่านจากที่เดียวกัน
- ถ้าไม่มี silver แต่ละ job (OHLCV, pump dump, wash trade) ต้องทำ cleaning ซ้ำเอง เปลืองเวลาและเสี่ยง inconsistency

---

## Layer 5 — Gold (`data/gold/`)

Gold layer แบ่งเป็น 3 sub-layer ที่ออกมาจาก silver พร้อมกัน (parallel):

```
silver ──┬── compute_ohlcv       → gold/ohlcv_5min/
         ├── detect_pump_dump    → gold/pump_dump_signals/
         └── detect_wash_trade   → gold/wash_trade_signals/
```

---

### Gold 5A — OHLCV (`data/gold/ohlcv_5min/`)

**ไฟล์ที่รับผิดชอบ:** `spark_jobs/compute_ohlcv.py`

**วัตถุประสงค์:** รวบรวม transactions หลายแสน row ให้เป็น 288 candles (24 ชั่วโมง × 12 candles/ชั่วโมง)
เพื่อให้มองเห็น pattern ราคาในระดับ "ช่วงเวลา" แทนที่จะดูทีละ transaction

**โครงสร้าง folder:**

```
data/gold/ohlcv_5min/
└── trade_date=2026-05-10/
    └── part-00000-....snappy.parquet
```

**วิธีคำนวณ:**

Spark `window()` function จัด trades เป็น 5-minute buckets อัตโนมัติ:

```
00:00:00 – 00:04:59  →  candle 1
00:05:00 – 00:09:59  →  candle 2
...
23:55:00 – 23:59:59  →  candle 288
```

ใน Spark 5-minute window ใช้:

```python
F.window(F.col("trade_time"), "5 minutes")
```

**Aggregation ต่อ candle:**

| Column | สูตร | ความหมาย |
|---|---|---|
| open | first(price) | ราคา transaction แรกใน window |
| high | max(price) | ราคาสูงสุด |
| low | min(price) | ราคาต่ำสุด |
| close | last(price) | ราคา transaction สุดท้าย |
| volume_pepe | sum(qty) | ปริมาณ PEPE รวม |
| volume_usdt | sum(usd_value) | มูลค่า USDT รวม |
| trade_count | count(trade_id) | จำนวน transactions |
| buyer_initiated_count | count where is_sell=False | จำนวน buy pressure |
| seller_initiated_count | count where is_sell=True | จำนวน sell pressure |
| buy_sell_ratio | buyer_count / trade_count | > 0.5 = buy มากกว่า sell |

**ตัวอย่าง candle จริง (May 10, 00:00–00:05):**

```
window_start           : 2026-05-10 00:00:00
window_end             : 2026-05-10 00:05:00
open                   : 0.000004
high                   : 0.000004
low                    : 0.000004
close                  : 0.000004
volume_pepe            : 27,598,000,000   ← PEPE จำนวนมหาศาลเพราะราคาถูก
volume_usdt            : 117,002          ← แต่มูลค่าจริงแค่ 117k USD
trade_count            : 174
buyer_initiated_count  : 78
seller_initiated_count : 96
buy_sell_ratio         : 0.448            ← sell มากกว่า buy ช่วงนี้
```

**ทำไมต้อง OHLCV:** detect_pump_dump ใช้ OHLCV เป็น input โดยตรง ถ้าไม่สรุปเป็น candle ก่อน จะต้อง process หลักแสน rows ทุกครั้งที่ detect

---

### Gold 5B — Pump & Dump Signals (`data/gold/pump_dump_signals/`)

**ไฟล์ที่รับผิดชอบ:** `spark_jobs/detect_pump_dump.py`

**วัตถุประสงค์:** ตรวจจับช่วงเวลาที่ราคา spike ขึ้นผิดปกติแล้วตามด้วยการลงของราคา

**โครงสร้าง folder:**

```
data/gold/pump_dump_signals/
└── trade_date=2026-05-10/
    └── part-00000-....snappy.parquet
```

**Algorithm ทีละขั้น:**

**ขั้น 1 — คำนวณ Rolling 30-min Low**

สำหรับแต่ละ candle ให้หา "ราคาต่ำสุดใน 6 candle ก่อนหน้า" (= 30 นาที):

```python
w_lookback = Window.orderBy("window_start").rowsBetween(-6, -1)
rolling_low_30m = min("close").over(w_lookback)
```

เหตุผล: แทนที่จะดูว่า candle เดียวขึ้นกี่ % เราดูว่าราคาขยับจาก "จุดต่ำสุดล่าสุด" มาเท่าไหร่ ซึ่ง realistic กว่าสำหรับ PEPE ที่ราคาเคลื่อนช้า

**ขั้น 2 — คำนวณ rise_from_low_pct**

```
rise_from_low_pct = (close - rolling_low_30m) / rolling_low_30m × 100
```

**ขั้น 3 — กำหนด Pump Peak**

Pump peak = candle ที่ผ่านเงื่อนไขทั้งสอง:

- `rise_from_low_pct ≥ 1.5%` — ราคาขึ้นจากจุดต่ำสุด 30m อย่างน้อย 1.5%
- `volume_usdt ≥ 2 × rolling_avg_volume_30m` — มีแรงซื้อผิดปกติ

ทำไม 1.5%: จาก distribution จริงของ PEPE พบว่า p90 ของการขยับปกติคือ ~0.9% ต่อ candle ดังนั้น 1.5% เป็น outlier ที่ผิดปกติจริง

**ขั้น 4 — มองหน้าไป 60 นาที**

สำหรับ pump peak แต่ละจุด join กับ 12 candles ถัดไป (= 60 นาที) หา `min(future_close)`:

```python
future_alias.join(pump_alias,
    (future_row > pump_row) & (future_row <= pump_row + 12)
)
min_future_close = min("future_close")
```

**ขั้น 5 — ยืนยัน Dump**

```
dump_pct = (min_future_close - price_at_peak) / price_at_peak × 100
```

Dump confirmed เมื่อ `dump_pct ≤ -1.0%`

**ตัวอย่าง event จริง (May 10, 17:25):**

```
pump_window_start    : 2026-05-10 17:25:00
pump_window_end      : 2026-05-10 17:30:00
price_at_pump_start  : 0.000004   ← rolling low 30m
price_at_peak        : 0.000005   ← ราคา ณ pump peak
pump_pct             : 4.147%     ← ขึ้น 4.1% จากจุดต่ำสุด
price_after_dump     : 0.000004   ← ราคาต่ำสุดใน 60 นาทีถัดไป
dump_pct             : -1.327%    ← ลงหลัง pump
volume_usdt          : 2,543,788  ← 2.5M USDT ขณะ pump
severity             : HIGH
```

**Schema ของ Pump Dump Parquet:**

| Column | Type | ความหมาย |
|---|---|---|
| pump_window_start | datetime | เริ่ม pump |
| pump_window_end | datetime | จบ pump |
| price_at_pump_start | float64 | ราคา rolling low ก่อน pump |
| price_at_peak | float64 | ราคาสูงสุด ณ pump |
| price_after_dump | float64 | ราคาต่ำสุดใน 60m หลัง pump |
| pump_pct | float64 | % ที่ขึ้น |
| dump_pct | float64 | % ที่ลง (ติดลบ) |
| volume_usdt_during_pump | float64 | volume ขณะ pump (USDT) |
| estimated_profit_pct | float64 | = pump_pct (กำไรทางทฤษฎีถ้าเข้าก่อน) |
| severity | string | HIGH / MEDIUM |
| trade_date | date | |
| processed_date | date | |

---

### Gold 5C — Wash Trade Signals (`data/gold/wash_trade_signals/`)

**ไฟล์ที่รับผิดชอบ:** `spark_jobs/detect_wash_trade.py`

**วัตถุประสงค์:** ค้นหาคู่ buy–sell ที่เกิดขึ้นเกือบพร้อมกัน ราคาเหมือนกัน ปริมาณเหมือนกัน ซึ่งเป็น pattern ของการทำ wash trade

**โครงสร้าง folder:**

```
data/gold/wash_trade_signals/
└── trade_date=2026-04-29/
    ├── part-00000-....snappy.parquet
    └── part-00001-....snappy.parquet
```

**Algorithm ทีละขั้น:**

**ขั้น 1 — Time Bucket**

แทนที่จะ join ทุก buy กับทุก sell (O(n²) = หลายสิบล้านคู่)
ใช้ 1-second bucket เพื่อ limit search space:

```python
time_bucket = floor(unix_timestamp(trade_time))  # ระดับวินาที
```

trades ที่เกิดใน second เดียวกันจะมี bucket เดียวกัน

**ขั้น 2 — แยก Buys และ Sells**

```
buys  = trades where is_sell == False
sells = trades where is_sell == True
```

**ขั้น 3 — Join ใน Bucket เดียวกัน**

```python
pairs = buys.join(sells, buys.buy_bucket == sells.sell_bucket)
```

ได้คู่ทั้งหมดที่เกิดใน second เดียวกัน

**ขั้น 4 — กรอง 3 เงื่อนไข**

```
time_diff_ms      < 1000     ← ห่างกันไม่เกิน 1 วินาที
price_diff_pct    < 0.1%     ← ราคาต่างกันน้อยมาก
qty_similarity    > 90%      ← ปริมาณใกล้เคียงกัน
```

**ขั้น 5 — คำนวณ wash_score**

คะแนน 0–1 แบบ weighted:

```
time_score  = 1 - (time_diff_ms / 1000)     → weight 40%
price_score = 1 - (price_diff_pct / 0.1)    → weight 35%
qty_score   = qty_similarity_pct / 100      → weight 25%

wash_score = time_score×0.40 + price_score×0.35 + qty_score×0.25
```

เก็บเฉพาะ `wash_score ≥ 0.8`

**ตัวอย่าง pair จริง (Apr 29):**

```
buy_trade_id        : 551656666
sell_trade_id       : 551656710
time_diff_ms        : 159       ← ห่างกัน 0.159 วินาที
price_diff_pct      : 0.0       ← ราคาเท่ากันทุก decimal
qty_similarity_pct  : 96.74     ← ปริมาณใกล้กัน 96.7%
wash_score          : 0.928     ← น่าสงสัยมาก
```

**Schema ของ Wash Trade Parquet:**

| Column | Type | ความหมาย |
|---|---|---|
| trade_date | date | |
| buy_trade_id | int64 | ID ของฝั่ง buy |
| sell_trade_id | int64 | ID ของฝั่ง sell |
| time_diff_ms | int64 | ห่างกันกี่ ms |
| price_diff_pct | float64 | ราคาต่างกันกี่ % |
| qty_similarity_pct | float64 | ปริมาณเหมือนกันกี่ % |
| wash_score | float64 | คะแนน 0–1 |
| processed_date | date | |

---

## Layer 6 — MySQL Data Warehouse (`pepe_dw`)

**ไฟล์ที่รับผิดชอบ:** `dags/pepe_daily_pipeline.py` → task `load_gold_to_mysql` + `sql/init.sql`

**วัตถุประสงค์:** รวมผลลัพธ์ทั้งหมดไว้ใน relational database ที่ query ได้ง่าย ใช้ TablePlus, SQL client, หรือ Python ได้ทันที

**Schema Design (Star Schema):**

```
dim_date
  date_id PK ─────────────────────────── fact_trades.date_id FK
  full_date
  year, month, day_of_month
  day_of_week  (0=Monday, 6=Sunday)
  is_weekend


fact_trades                              fact_pump_dump_events
  trade_id PK                              event_id PK
  date_id FK → dim_date                    trade_date
  price                                    pump_window_start / end
  qty                                      price_at_pump_start
  quote_qty                                price_at_peak
  trade_time                               price_after_dump
  is_buyer_maker                           pump_pct
  is_sell                                  dump_pct
  usd_value                                volume_usdt_during_pump
  trade_hour                               estimated_profit_pct
  processed_date                           severity  (HIGH/MEDIUM/LOW)
                                           processed_date


fact_wash_trade_pairs
  pair_id PK
  trade_date
  buy_trade_id
  sell_trade_id
  time_diff_ms
  price_diff_pct
  qty_similarity_pct
  wash_score
  processed_date
```

**task `load_gold_to_mysql` ทำอะไร:**

1. **upsert `dim_date`** — INSERT IGNORE วันที่ลงไป (seed ทั้งปี 2026–2029 ไว้แล้วใน init.sql)

2. **load `fact_trades`** จาก silver parquet:
   - DELETE ข้อมูลวันนั้นออกก่อน (`DELETE WHERE processed_date = ds`)
   - แล้ว INSERT ใหม่ทั้งหมด → idempotent ถ้า run ซ้ำจะไม่ duplicate

3. **load `fact_pump_dump_events`** จาก gold parquet:
   - DELETE + INSERT เหมือนกัน
   - ถ้าไม่มี events วันนั้น (parquet ว่าง) → skip ไม่ error

4. **load `fact_wash_trade_pairs`** จาก gold parquet:
   - DELETE + INSERT เหมือนกัน
   - ใช้ `chunksize=5000` เพราะมีหลักหมื่น rows

5. **task `validate_dw_counts`**:
   - `SELECT COUNT(*) FROM fact_trades WHERE processed_date = ds`
   - ถ้า count = 0 → raise error (pipeline ถือว่า fail)

**ผลลัพธ์ปัจจุบันใน MySQL (8 วัน):**

| Table | Rows ทั้งหมด |
|---|---|
| fact_trades | 814,733 |
| fact_pump_dump_events | 3 |
| fact_wash_trade_pairs | 178,064 |
| dim_date | seeded ครบ 2026–2029 |

---

## สรุป: ข้อมูลเดินทางผ่านแต่ละ Layer อย่างไร

```
ZIP (Binance)
  → landing/         : drop zone, check ว่าไฟล์มีอยู่
  → bronze/          : แตก ZIP, อ่าน CSV ดิบ, เขียน Parquet ไม่แก้ไข
  → silver/          : cast types, DQ check, filter เสีย, เพิ่ม columns
  → gold/ohlcv/      : group by 5min window → 288 candles ต่อวัน
  → gold/pump_dump/  : rolling 30m low → detect spike + dump ใน 60m
  → gold/wash_trade/ : time-bucket join → detect buy-sell pairs น่าสงสัย
  → MySQL/           : load ทั้งหมด, query ได้ผ่าน TablePlus หรือ SQL
```

แต่ละ layer มีหน้าที่เดียวชัดเจน ถ้า logic ไหนผิดพลาด แก้ได้โดยไม่กระทบ layer อื่น
