# PEPEUSDT Market Manipulation Detection Pipeline — เอกสารโปรเจค

---

## 1. โปรเจคนี้คืออะไร ทำขึ้นมาเพื่ออะไร

PEPE เป็น meme coin บน Binance ที่มีราคาต่ำมาก (ประมาณ 0.000004 USDT) และมีปริมาณการเทรดสูงมากในแต่ละวัน ลักษณะแบบนี้ทำให้เป็นเป้าหมายที่ดีของการ **บิดเบือนราคา (Market Manipulation)** ซึ่งในตลาด crypto มีรูปแบบที่พบบ่อย 2 แบบหลัก:

### Pump & Dump
ผู้ไม่หวังดีซื้อเหรียญจำนวนมากเพื่อดันราคาขึ้นอย่างรวดเร็ว (Pump) ดึงดูดนักลงทุนรายย่อยให้ซื้อตาม แล้วทยอยขายออกทั้งหมดในราคาสูง (Dump) ทำให้ราคาตกฮวบ นักลงทุนรายย่อยขาดทุน

### Wash Trading
นักเทรดคนเดียวหรือกลุ่มเดียวกัน ทำการซื้อและขายให้กันเองในราคาและจำนวนใกล้เคียงกัน ภายในเวลาสั้นมาก เพื่อสร้าง volume ปลอม ทำให้เหรียญดูน่าสนใจและมีสภาพคล่องสูง ทั้งที่จริงไม่มีการเปลี่ยนมือของสินทรัพย์

**เป้าหมายของโปรเจค:** สร้าง data pipeline ที่รับข้อมูลเทรดจริงจาก Binance แล้วประมวลผลอัตโนมัติทุกวัน เพื่อตรวจจับ event ทั้ง 2 แบบ และจัดเก็บผลลัพธ์ไว้ใน Data Warehouse สำหรับวิเคราะห์ต่อได้

---

## 2. Architecture ภาพรวม

```
Binance Vision (ดาวน์โหลด ZIP)
        ↓
  data/landing/          ← วางไฟล์ ZIP ที่นี่
        ↓
  [Airflow DAG]          ← ควบคุมลำดับการทำงานทั้งหมด
        ↓
  data/bronze/           ← CSV → Parquet ดิบ
        ↓ (Spark)
  data/silver/           ← ทำความสะอาดข้อมูล + เพิ่ม columns
        ↓ (Spark)
  data/gold/
  ├── ohlcv_5min/        ← ราคาสรุปทุก 5 นาที
  ├── pump_dump_signals/ ← Pump & Dump events ที่ตรวจพบ
  └── wash_trade_signals/← Wash Trade pairs ที่ตรวจพบ
        ↓ (Python)
  MySQL (pepe_dw)        ← Data Warehouse สำหรับ query / วิเคราะห์
```

**เทคโนโลยีที่ใช้:**
- **Apache Airflow** — orchestration ควบคุมลำดับ task และ schedule
- **Apache Spark (PySpark)** — ประมวลผลข้อมูลหลักแสนถึงหลักล้านแถวได้รวดเร็ว
- **MySQL** — Data Warehouse ปลายทางสำหรับ query
- **Docker Compose** — รันทุก service บนเครื่องเดียว

---

## 3. โครงสร้าง Folder และไฟล์ทั้งหมด

```
pepe-pipeline/
├── dags/                    ← Airflow DAG definitions
├── spark_jobs/              ← PySpark transformation & detection scripts
├── dq/                      ← Data Quality checks
├── sql/                     ← SQL schema สำหรับ MySQL
├── data/                    ← ข้อมูลทุก layer (landing → bronze → silver → gold)
├── scripts/                 ← utility scripts
├── Dockerfile.airflow       ← Docker image สำหรับ Airflow
├── Dockerfile.spark         ← Docker image สำหรับ Spark
└── docker-compose.yml       ← กำหนด services ทั้งหมด
```

---

## 4. อธิบายแต่ละไฟล์โดยละเอียด

---

### 4.1 `docker-compose.yml`

**ทำหน้าที่:** กำหนด services ทั้งหมดที่รันอยู่ใน Docker พร้อม configuration

**Services ที่รัน:**

| Service | หน้าที่ | Port |
|---------|---------|------|
| `postgres` | เก็บ Airflow metadata (run history, task states) | 5432 |
| `airflow-webserver` | หน้า UI ของ Airflow | 8080 |
| `airflow-scheduler` | ควบคุมการ schedule และรัน task | — |
| `airflow-init` | สร้าง user admin และ migrate database ตอน startup | — |
| `spark-master` | จัดการ Spark cluster | 7077, 8090 |
| `spark-worker` | รัน Spark job จริง (memory 1500MB, 2 cores) | — |
| `mysql` | Data Warehouse เก็บผลลัพธ์สุดท้าย | 3306 |

**ทำไมต้องใช้ Docker Compose:** เพื่อให้รันทุก service พร้อมกันบนเครื่องเดียว reproducible และลบ/สร้างใหม่ได้ง่าย

---

### 4.2 `dags/pepe_daily_pipeline.py`

**ทำหน้าที่:** DAG หลักของโปรเจค กำหนดลำดับการทำงานทุก task สำหรับ 1 วัน

**ลำดับ task:**
```
start
  → check_landing_file      ← เช็คว่าไฟล์ ZIP อยู่ใน landing หรือยัง
  → ingest_to_bronze        ← แตก ZIP → อ่าน CSV → เขียน Parquet (bronze)
  → bronze_to_silver        ← Spark: ทำความสะอาดข้อมูล
  → compute_ohlcv           ← Spark: คำนวณ OHLCV ทุก 5 นาที
  → [detect_pump_dump,      ← Spark: วิเคราะห์ทั้งสอง parallel กัน
     detect_wash_trade]
  → load_gold_to_mysql      ← Python: เอาผลลัพธ์ทั้งหมดใส่ MySQL
  → validate_dw_counts      ← เช็คว่า MySQL มีข้อมูลจริง ไม่ได้ 0 rows
end
```

**จุดสำคัญ:**
- `schedule=None` หมายความว่าไม่ได้รันอัตโนมัติ ต้อง trigger เอง
- `max_active_runs=1` ป้องกันรันซ้อนกัน เพราะ Spark resource จำกัด
- รับ parameter `ds` (date string) เพื่อให้ระบุวันที่ต้องการประมวลผลได้

---

### 4.3 `dags/pepe_run_all.py`

**ทำหน้าที่:** DAG utility สำหรับ backfill — scan ไฟล์ทั้งหมดใน `data/landing/` แล้ว trigger `pepe_daily_pipeline` ทีละวันอัตโนมัติ

**ทำไมต้องมีไฟล์นี้:** ถ้ามีข้อมูล 8 วัน แทนที่จะต้อง trigger ทีละวัน 8 ครั้ง แค่รัน `pepe_run_all` ครั้งเดียว มันจะ scan แล้ว trigger ให้ครบเอง

**Logic:**
1. `scan_landing_files` — glob หาไฟล์ที่ match pattern `PEPEUSDT-trades-*.zip`
2. `trigger_all_dates` — loop ผ่าน dates ที่พบ แล้วเรียก `airflow dags trigger` สำหรับแต่ละวัน

---

### 4.4 `spark_jobs/bronze_to_silver.py`

**ทำหน้าที่:** ทำความสะอาดข้อมูลดิบจาก bronze layer และเพิ่ม columns ที่เป็นประโยชน์

**สิ่งที่ทำ:**
1. อ่าน Parquet จาก bronze
2. Cast type ให้ถูกต้อง (price → Double, time → Long, is_buyer_maker → Boolean)
3. แปลง timestamp จาก epoch microseconds → DateTime ที่อ่านได้
4. รัน Data Quality checks (ดูหัวข้อ 4.7)
5. กรองข้อมูลที่ไม่ถูกต้องออก:
   - price ≤ 0
   - qty ≤ 0
   - timestamp อยู่นอกวันที่
   - duplicate trade_id
6. เพิ่ม columns ใหม่:
   - `trade_hour` / `trade_minute` — ชั่วโมงและนาทีของการเทรด
   - `is_sell` — True ถ้าเป็นฝั่ง seller-initiated (= is_buyer_maker)
   - `usd_value` — มูลค่าเป็น USDT (qty × price)
   - `processed_date` — วันที่ประมวลผล

**ทำไมต้องมี silver layer:** Bronze คือข้อมูลดิบที่ยังไม่ผ่านการตรวจสอบ การแยก silver layer ออกมาทำให้ downstream jobs ทุกตัวได้ข้อมูลที่สะอาดแล้ว ไม่ต้องทำซ้ำ

---

### 4.5 `spark_jobs/compute_ohlcv.py`

**ทำหน้าที่:** รวบรวม trades ทุก transaction ในช่วง 5 นาที มาสรุปเป็น OHLCV candle

**OHLCV คืออะไร:**
- **O**pen — ราคาเทรดแรกใน window นั้น
- **H**igh — ราคาสูงสุดใน window
- **L**ow — ราคาต่ำสุดใน window
- **C**lose — ราคาเทรดสุดท้ายใน window
- **V**olume — ปริมาณเทรดทั้งหมด (ทั้ง PEPE และ USDT)

**Columns เพิ่มเติม:**
- `trade_count` — จำนวน transactions ใน window
- `buyer_initiated_count` / `seller_initiated_count` — แยกว่าเป็น buy หรือ sell pressure
- `buy_sell_ratio` — สัดส่วน buyer initiated (> 0.5 แปลว่า buy pressure มากกว่า)

**ทำไมต้อง OHLCV:** ข้อมูลดิบมี transactions หลักแสนต่อวัน ถ้าจะหา pump & dump ต้องมองเป็น "กรอบเวลา" ไม่ใช่ทีละ transaction OHLCV ทุก 5 นาทีทำให้เห็น pattern การขยับของราคาได้

---

### 4.6 `spark_jobs/detect_pump_dump.py`

**ทำหน้าที่:** ตรวจจับ Pump & Dump events จาก OHLCV ที่คำนวณไว้

**Algorithm (Rolling Peak/Trough):**

> **ทำไมต้องใช้ rolling แทน single candle:**
> PEPE ในช่วงนี้ราคาขยับต่อ candle 5 นาที สูงสุดแค่ ~1-4% เท่านั้น ถ้าใช้ threshold เดิม (candle เดียวขึ้น ≥10%) จะไม่เจออะไรเลย จึงเปลี่ยนมาดูว่าราคาขึ้นจาก "จุดต่ำสุดในช่วง 30 นาทีที่ผ่านมา" เท่าไหร่แทน

**ขั้นตอน:**
1. คำนวณ `rolling_low_30m` = ราคาต่ำสุดใน 6 candle ก่อนหน้า (= 30 นาที)
2. คำนวณ `rise_from_low_pct` = (ราคาตอนนี้ − rolling_low) / rolling_low × 100
3. **Pump confirmed** เมื่อ:
   - `rise_from_low_pct ≥ 1.5%` (สูงกว่า p90 ของการขยับปกติที่ ~0.9%)
   - volume ≥ 2× rolling average volume (มีแรงซื้อผิดปกติ)
4. สำหรับแต่ละ pump peak ให้มองหน้าไป 12 candle (= 60 นาที)
5. **Dump confirmed** เมื่อ `min(future_close)` ลดลงจาก peak ≥ 1.0%

**Severity:**
- **HIGH** — pump_pct ≥ 2.5% (ผิดปกติมาก)
- **MEDIUM** — pump_pct ≥ 1.5%

**Output columns สำคัญ:**
| Column | ความหมาย |
|--------|---------|
| `pump_window_start` | เวลาที่ pump เริ่ม |
| `price_at_pump_start` | ราคาก่อน pump (จุดต่ำสุด 30m) |
| `price_at_peak` | ราคาสูงสุดขณะ pump |
| `price_after_dump` | ราคาต่ำสุดหลัง dump (ใน 60m ถัดไป) |
| `pump_pct` | % ที่ราคาขึ้น |
| `dump_pct` | % ที่ราคาลง (ค่าติดลบ) |
| `volume_usdt_during_pump` | volume ขณะ pump เป็น USDT |
| `severity` | HIGH / MEDIUM |

---

### 4.7 `spark_jobs/detect_wash_trade.py`

**ทำหน้าที่:** ตรวจจับคู่การเทรดที่น่าสงสัยว่าเป็น Wash Trading

**ทำไม Wash Trading อันตราย:** สร้าง volume ปลอม ทำให้คนภายนอกเข้าใจผิดว่าเหรียญนี้มีสภาพคล่องสูง

**Algorithm (Time-bucket Join):**

> **ทำไมต้อง bucket ก่อน:** ถ้า cross-join trades ทั้งหมด (หลักแสน) กับตัวเอง จะได้ pairs หลักสิบล้าน ใช้ memory มากเกินไป การ bucket ด้วย 1 วินาที จำกัด search space ให้เฉพาะ trades ที่เกิดใกล้กัน

**ขั้นตอน:**
1. แบ่ง trades เป็น **buys** (is_sell = False) และ **sells** (is_sell = True)
2. เพิ่ม `time_bucket` = floor ของ timestamp ไปที่ระดับวินาที
3. Join buys กับ sells ที่อยู่ใน `time_bucket` เดียวกัน (เกิดภายใน 1 วินาที)
4. กรองด้วย 3 เงื่อนไข:
   - `time_diff_ms < 1000` — ห่างกันไม่เกิน 1 วินาที
   - `price_diff_pct < 0.1%` — ราคาต่างกันไม่ถึง 0.1%
   - `qty_similarity_pct > 90%` — ปริมาณใกล้เคียงกัน > 90%
5. คำนวณ `wash_score` (0–1):
   - `time_score × 0.40` — ยิ่งเกิดใกล้กันยิ่งน่าสงสัย
   - `price_score × 0.35` — ยิ่งราคาเท่ากันยิ่งน่าสงสัย
   - `qty_score × 0.25` — ยิ่งจำนวนเท่ากันยิ่งน่าสงสัย
6. เก็บเฉพาะคู่ที่ `wash_score ≥ 0.8`

**Output columns สำคัญ:**
| Column | ความหมาย |
|--------|---------|
| `buy_trade_id` / `sell_trade_id` | ID ของคู่ที่น่าสงสัย |
| `time_diff_ms` | ห่างกันกี่ millisecond |
| `price_diff_pct` | ราคาต่างกันกี่ % |
| `qty_similarity_pct` | ปริมาณเหมือนกันกี่ % |
| `wash_score` | คะแนนความน่าสงสัย (ยิ่งใกล้ 1 ยิ่งน่าสงสัย) |

---

### 4.8 `dq/quality_checks.py`

**ทำหน้าที่:** Data Quality checks ที่รันก่อน silver layer จะถูกเขียน

**Checks ที่ทำ:**

| Check | เกณฑ์ fail | ผลเมื่อ fail |
|-------|-----------|------------|
| `check_row_count` | count = 0 | raise error หยุด pipeline |
| `check_price_validity` | invalid price > 1% | raise error |
| `check_qty_validity` | invalid qty > 1% | raise error |
| `check_timestamp_range` | out-of-range timestamp > 5% | raise error |
| `check_duplicates` | มี duplicate trade_id | log warning (ไม่ fail) |

**ทำไมต้องมี DQ:** ถ้าข้อมูลดิบจาก Binance เสีย (ไฟล์ corrupt, download ไม่ครบ) และปล่อยผ่านไป จะทำให้ผลลัพธ์ด้านล่างผิดพลาดทั้งหมด DQ layer จับปัญหาตั้งแต่ต้น

---

### 4.9 `sql/init.sql`

**ทำหน้าที่:** สร้าง schema ของ MySQL Data Warehouse ตอน container เริ่มต้นครั้งแรก

**Star Schema ที่ออกแบบ:**

```
dim_date ──────────────┐
dim_time_window        ├── fact_trades
                       │
                       ├── fact_pump_dump_events
                       └── fact_wash_trade_pairs
```

**Tables:**

| Table | หน้าที่ |
|-------|---------|
| `dim_date` | Dimension วันที่ มี year, month, day_of_week, is_weekend |
| `dim_time_window` | Dimension กรอบเวลา (ยังไม่ใช้เต็มรูปแบบ) |
| `fact_trades` | ข้อมูล trades ทั้งหมดที่ผ่าน silver layer |
| `fact_pump_dump_events` | Pump & Dump events ที่ตรวจพบ |
| `fact_wash_trade_pairs` | คู่ Wash Trade ที่น่าสงสัย |

**ทำไมต้อง Star Schema:** รูปแบบนี้ query ได้รวดเร็ว เหมาะกับ analytical queries เช่น "เหตุการณ์ pump วันไหนหนักที่สุด" หรือ "wash trade มากช่วงวันไหนของสัปดาห์"

---

### 4.10 `Dockerfile.airflow` และ `Dockerfile.spark`

**ทำหน้าที่:** สร้าง custom Docker images ที่ติดตั้ง dependencies ที่โปรเจคต้องการ

- **Dockerfile.airflow** — base image Airflow 2.8.1 + ติดตั้ง `pandas`, `pyarrow`, `sqlalchemy`, `pymysql`
- **Dockerfile.spark** — base image Spark 3.5.0 + Python 3.11 + ติดตั้ง `pyspark`, `pandas`, `pyarrow`

---

## 5. ข้อมูล Input และ Output

### Input
ดาวน์โหลดจาก Binance Vision:
```
https://data.binance.vision/?prefix=data/spot/daily/trades/PEPEUSDT/
```
ชื่อไฟล์ format: `PEPEUSDT-trades-YYYY-MM-DD.zip`
วางไว้ที่: `data/landing/`

### Output สุดท้ายใน MySQL

**`fact_trades`** — ข้อมูล trade ทุก transaction ที่ผ่านการทำความสะอาดแล้ว

**`fact_pump_dump_events`** — Pump & Dump ที่ตรวจพบ

**`fact_wash_trade_pairs`** — คู่ Wash Trade ที่น่าสงสัย

---

## 6. ผลลัพธ์ที่ได้ และความหมาย

### 6.1 Pump & Dump Events (ข้อมูล 8 วัน: Apr 13–15, Apr 29, May 9–12)

| วันที่ | Severity | pump_pct | dump_pct | Volume ขณะ pump |
|--------|----------|----------|----------|----------------|
| 2026-04-14 | MEDIUM | 2.17% | -1.59% | 1.7M USDT |
| 2026-04-15 | MEDIUM | 1.62% | -1.33% | 1.35M USDT |
| 2026-05-10 | **HIGH** | **4.15%** | -1.33% | **4.68M USDT** |

**การแปลผล:**
- **May 10 น่าสนใจที่สุด** — ราคาขึ้น 4.15% จากจุดต่ำสุดใน 30 นาทีก่อน พร้อม volume ผิดปกติ 4.68M USDT แล้วตามมาด้วยการลง -1.33% ใน 60 นาทีถัดไป เป็น pattern ที่ชัดเจนของ pump & dump
- วันที่ไม่พบ event (Apr 13, Apr 29, May 9, May 11, May 12) ราคาขยับในระดับปกติ ไม่มี spike ที่เข้าเกณฑ์

### 6.2 Wash Trade Pairs (ทุกวัน)

| วันที่ | คู่ที่น่าสงสัย | avg wash_score | High-confidence (≥0.95) |
|--------|--------------|----------------|------------------------|
| Apr 13 | 21,822 | 0.9288 | 8,867 |
| Apr 14 | 28,648 | 0.9331 | 14,621 |
| Apr 15 | 27,990 | 0.9232 | 12,424 |
| Apr 29 | **47,390** | 0.9222 | **19,680** |
| May 09 | 3,122 | 0.9124 | 837 |
| May 10 | 25,981 | 0.9387 | 13,790 |
| May 11 | 14,214 | 0.9300 | 6,806 |
| May 12 | 9,125 | 0.9330 | 4,495 |

**การแปลผล:**
- **Apr 29 น่ากังวลที่สุด** — พบ 47,390 คู่ ซึ่งสูงกว่าวันอื่นมาก แสดงว่ามีกิจกรรม wash trading ผิดปกติ volume จริงของวันนี้ (76.6M USDT) อาจถูก inflate ด้วย wash trade
- **avg wash_score > 0.92 ทุกวัน** — หมายความว่าคู่ที่เจอมีความน่าสงสัยสูงมาก ไม่ใช่แค่บังเอิญ
- **May 9 ต่ำที่สุด (3,122 คู่)** — volume วันนั้นก็ต่ำสุดด้วย (24.1M USDT) สอดคล้องกัน

---

## 7. วิธีดูผลลัพธ์

### ผ่าน TablePlus (แนะนำ)
- Host: `127.0.0.1` Port: `3306`
- User: `pepe_user` Password: *(ดูใน `.env`)* Database: `pepe_dw`
- เปิด table ที่ต้องการ filter/sort ได้เลย

### ผ่าน Terminal
```bash
docker exec pepe-pipeline-mysql-1 sh -c \
  'mysql -u pepe_user -pdwpass123 pepe_dw -e "SELECT * FROM fact_pump_dump_events;"'
```

### ผ่าน Python (pandas)
```python
import pandas as pd

# Pump & Dump
df = pd.read_parquet("data/gold/pump_dump_signals/trade_date=2026-05-10/")

# Wash Trade
df = pd.read_parquet("data/gold/wash_trade_signals/trade_date=2026-04-29/")

# OHLCV
df = pd.read_parquet("data/gold/ohlcv_5min/trade_date=2026-05-10/")
```

---

## 8. วิธีเพิ่มข้อมูลวันใหม่

1. ดาวน์โหลด ZIP จาก Binance Vision
2. วางที่ `data/landing/PEPEUSDT-trades-YYYY-MM-DD.zip`
3. Trigger `pepe_run_all` ใน Airflow UI (http://localhost:8080)
4. Pipeline จะรันอัตโนมัติและ update MySQL ให้ครบ

---

## 9. ข้อจำกัดและสิ่งที่ควรพัฒนาต่อ

| ข้อจำกัด | เหตุผล | แนวทางแก้ |
|---------|--------|-----------|
| Pump & Dump threshold ปรับ manual | แต่ละเหรียญ volatility ต่างกัน | ใช้ statistical outlier detection แบบ dynamic |
| Wash Trade ดูแค่ภายใน 1 วินาที | บาง wash trade อาจ spread ข้าม seconds | ขยาย time window และเพิ่ม network graph analysis |
| ไม่มี cross-day analysis | pump อาจเริ่มวันนึงและ dump อีกวัน | รวม data ข้ามวันก่อนวิเคราะห์ |
| ไม่มี dashboard | ดูผลต้องผ่าน SQL หรือ code | เพิ่ม Metabase หรือ Grafana |

---

*เอกสารนี้อธิบายสถานะของโปรเจค ณ วันที่ 13 พฤษภาคม 2026*
