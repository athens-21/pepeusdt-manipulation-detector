-- ============================================================
-- pepe-pipeline: Star Schema for PEPEUSDT Market Manipulation DW
-- ============================================================

CREATE DATABASE IF NOT EXISTS pepe_dw;
USE pepe_dw;

-- ------------------------------------------------------------
-- Dimension: dim_date
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         INT PRIMARY KEY COMMENT 'Format: YYYYMMDD',
    full_date       DATE         NOT NULL,
    year            SMALLINT     NOT NULL,
    month           TINYINT      NOT NULL,
    day_of_month    TINYINT      NOT NULL,
    day_of_week     TINYINT      NOT NULL COMMENT '0=Monday, 6=Sunday',
    is_weekend      BOOLEAN      NOT NULL
);

-- ------------------------------------------------------------
-- Dimension: dim_time_window
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_time_window (
    window_id           INT AUTO_INCREMENT PRIMARY KEY,
    window_start        DATETIME     NOT NULL,
    window_end          DATETIME     NOT NULL,
    window_size_minutes INT          NOT NULL,
    hour_of_day         TINYINT      NOT NULL,
    INDEX idx_window_start (window_start)
);

-- ------------------------------------------------------------
-- Fact: fact_trades
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_trades (
    trade_id        BIGINT PRIMARY KEY,
    date_id         INT             NOT NULL,
    price           DECIMAL(20, 10) NOT NULL,
    qty             DECIMAL(20, 8)  NOT NULL,
    quote_qty       DECIMAL(20, 8)  NOT NULL,
    trade_time      DATETIME        NOT NULL,
    is_buyer_maker  BOOLEAN         NOT NULL,
    is_sell         BOOLEAN         NOT NULL,
    usd_value       DECIMAL(20, 8)  NOT NULL,
    trade_hour      TINYINT         NOT NULL,
    processed_date  DATE            NOT NULL,
    CONSTRAINT fk_trades_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    INDEX idx_trade_time   (trade_time),
    INDEX idx_processed    (processed_date),
    INDEX idx_date_id      (date_id)
);

-- ------------------------------------------------------------
-- Fact: fact_pump_dump_events
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_pump_dump_events (
    event_id                INT AUTO_INCREMENT PRIMARY KEY,
    trade_date              DATE            NOT NULL,
    pump_window_start       DATETIME        NOT NULL,
    pump_window_end         DATETIME        NOT NULL,
    price_at_pump_start     DECIMAL(20, 10) NOT NULL,
    price_at_peak           DECIMAL(20, 10) NOT NULL,
    price_after_dump        DECIMAL(20, 10) NOT NULL,
    pump_pct                DECIMAL(10, 4)  NOT NULL COMMENT 'Price increase % at pump candle',
    dump_pct                DECIMAL(10, 4)  NOT NULL COMMENT 'Price decrease % after peak (negative)',
    volume_usdt_during_pump DECIMAL(20, 8)  NOT NULL,
    estimated_profit_pct    DECIMAL(10, 4)  NOT NULL,
    severity                ENUM('HIGH', 'MEDIUM', 'LOW') NOT NULL,
    processed_date          DATE            NOT NULL,
    INDEX idx_trade_date    (trade_date),
    INDEX idx_severity      (severity),
    INDEX idx_pump_start    (pump_window_start)
);

-- ------------------------------------------------------------
-- Fact: fact_wash_trade_pairs
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_wash_trade_pairs (
    pair_id             INT AUTO_INCREMENT PRIMARY KEY,
    trade_date          DATE            NOT NULL,
    buy_trade_id        BIGINT          NOT NULL,
    sell_trade_id       BIGINT          NOT NULL,
    time_diff_ms        BIGINT          NOT NULL COMMENT 'Milliseconds between buy and sell',
    price_diff_pct      DECIMAL(10, 6)  NOT NULL,
    qty_similarity_pct  DECIMAL(10, 4)  NOT NULL,
    wash_score          DECIMAL(5, 4)   NOT NULL,
    processed_date      DATE            NOT NULL,
    INDEX idx_trade_date    (trade_date),
    INDEX idx_wash_score    (wash_score),
    INDEX idx_buy_trade     (buy_trade_id),
    INDEX idx_sell_trade    (sell_trade_id)
);

-- ------------------------------------------------------------
-- Seed dim_date for 2026 (extend as needed)
-- ------------------------------------------------------------
INSERT IGNORE INTO dim_date (date_id, full_date, year, month, day_of_month, day_of_week, is_weekend)
SELECT
    CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED)  AS date_id,
    d                                            AS full_date,
    YEAR(d)                                      AS year,
    MONTH(d)                                     AS month,
    DAY(d)                                       AS day_of_month,
    WEEKDAY(d)                                   AS day_of_week,
    IF(WEEKDAY(d) >= 5, TRUE, FALSE)             AS is_weekend
FROM (
    SELECT DATE('2026-01-01') + INTERVAL seq DAY AS d
    FROM (
        SELECT a.N + b.N * 10 + c.N * 100 AS seq
        FROM
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) c
    ) seq_gen
    WHERE DATE('2026-01-01') + INTERVAL seq DAY <= DATE('2029-12-31')
) dates;
