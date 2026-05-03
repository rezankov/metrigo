CREATE DATABASE IF NOT EXISTS metrigo;

DROP TABLE IF EXISTS metrigo.raw_events;
DROP TABLE IF EXISTS metrigo.etl_state;
DROP TABLE IF EXISTS metrigo.etl_runs;
DROP TABLE IF EXISTS metrigo.fact_sales;
DROP TABLE IF EXISTS metrigo.fact_orders;
DROP TABLE IF EXISTS metrigo.fact_stock_snapshot;
DROP TABLE IF EXISTS metrigo.fact_incomes;
DROP TABLE IF EXISTS metrigo.fact_fin_report;

CREATE TABLE metrigo.raw_events
(
    seller_id String,
    source String,
    event_dt Nullable(DateTime),
    payload String,
    payload_hash String,
    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (seller_id, source, payload_hash, version);

CREATE TABLE metrigo.etl_state
(
    seller_id String,
    source LowCardinality(String),
    watermark DateTime,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (seller_id, source);

CREATE TABLE metrigo.etl_runs
(
    ts DateTime DEFAULT now(),
    seller_id String,
    source LowCardinality(String),
    status LowCardinality(String),
    loaded UInt64,
    message String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (seller_id, source, ts);

CREATE TABLE metrigo.fact_sales
(
    seller_id String,
    date_time DateTime,
    sale_date Date,
    sell_id String,
    op LowCardinality(String),
    order_number String,
    delivery_number String,
    seller_art String,
    wb_art String,
    barcode String,
    warehouse String,
    warehouse_district String,
    warehouse_region String,
    full_price Float64,
    seller_discount Float64,
    wb_discount Float64,
    buyers_price Float64,
    seller_price Float64,
    transfer_to_seller Float64,
    dedup_key FixedString(32),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(sale_date)
ORDER BY (seller_id, sale_date, seller_art, warehouse, dedup_key);

CREATE TABLE metrigo.fact_orders
(
    seller_id String,
    date_time DateTime,
    last_change_date DateTime,
    supplier_article String,
    nm_id UInt64,
    barcode String,
    warehouse_name String,
    quantity Int32,
    total_price Float64,
    discount_percent Int32,
    is_cancel Bool,
    cancel_dt Nullable(DateTime),
    payload_hash FixedString(32),
    dedup_key FixedString(32),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(date_time)
ORDER BY (seller_id, date_time, supplier_article, barcode, warehouse_name, dedup_key);

CREATE TABLE metrigo.fact_stock_snapshot
(
    seller_id String,
    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),
    warehouse String,
    seller_art String,
    barcode String,
    qty Int32,
    version UInt64,
    in_way_to_client Int32 DEFAULT 0,
    in_way_from_client Int32 DEFAULT 0,
    qty_full Int32 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, snapshot_date, warehouse, seller_art, barcode);

CREATE TABLE metrigo.fact_incomes
(
    seller_id String,
    income_id UInt64,
    date_time DateTime,
    income_date Date,
    warehouse String,
    seller_art String,
    barcode String,
    nm_id UInt64,
    qty Int32,
    last_change_dt DateTime,
    payload_hash FixedString(32),
    dedup_key FixedString(32),
    loaded_at DateTime
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(income_date)
ORDER BY (seller_id, income_date, warehouse, seller_art, barcode, income_id, dedup_key);

CREATE TABLE metrigo.fact_fin_report
(
    seller_id String,
    rrdid UInt64,
    report_date Date,
    doc_type_name String,
    supplier_oper_name String,
    seller_art String,
    nm_id UInt64,
    barcode String,
    quantity Int32,
    retail_price Float64,
    ppvz_for_pay Float64,
    delivery_rub Float64,
    return_rub Float64,
    commission_rub Float64,
    penalty_rub Float64,
    additional_payment Float64,
    acquiring_fee Float64,
    realized_commission Float64,
    pay_to_seller Float64,
    payload_hash FixedString(32),
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (seller_id, report_date, rrdid, seller_art, barcode, payload_hash);