CREATE DATABASE IF NOT EXISTS metrigo;

CREATE TABLE IF NOT EXISTS metrigo.raw_events
(
    seller_id String,
    source String,
    event_id String,
    loaded_at DateTime DEFAULT now(),
    payload String
)
ENGINE = MergeTree
ORDER BY (seller_id, source, event_id);

CREATE TABLE IF NOT EXISTS metrigo.fact_sales
(
    seller_id String,
    sale_id String,
    date_time DateTime,
    seller_art String,
    nm_id UInt64,
    barcode String,
    warehouse String,
    quantity Int32,
    seller_price Float64,
    transfer_to_seller Float64,
    buyers_price Float64,
    is_return UInt8,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (seller_id, sale_id);

CREATE TABLE IF NOT EXISTS metrigo.fact_stocks
(
    seller_id String,
    snapshot_date Date,
    seller_art String,
    nm_id UInt64,
    barcode String,
    warehouse String,
    quantity Int32,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (seller_id, snapshot_date, warehouse, seller_art, barcode);

CREATE TABLE IF NOT EXISTS metrigo.fact_finance
(
    seller_id String,
    report_id String,
    operation_date Date,
    seller_art String,
    nm_id UInt64,
    operation_type String,
    retail_amount Float64,
    for_pay Float64,
    delivery_rub Float64,
    return_rub Float64,
    commission_rub Float64,
    penalty_rub Float64,
    storage_rub Float64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (seller_id, report_id, operation_date, seller_art);

CREATE TABLE IF NOT EXISTS metrigo.etl_state
(
    seller_id String,
    source String,
    watermark String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (seller_id, source);