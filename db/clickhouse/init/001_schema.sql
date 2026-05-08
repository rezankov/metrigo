-- ============================================================================
-- Metrigo ClickHouse Schema v1
-- Multi-tenant WB analytics storage
-- ============================================================================

CREATE DATABASE IF NOT EXISTS metrigo;

-- ----------------------------------------------------------------------------
-- RAW EVENTS
-- Сырые ответы WB API. Это источник истины для пересборки fact-таблиц.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.raw_events
(
    seller_id String,
    source LowCardinality(String),

    event_dt Nullable(DateTime),
    payload String,

    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (seller_id, source, dedup_key);


-- ----------------------------------------------------------------------------
-- ETL STATE
-- Watermark по каждому seller_id + source.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.etl_state
(
    seller_id String,
    source LowCardinality(String),
    watermark DateTime,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (seller_id, source);


-- ----------------------------------------------------------------------------
-- ETL RUNS
-- Логи запусков сборщиков.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.etl_runs
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


-- ----------------------------------------------------------------------------
-- FACT SALES
-- Продажи и возвраты WB.
-- Важно: seller_price используем для налоговой базы 6%.
-- transfer_to_seller показывает сумму к перечислению продавцу.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.fact_sales
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

    payload_hash FixedString(32),
    dedup_key FixedString(32),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(sale_date)
ORDER BY (seller_id, dedup_key);

-- ----------------------------------------------------------------------------
-- FACT ORDERS
-- Заказы WB. Нужны для анализа спроса до фактической продажи.
-- Watermark ведём по last_change_date.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.fact_orders
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
ORDER BY (seller_id, dedup_key);


-- ----------------------------------------------------------------------------
-- FACT STOCK SNAPSHOT
-- Снимки остатков WB. Это не событие, а состояние склада на момент загрузки.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.fact_stock_snapshot
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
    qty_full Int32 DEFAULT 0,

    payload_hash FixedString(32),
    dedup_key FixedString(32)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, dedup_key);



-- ----------------------------------------------------------------------------
-- FACT FIN REPORT
-- Финансовый отчёт WB.
-- Главный источник:
-- - комиссий
-- - логистики
-- - штрафов
-- - выплат
-- - удержаний
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.fact_fin_report
(
    seller_id String,

    report_date Date,

    rr_dt Nullable(DateTime),
    rr_id UInt64,
    rrd_id UInt64,

    doc_type_name String,
    supplier_oper_name String,

    operation_type String,
    operation_type_name String,

    srid String,

    nm_id UInt64,
    sa_name String,
    ts_name String,
    brand_name String,

    subject_name String,
    supplier_article String,
    barcode String,

    quantity Int32,

    retail_price Float64,
    retail_amount Float64,

    sale_percent Float64,
    commission_percent Float64,

    office_name String,
    warehouse String,

    supplier_oper_dt Nullable(DateTime),
    order_dt Nullable(DateTime),
    sale_dt Nullable(DateTime),

    shk_id String,

    retail_price_withdisc_rub Float64,
    delivery_amount Int32,

    return_amount Int32,

    delivery_rub Float64,
    gi_box_type_name String,

    product_discount_for_report Float64,
    supplier_promo Float64,

    rid String,

    ppvz_spp_prc Float64,
    ppvz_kvw_prc_base Float64,
    ppvz_kvw_prc Float64,

    sup_rating_prc_up Float64,
    is_kgvp_v2 Float64,
    ppvz_sales_commission Float64,

    ppvz_for_pay Float64,

    ppvz_reward Float64,
    acquiring_fee Float64,

    acquiring_percent Float64,
    acquiring_bank String,

    ppvz_vw Float64,
    ppvz_vw_nds Float64,

    ppvz_office_id Int64,
    ppvz_office_name String,

    ppvz_supplier_id Int64,
    ppvz_supplier_name String,

    ppvz_inn String,

    declaration_number String,

    bonus_type_name String,
    sticker_id String,

    site_country String,

    penalty Float64,
    additional_payment Float64,

    rebill_logistic_cost Float64,
    rebill_logistic_org Float64,

    kiz String,

    storage_fee Float64,
    deduction Float64,

    acceptance Float64,

    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(report_date)
ORDER BY (seller_id, dedup_key);


-- ----------------------------------------------------------------------------
-- DIM COGS
-- Себестоимость.
-- Позже поддержим:
-- - партии
-- - разные поставки
-- - интервалы действия
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrigo.dim_cogs
(
    seller_id String,
    cogs_key FixedString(32),

    seller_art String,
    barcode String DEFAULT '',
    nm_id UInt64 DEFAULT 0,

    supply_id String DEFAULT '',
    delivery_number String DEFAULT '',
    batch_name String DEFAULT '',

    cost_per_unit Float64,

    currency LowCardinality(String) DEFAULT 'RUB',

    comment String DEFAULT '',

    is_active UInt8 DEFAULT 1,

    valid_from Date DEFAULT toDate('1970-01-01'),
    valid_to Nullable(Date),

    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (
    seller_id,
    seller_art,
    barcode,
    valid_from,
    cogs_key
);



-- ----------------------------------------------------------------------------
-- MART STOCKS LATEST
-- Последний снимок остатков по SKU.
-- ----------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS metrigo.mart_stocks_latest AS
SELECT
    s.seller_id,
    s.warehouse,
    s.seller_art,
    s.barcode,
    sum(s.qty) AS qty,
    sum(s.qty_full) AS qty_full,
    sum(s.in_way_to_client) AS in_way_to_client,
    sum(s.in_way_from_client) AS in_way_from_client,
    max(s.snapshot_dt) AS snapshot_dt
FROM metrigo.fact_stock_snapshot s
INNER JOIN
(
    SELECT
        seller_id,
        max(snapshot_dt) AS snapshot_dt
    FROM metrigo.fact_stock_snapshot
    GROUP BY seller_id
) latest
    ON s.seller_id = latest.seller_id
   AND s.snapshot_dt = latest.snapshot_dt
GROUP BY
    s.seller_id,
    s.warehouse,
    s.seller_art,
    s.barcode;


-- ----------------------------------------------------------------------------
-- MART ETL HEALTH
-- Последние запуски сборщиков.
-- ----------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS metrigo.mart_etl_health AS
SELECT
    seller_id,
    source,
    max(ts) AS last_run,
    argMax(status, ts) AS last_status,
    argMax(loaded, ts) AS last_loaded,
    argMax(message, ts) AS last_message
FROM metrigo.etl_runs
GROUP BY
    seller_id,
    source;


