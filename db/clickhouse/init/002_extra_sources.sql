-- ============================================================================
-- Metrigo ClickHouse Schema v2 extra WB sources
-- ============================================================================

CREATE TABLE IF NOT EXISTS metrigo.fact_supplies
(
    seller_id String,

    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),

    supply_id String,
    supply_status String DEFAULT '',
    supply_type String DEFAULT '',

    warehouse_id UInt64 DEFAULT 0,
    warehouse_name String DEFAULT '',

    created_at Nullable(DateTime),
    closed_at Nullable(DateTime),

    payload String,
    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, dedup_key);


CREATE TABLE IF NOT EXISTS metrigo.fact_supply_items
(
    seller_id String,

    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),

    supply_id String,

    nm_id UInt64 DEFAULT 0,
    barcode String DEFAULT '',
    seller_art String DEFAULT '',

    quantity Int32 DEFAULT 0,
    quantity_plan Int32 DEFAULT 0,
    quantity_fact Int32 DEFAULT 0,
    item_status String DEFAULT '',

    warehouse_id UInt64 DEFAULT 0,
    warehouse_name String DEFAULT '',

    payload String,
    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, dedup_key);


CREATE TABLE IF NOT EXISTS metrigo.fact_prices_discounts_snapshot
(
    seller_id String,

    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),

    nm_id UInt64 DEFAULT 0,
    vendor_code String DEFAULT '',
    barcode String DEFAULT '',
    size_id UInt64 DEFAULT 0,

    price Float64 DEFAULT 0,
    discount Int32 DEFAULT 0,
    discounted_price Float64 DEFAULT 0,
    club_discount Int32 DEFAULT 0,

    payload String,
    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, dedup_key);


CREATE TABLE IF NOT EXISTS metrigo.fact_content_cards_snapshot
(
    seller_id String,

    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),

    nm_id UInt64 DEFAULT 0,
    imt_id UInt64 DEFAULT 0,
    vendor_code String DEFAULT '',

    subject_id UInt64 DEFAULT 0,
    subject_name String DEFAULT '',
    brand String DEFAULT '',
    title String DEFAULT '',

    media_count UInt32 DEFAULT 0,
    updated_at Nullable(DateTime),

    payload String,
    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, dedup_key);


CREATE TABLE IF NOT EXISTS metrigo.fact_tariffs
(
    seller_id String,

    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),

    tariff_date Date DEFAULT snapshot_date,

    tariff_type LowCardinality(String),

    warehouse_id UInt64 DEFAULT 0,
    warehouse_name String DEFAULT '',
    box_type_name String DEFAULT '',

    coefficient Float64 DEFAULT 0,
    delivery_base Float64 DEFAULT 0,
    delivery_liter Float64 DEFAULT 0,
    storage_base Float64 DEFAULT 0,
    storage_liter Float64 DEFAULT 0,
    acceptance_base Float64 DEFAULT 0,

    payload String,
    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, dedup_key);


CREATE TABLE IF NOT EXISTS metrigo.fact_ads_campaigns
(
    seller_id String,

    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),

    advert_id UInt64,
    advert_name String DEFAULT '',
    advert_type String DEFAULT '',
    status String DEFAULT '',
    payment_type String DEFAULT '',
    daily_budget Float64 DEFAULT 0,

    created_at Nullable(DateTime),
    started_at Nullable(DateTime),
    ended_at Nullable(DateTime),

    payload String,
    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (seller_id, dedup_key);


CREATE TABLE IF NOT EXISTS metrigo.fact_ads_stats_daily
(
    seller_id String,
    snapshot_dt DateTime,
    snapshot_date Date DEFAULT toDate(snapshot_dt),

    stat_date Date,

    advert_id UInt64,
    nm_id UInt64 DEFAULT 0,

    views UInt64 DEFAULT 0,
    clicks UInt64 DEFAULT 0,
    ctr Float64 DEFAULT 0,
    cpc Float64 DEFAULT 0,
    spend Float64 DEFAULT 0,

    orders UInt64 DEFAULT 0,
    shks UInt64 DEFAULT 0,
    sum_price Float64 DEFAULT 0,
    canceled UInt64 DEFAULT 0,

    payload String,
    payload_hash FixedString(32),
    dedup_key FixedString(32),

    version UInt64,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(stat_date)
ORDER BY (seller_id, dedup_key);