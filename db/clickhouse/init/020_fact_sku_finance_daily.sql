CREATE TABLE IF NOT EXISTS metrigo.fact_sku_finance_daily
(
    seller_id String,

    sale_date Date,

    sku String,

    nm_id UInt64,
    barcode String,

    sales_count Int32,
    returns_count Int32,

    revenue Float64,
    revenue_returns Float64,

    commission Float64,
    acquiring Float64,

    logistics Float64,
    storage Float64,

    penalty Float64,
    deduction Float64,
    acceptance Float64,

    for_pay Float64,

    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(sale_date)
ORDER BY (
    seller_id,
    sale_date,
    sku
);