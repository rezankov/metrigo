/*
003_finance_marts.sql

Финансовые mart/fact таблицы для SKU-экономики Metrigo.

Слои:
1. fact_sku_finance_daily — честная SKU-экономика по дням.
2. fact_shop_expenses_monthly — общие расходы магазина по месяцам.
3. agg_sku_snapshot — готовый слой для UI/AI по SKU.
*/

DROP TABLE IF EXISTS metrigo.agg_sku_snapshot;
DROP TABLE IF EXISTS metrigo.fact_shop_expenses_monthly;
DROP TABLE IF EXISTS metrigo.fact_sku_finance_daily;


CREATE TABLE metrigo.fact_sku_finance_daily
(
    seller_id String,

    sale_date Date,
    sku String,
    nm_id UInt64,
    barcode String,

    sales_count UInt32,
    returns_count UInt32,

    revenue Decimal(18, 2),
    revenue_returns Decimal(18, 2),

    commission Decimal(18, 2),
    acquiring Decimal(18, 2),
    logistics Decimal(18, 2),

    tax Decimal(18, 2),

    cogs Decimal(18, 2),
    gross_profit Decimal(18, 2),
    margin_percent Float32,

    for_pay Decimal(18, 2),

    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(sale_date)
ORDER BY (
    seller_id,
    sale_date,
    sku
);


CREATE TABLE metrigo.fact_shop_expenses_monthly
(
    seller_id String,

    month Date,

    expense_type LowCardinality(String),
    expense_name String,

    amount Decimal(18, 2),

    source LowCardinality(String),

    comment String,

    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(month)
ORDER BY (
    seller_id,
    month,
    expense_type,
    expense_name,
    source
);


CREATE TABLE metrigo.agg_sku_snapshot
(
    seller_id String,

    sku String,

    snapshot_date Date,

    sales_7d UInt32,
    orders_7d UInt32,
    buyouts_7d UInt32,

    revenue_7d Decimal(18, 2),
    revenue_30d Decimal(18, 2),

    avg_price Decimal(18, 2),

    stock_qty UInt32,
    stock_qty_full UInt32,

    coverage_days Float32,

    cogs Decimal(18, 2),

    commission Decimal(18, 2),
    acquiring Decimal(18, 2),
    logistics Decimal(18, 2),
    tax Decimal(18, 2),

    profit_per_unit Decimal(18, 2),
    margin_percent Float32,

    last_update DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_update)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (
    seller_id,
    snapshot_date,
    sku
);