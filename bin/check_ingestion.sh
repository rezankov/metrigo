#!/usr/bin/env bash
set -euo pipefail

DB="metrigo"
CH="docker exec -i metrigo_clickhouse clickhouse-client --database ${DB}"

echo
echo "== RAW EVENTS =="
$CH --query "
SELECT
    source,
    count() AS raw_rows
FROM raw_events
WHERE seller_id='main'
GROUP BY source
ORDER BY source
"

echo
echo "== FACT TABLES =="
$CH --query "
SELECT *
FROM
(
    SELECT 'fact_sales' AS table_name, count() AS rows_count FROM fact_sales WHERE seller_id='main'
    UNION ALL SELECT 'fact_orders' AS table_name, count() AS rows_count FROM fact_orders WHERE seller_id='main'
    UNION ALL SELECT 'fact_stock_snapshot' AS table_name, count() AS rows_count FROM fact_stock_snapshot WHERE seller_id='main'
    UNION ALL SELECT 'fact_supplies' AS table_name, count() AS rows_count FROM fact_supplies WHERE seller_id='main'
    UNION ALL SELECT 'fact_supply_items' AS table_name, count() AS rows_count FROM fact_supply_items WHERE seller_id='main'
    UNION ALL SELECT 'fact_prices_discounts_snapshot' AS table_name, count() AS rows_count FROM fact_prices_discounts_snapshot WHERE seller_id='main'
    UNION ALL SELECT 'fact_content_cards_snapshot' AS table_name, count() AS rows_count FROM fact_content_cards_snapshot WHERE seller_id='main'
    UNION ALL SELECT 'fact_tariffs' AS table_name, count() AS rows_count FROM fact_tariffs WHERE seller_id='main'
    UNION ALL SELECT 'fact_ads_campaigns' AS table_name, count() AS rows_count FROM fact_ads_campaigns WHERE seller_id='main'
    UNION ALL SELECT 'fact_ads_stats_daily' AS table_name, count() AS rows_count FROM fact_ads_stats_daily WHERE seller_id='main'
    UNION ALL SELECT 'fact_fin_report' AS table_name, count() AS rows_count FROM fact_fin_report WHERE seller_id='main'
)
ORDER BY table_name
"

echo
echo "== ETL HEALTH =="
$CH --query "
SELECT
    source,
    last_run,
    last_status,
    last_loaded,
    last_message
FROM mart_etl_health
WHERE seller_id='main'
ORDER BY source
FORMAT PrettyCompact
"

echo
echo "== ETL ERRORS =="
$CH --query "
SELECT
    ts,
    source,
    status,
    loaded,
    message
FROM etl_runs
WHERE seller_id='main'
  AND status='error'
ORDER BY ts DESC
LIMIT 20
FORMAT PrettyCompact
"