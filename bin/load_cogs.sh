#!/usr/bin/env bash
set -e

cd /opt/metrigo

docker compose exec -T clickhouse clickhouse-client --query "
INSERT INTO metrigo.dim_cogs
SELECT
  'main' AS seller_id,
  toFixedString(hex(MD5(concat('main', seller_art, barcode, wb_art, toString(cost_per_unit)))), 32) AS cogs_key,
  seller_art,
  barcode,
  toUInt64(nm_id) AS nm_id,
  '' AS supply_id,
  '' AS delivery_number,
  'manual' AS batch_name,
  toFloat64(cost_per_unit) AS cost_per_unit,
  'RUB' AS currency,
  concat('wb_art=', wb_art) AS comment,
  1 AS is_active,
  toDate('2026-01-01') AS valid_from,
  NULL AS valid_to,
  now() AS loaded_at
FROM input(
  'seller_art String, barcode String, wb_art String, nm_id UInt64, cost_per_unit Float64'
)
FORMAT CSVWithNames
" < data/cogs/cogs_template.csv

echo "COGS loaded"