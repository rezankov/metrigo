SELECT
    table,
    name,
    type
FROM system.columns
WHERE database = 'metrigo'
  AND table IN (
      'fact_sales',
      'fact_fin_report',
      'dim_cogs',
      'agg_sku_snapshot'
  )
ORDER BY table, position
FORMAT PrettyCompact;