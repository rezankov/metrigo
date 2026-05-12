#!/usr/bin/env bash
set -e

echo "=== dim_cogs count ==="
docker compose exec clickhouse clickhouse-client --query "
SELECT count() AS rows
FROM metrigo.dim_cogs
FORMAT PrettyCompact;
"

echo
echo "=== fact_fin_report date range ==="
docker compose exec clickhouse clickhouse-client --query "
SELECT
  min(report_date) AS min_report_date,
  max(report_date) AS max_report_date,
  count() AS rows
FROM metrigo.fact_fin_report
FORMAT Vertical;
"

echo
echo "=== fact_fin_report last 7 days totals ==="
docker compose exec clickhouse clickhouse-client --query "
SELECT
  round(sum(ppvz_for_pay), 2) AS wb_for_pay,
  round(sum(retail_amount), 2) AS retail_amount,
  round(sum(retail_price_withdisc_rub), 2) AS retail_price_withdisc_rub,
  round(sum(ppvz_sales_commission), 2) AS commission,
  round(sum(delivery_rub), 2) AS delivery,
  round(sum(storage_fee), 2) AS storage,
  round(sum(deduction), 2) AS deduction,
  round(sum(acceptance), 2) AS acceptance,
  round(sum(penalty), 2) AS penalty,
  round(sum(additional_payment), 2) AS additional_payment,
  round(sum(rebill_logistic_cost), 2) AS rebill_logistic,
  round(sum(acquiring_fee), 2) AS acquiring,
  count() AS rows
FROM metrigo.fact_fin_report
WHERE seller_id = 'main'
  AND report_date >= today() - 6
  AND report_date <= today()
FORMAT Vertical;
"

echo
echo "=== fact_fin_report operation summary ==="
docker compose exec clickhouse clickhouse-client --query "
SELECT
  doc_type_name,
  supplier_oper_name,
  operation_type_name,
  count() AS rows,
  round(sum(quantity), 2) AS quantity,
  round(sum(retail_amount), 2) AS retail_amount,
  round(sum(retail_price_withdisc_rub), 2) AS retail_price_withdisc_rub,
  round(sum(ppvz_for_pay), 2) AS ppvz_for_pay,
  round(sum(ppvz_sales_commission), 2) AS ppvz_sales_commission,
  round(sum(delivery_rub), 2) AS delivery_rub,
  round(sum(storage_fee), 2) AS storage_fee,
  round(sum(deduction), 2) AS deduction,
  round(sum(acceptance), 2) AS acceptance,
  round(sum(penalty), 2) AS penalty,
  round(sum(additional_payment), 2) AS additional_payment,
  round(sum(rebill_logistic_cost), 2) AS rebill_logistic_cost,
  round(sum(acquiring_fee), 2) AS acquiring_fee
FROM metrigo.fact_fin_report
GROUP BY
  doc_type_name,
  supplier_oper_name,
  operation_type_name
ORDER BY rows DESC
LIMIT 80
FORMAT Vertical;
"