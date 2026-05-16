"""
build_fact_sku_finance_daily.py

Строит чистую SKU-экономику по дням.

Выручка, продажи, возвраты и налоговая база берутся из fact_sales.
WB-расходы берутся из fact_fin_report.
"""

import argparse
import os

from app.db import ch


DEFAULT_SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"
DEFAULT_TAX_RATE = float(os.getenv("DEFAULT_TAX_RATE", "0.06"))


def rebuild(seller_id: str, tax_rate: float) -> None:
    client = ch()

    print("[start] rebuild fact_sku_finance_daily")

    client.command("TRUNCATE TABLE metrigo.fact_sku_finance_daily")

    client.query(
        """
        INSERT INTO metrigo.fact_sku_finance_daily
        (
            seller_id,
            sale_date,
            sku,
            nm_id,
            barcode,
            sales_count,
            returns_count,
            revenue,
            revenue_returns,
            commission,
            acquiring,
            logistics,
            tax,
            cogs,
            gross_profit,
            margin_percent,
            for_pay,
            created_at
        )
        WITH

        sales AS (
            SELECT
                seller_id,
                sale_date,
                seller_art AS sku,
                toUInt64(max(toUInt64OrZero(wb_art))) AS nm_id,
                any(barcode) AS barcode,

                toUInt32(countIf(op = 'S')) AS sales_count,
                toUInt32(countIf(op = 'R')) AS returns_count,

                sumIf(seller_price, op = 'S') AS revenue,
                abs(sumIf(seller_price, op = 'R')) AS revenue_returns
            FROM metrigo.fact_sales
            WHERE seller_id = %(seller_id)s
              AND seller_art != ''
            GROUP BY
                seller_id,
                sale_date,
                seller_art
        ),

        fin AS (
            SELECT
                seller_id,
                toDate(coalesce(sale_dt, rr_dt, supplier_oper_dt, toDateTime(report_date))) AS sale_date,
                sa_name AS sku,

                sumIf(
                    ifNull(retail_price_withdisc_rub, 0)
                    - ifNull(ppvz_for_pay, 0)
                    - ifNull(acquiring_fee, 0),
                    supplier_oper_name = 'Продажа'
                    AND doc_type_name = 'Продажа'
                    AND retail_price_withdisc_rub > 0
                ) AS commission,

                sumIf(
                    ifNull(acquiring_fee, 0),
                    supplier_oper_name = 'Продажа'
                    AND doc_type_name = 'Продажа'
                    AND retail_price_withdisc_rub > 0
                ) AS acquiring,

                sum(ifNull(delivery_rub, 0))
                + sum(ifNull(rebill_logistic_cost, 0)) AS logistics,

                sum(ifNull(ppvz_for_pay, 0)) AS for_pay
            FROM metrigo.fact_fin_report
            WHERE seller_id = %(seller_id)s
              AND sa_name != ''
            GROUP BY
                seller_id,
                sale_date,
                sku
        ),

        cogs AS (
            SELECT
                seller_id,
                seller_art AS sku,
                avg(cost_per_unit) AS cost_per_unit
            FROM metrigo.dim_cogs
            WHERE seller_id = %(seller_id)s
              AND is_active = 1
              AND seller_art != ''
            GROUP BY seller_id, seller_art
        ),

        joined AS (
            SELECT
                s.seller_id AS seller_id,
                s.sale_date AS sale_date,
                s.sku AS sku,
                s.nm_id AS nm_id,
                s.barcode AS barcode,

                s.sales_count AS sales_count,
                s.returns_count AS returns_count,

                s.revenue AS revenue,
                s.revenue_returns AS revenue_returns,

                ifNull(f.commission, 0) AS commission,
                ifNull(f.acquiring, 0) AS acquiring,
                ifNull(f.logistics, 0) AS logistics,
                ifNull(f.for_pay, 0) AS for_pay,

                ifNull(c.cost_per_unit, 0) AS cost_per_unit,

                (s.revenue - s.revenue_returns) AS net_revenue,
                (toInt32(s.sales_count) - toInt32(s.returns_count)) AS net_units
            FROM sales s

            LEFT JOIN fin f
                ON s.seller_id = f.seller_id
               AND s.sale_date = f.sale_date
               AND s.sku = f.sku

            LEFT JOIN cogs c
                ON s.seller_id = c.seller_id
               AND s.sku = c.sku
        )

        SELECT
            seller_id,
            sale_date,
            sku,
            nm_id,
            barcode,

            sales_count,
            returns_count,

            toDecimal64(revenue, 2) AS revenue,
            toDecimal64(revenue_returns, 2) AS revenue_returns,

            toDecimal64(commission, 2) AS commission,
            toDecimal64(acquiring, 2) AS acquiring,
            toDecimal64(logistics, 2) AS logistics,

            toDecimal64(net_revenue * %(tax_rate)s, 2) AS tax,

            toDecimal64(cost_per_unit * net_units, 2) AS cogs,

            toDecimal64(
                net_revenue
                - commission
                - acquiring
                - logistics
                - net_revenue * %(tax_rate)s
                - cost_per_unit * net_units,
                2
            ) AS gross_profit,

            toFloat32(
                if(
                    net_revenue > 0,
                    (
                        net_revenue
                        - commission
                        - acquiring
                        - logistics
                        - net_revenue * %(tax_rate)s
                        - cost_per_unit * net_units
                    ) / net_revenue * 100,
                    0
                )
            ) AS margin_percent,

            toDecimal64(for_pay, 2) AS for_pay,

            now() AS created_at

        FROM joined
        """,
        {
            "seller_id": seller_id,
            "tax_rate": tax_rate,
        },
    )

    rows = client.query(
        """
        SELECT count()
        FROM metrigo.fact_sku_finance_daily
        WHERE seller_id = %(seller_id)s
        """,
        {"seller_id": seller_id},
    ).result_rows[0][0]

    print(f"[ok] inserted rows: {rows}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seller-id", default=DEFAULT_SELLER_ID)
    parser.add_argument("--tax-rate", type=float, default=DEFAULT_TAX_RATE)

    args = parser.parse_args()

    rebuild(
        seller_id=args.seller_id,
        tax_rate=args.tax_rate,
    )


if __name__ == "__main__":
    main()