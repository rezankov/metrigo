"""
build_agg_sku_snapshot.py

Строит актуальный SKU snapshot для UI и AI.

Источник:
- fact_sku_finance_daily
- fact_orders
- fact_sales
- mart_stocks_latest
- dim_cogs
"""

import argparse
import os

from app.db import ch


DEFAULT_SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"


def rebuild(seller_id: str) -> None:
    client = ch()

    print("[start] rebuild agg_sku_snapshot")

    client.command(
        """
        TRUNCATE TABLE metrigo.agg_sku_snapshot
        """
    )

    client.query(
        """
        INSERT INTO metrigo.agg_sku_snapshot
        (
            seller_id,
            sku,
            snapshot_date,
            sales_7d,
            orders_7d,
            buyouts_7d,
            revenue_7d,
            revenue_30d,
            avg_price,
            stock_qty,
            stock_qty_full,
            coverage_days,
            cogs,
            commission,
            acquiring,
            logistics,
            tax,
            profit_per_unit,
            margin_percent,
            last_update
        )
        WITH

        sku_universe AS (
            SELECT seller_id, sku
            FROM metrigo.fact_sku_finance_daily
            WHERE seller_id = %(seller_id)s

            UNION DISTINCT

            SELECT seller_id, seller_art AS sku
            FROM metrigo.mart_stocks_latest
            WHERE seller_id = %(seller_id)s
              AND seller_art != ''

            UNION DISTINCT

            SELECT seller_id, supplier_article AS sku
            FROM metrigo.fact_orders
            WHERE seller_id = %(seller_id)s
              AND supplier_article != ''

            UNION DISTINCT

            SELECT seller_id, seller_art AS sku
            FROM metrigo.fact_sales
            WHERE seller_id = %(seller_id)s
              AND seller_art != ''

            UNION DISTINCT

            SELECT seller_id, seller_art AS sku
            FROM metrigo.dim_cogs
            WHERE seller_id = %(seller_id)s
              AND seller_art != ''
        ),

        finance_7d AS (
            SELECT
                seller_id,
                sku,
                sum(sales_count) AS sales_7d,
                sum(revenue) AS revenue_7d
            FROM metrigo.fact_sku_finance_daily
            WHERE seller_id = %(seller_id)s
              AND sale_date >= today() - 7
            GROUP BY seller_id, sku
        ),

        finance_30d AS (
            SELECT
                seller_id,
                sku,
                sum(sales_count) AS sales_30d,
                sum(revenue) AS revenue_30d,
                sum(commission) AS commission_30d,
                sum(acquiring) AS acquiring_30d,
                sum(logistics) AS logistics_30d,
                sum(tax) AS tax_30d,
                sum(cogs) AS cogs_30d,
                sum(gross_profit) AS gross_profit_30d
            FROM metrigo.fact_sku_finance_daily
            WHERE seller_id = %(seller_id)s
              AND sale_date >= today() - 30
            GROUP BY seller_id, sku
        ),

        orders_7d AS (
            SELECT
                seller_id,
                supplier_article AS sku,
                countIf(is_cancel = 0) AS orders_7d
            FROM metrigo.fact_orders
            WHERE seller_id = %(seller_id)s
              AND toDate(date_time) >= today() - 7
              AND supplier_article != ''
            GROUP BY seller_id, supplier_article
        ),

        buyouts_7d AS (
            SELECT
                seller_id,
                seller_art AS sku,
                countIf(op = 'S') AS buyouts_7d
            FROM metrigo.fact_sales
            WHERE seller_id = %(seller_id)s
              AND sale_date >= today() - 7
              AND seller_art != ''
            GROUP BY seller_id, seller_art
        ),

        stocks AS (
            SELECT
                seller_id,
                seller_art AS sku,
                sum(qty) AS stock_qty,
                sum(qty_full) AS stock_qty_full
            FROM metrigo.mart_stocks_latest
            WHERE seller_id = %(seller_id)s
            GROUP BY seller_id, seller_art
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
        )

        SELECT
            u.seller_id,
            u.sku,
            today() AS snapshot_date,

            toUInt32(ifNull(f7.sales_7d, 0)) AS sales_7d,
            toUInt32(ifNull(o7.orders_7d, 0)) AS orders_7d,
            toUInt32(ifNull(b7.buyouts_7d, 0)) AS buyouts_7d,

            toDecimal64(ifNull(f7.revenue_7d, 0), 2) AS revenue_7d,
            toDecimal64(ifNull(f30.revenue_30d, 0), 2) AS revenue_30d,

            toDecimal64(
                if(
                    ifNull(f30.sales_30d, 0) > 0,
                    ifNull(f30.revenue_30d, 0) / f30.sales_30d,
                    0
                ),
                2
            ) AS avg_price,

            toUInt32(ifNull(st.stock_qty, 0)) AS stock_qty,
            toUInt32(ifNull(st.stock_qty_full, 0)) AS stock_qty_full,

            toFloat32(
                if(
                    ifNull(b7.buyouts_7d, 0) > 0,
                    ifNull(st.stock_qty, 0) / (b7.buyouts_7d / 7),
                    0
                )
            ) AS coverage_days,

            toDecimal64(ifNull(c.cost_per_unit, 0), 2) AS cogs,

            toDecimal64(
                if(ifNull(f30.sales_30d, 0) > 0, ifNull(f30.commission_30d, 0) / f30.sales_30d, 0),
                2
            ) AS commission,

            toDecimal64(
                if(ifNull(f30.sales_30d, 0) > 0, ifNull(f30.acquiring_30d, 0) / f30.sales_30d, 0),
                2
            ) AS acquiring,

            toDecimal64(
                if(ifNull(f30.sales_30d, 0) > 0, ifNull(f30.logistics_30d, 0) / f30.sales_30d, 0),
                2
            ) AS logistics,

            toDecimal64(
                if(ifNull(f30.sales_30d, 0) > 0, ifNull(f30.tax_30d, 0) / f30.sales_30d, 0),
                2
            ) AS tax,

            toDecimal64(
                if(ifNull(f30.sales_30d, 0) > 0, ifNull(f30.gross_profit_30d, 0) / f30.sales_30d, 0),
                2
            ) AS profit_per_unit,

            toFloat32(
                if(
                    ifNull(f30.revenue_30d, 0) > 0,
                    ifNull(f30.gross_profit_30d, 0) / f30.revenue_30d * 100,
                    0
                )
            ) AS margin_percent,

            now() AS last_update

        FROM sku_universe u

        LEFT JOIN finance_7d f7
            ON u.seller_id = f7.seller_id
           AND u.sku = f7.sku

        LEFT JOIN finance_30d f30
            ON u.seller_id = f30.seller_id
           AND u.sku = f30.sku

        LEFT JOIN orders_7d o7
            ON u.seller_id = o7.seller_id
           AND u.sku = o7.sku

        LEFT JOIN buyouts_7d b7
            ON u.seller_id = b7.seller_id
           AND u.sku = b7.sku

        LEFT JOIN stocks st
            ON u.seller_id = st.seller_id
           AND u.sku = st.sku

        LEFT JOIN cogs c
            ON u.seller_id = c.seller_id
           AND u.sku = c.sku

        WHERE u.sku != ''
        """,
        {"seller_id": seller_id},
    )

    rows = client.query(
        """
        SELECT count()
        FROM metrigo.agg_sku_snapshot
        WHERE seller_id = %(seller_id)s
        """,
        {"seller_id": seller_id},
    ).result_rows[0][0]

    print(f"[ok] inserted rows: {rows}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seller-id", default=DEFAULT_SELLER_ID)

    args = parser.parse_args()

    rebuild(seller_id=args.seller_id)


if __name__ == "__main__":
    main()