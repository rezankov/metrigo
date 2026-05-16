"""
sku_list.py — список SKU для dashboard.

Источник:
- agg_sku_snapshot — готовые SKU-метрики
- fact_orders / fact_sales — заказы и выкупы за вчера
"""

from app.db import ch


def get_sku_list(
    seller_id: str,
    days: int = 30,
    limit: int = 100,
):
    client = ch()

    rows = client.query(
        """
        WITH

        latest_snapshot_date AS (
            SELECT max(snapshot_date) AS snapshot_date
            FROM metrigo.agg_sku_snapshot
            WHERE seller_id = %(seller_id)s
        ),

        snapshot AS (
            SELECT
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
                margin_percent
            FROM metrigo.agg_sku_snapshot
            WHERE seller_id = %(seller_id)s
              AND snapshot_date = (
                  SELECT snapshot_date
                  FROM latest_snapshot_date
              )
        ),

        orders_yesterday AS (
            SELECT
                seller_id,
                supplier_article AS sku,
                countIf(is_cancel = 0) AS orders_yesterday
            FROM metrigo.fact_orders
            WHERE seller_id = %(seller_id)s
              AND toDate(date_time) = today() - 1
              AND supplier_article != ''
            GROUP BY seller_id, supplier_article
        ),

        buyouts_yesterday AS (
            SELECT
                seller_id,
                seller_art AS sku,
                countIf(op = 'S') AS buyouts_yesterday
            FROM metrigo.fact_sales
            WHERE seller_id = %(seller_id)s
              AND sale_date = today() - 1
              AND seller_art != ''
            GROUP BY seller_id, seller_art
        )

        SELECT
            s.sku,

            round(ifNull(s.avg_price, 0), 2) AS avg_price,
            round(ifNull(s.margin_percent, 0), 2) AS margin_percent,
            round(ifNull(s.profit_per_unit, 0), 2) AS profit_per_unit,
            round(ifNull(s.revenue_30d, 0), 2) AS revenue,

            toUInt32(ifNull(s.stock_qty, 0)) AS stock_qty,
            toUInt32(ifNull(s.stock_qty_full, 0)) AS stock_qty_full,

            toUInt32(ifNull(oy.orders_yesterday, 0)) AS orders_24h,
            toUInt32(ifNull(by.buyouts_yesterday, 0)) AS buyouts_24h,

            toUInt32(ifNull(s.orders_7d, 0)) AS orders_7d,
            toUInt32(ifNull(s.buyouts_7d, 0)) AS buyouts_7d,

            round(ifNull(s.coverage_days, 0), 1) AS coverage_days,

            round(ifNull(s.cogs, 0), 2) AS cogs,
            round(ifNull(s.commission, 0), 2) AS commission,
            round(ifNull(s.acquiring, 0), 2) AS acquiring,
            round(ifNull(s.logistics, 0), 2) AS logistics,
            round(ifNull(s.tax, 0), 2) AS tax

        FROM snapshot s

        LEFT JOIN orders_yesterday oy
            ON s.seller_id = oy.seller_id
           AND s.sku = oy.sku

        LEFT JOIN buyouts_yesterday by
            ON s.seller_id = by.seller_id
           AND s.sku = by.sku

        ORDER BY revenue DESC
        LIMIT %(limit)s
        """,
        {
            "seller_id": seller_id,
            "limit": limit,
        },
    ).result_rows

    return [
        {
            "sku": row[0],
            "avg_price": float(row[1] or 0),
            "margin_percent": float(row[2] or 0),
            "profit_per_unit": float(row[3] or 0),
            "revenue": float(row[4] or 0),
            "stock_qty": int(row[5] or 0),
            "stock_qty_full": int(row[6] or 0),
            "orders_24h": int(row[7] or 0),
            "buyouts_24h": int(row[8] or 0),
            "orders_7d": int(row[9] or 0),
            "buyouts_7d": int(row[10] or 0),
            "coverage_days": float(row[11] or 0),

            # Для совместимости со старым фронтом.
            "current_price": float(row[1] or 0),
            "turnover_30d": float(row[4] or 0),
            "days_cover": float(row[11] or 0),

            # Новая чистая юнит-экономика.
            "cogs": float(row[12] or 0),
            "commission": float(row[13] or 0),
            "acquiring": float(row[14] or 0),
            "logistics": float(row[15] or 0),
            "tax": float(row[16] or 0),
        }
        for row in rows
    ]