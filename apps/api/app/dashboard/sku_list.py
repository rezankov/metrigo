"""
sku_list.py — список SKU для dashboard Metrigo.

Возвращает:
- SKU
- остатки
- продажи
- дни покрытия
- текущую цену
- маржинальность
- оборот за период
"""

from datetime import date, timedelta
from typing import Dict, List

from app.db import ch


def get_sku_list(
    seller_id: str,
    days: int = 30,
) -> Dict:
    """
    Получить dashboard-список SKU.
    """

    client = ch()

    days = max(1, min(int(days or 30), 120))
    start_date = (date.today() - timedelta(days=days)).isoformat()

    rows = client.query(
        """
        WITH sales AS (
            SELECT
                seller_id,
                seller_art,
                round(sumIf(seller_price, op='S'), 2) AS turnover,
                countIf(op='S') AS sales_count,
                round(toFloat64(countIf(op='S')) / 14, 2) AS avg_sales_per_day
            FROM metrigo.fact_sales
            WHERE seller_id = %(seller_id)s
              AND sale_date >= today() - 14
            GROUP BY seller_id, seller_art
        ),

        stocks AS (
            SELECT
                seller_id,
                seller_art,
                sum(qty) AS stock_qty
            FROM metrigo.mart_stocks_latest
            WHERE seller_id = %(seller_id)s
            GROUP BY seller_id, seller_art
        ),

        prices AS (
            SELECT
                seller_id,
                seller_art,
                round(avg(seller_price), 2) AS current_price
            FROM metrigo.fact_sales
            WHERE seller_id = %(seller_id)s
              AND sale_date >= %(start_date)s
              AND op = 'S'
            GROUP BY seller_id, seller_art
        ),

        cogs AS (
            SELECT
                seller_id,
                seller_art,
                round(avg(cost_per_unit), 2) AS cost_per_unit
            FROM metrigo.dim_cogs
            WHERE seller_id = %(seller_id)s
              AND is_active = 1
            GROUP BY seller_id, seller_art
        )

        SELECT
            s.seller_art AS sku,
            toInt32(ifNull(st.stock_qty, 0)) AS stock_qty,
            toInt32(ifNull(s.sales_count, 0)) AS sales_14d,
            round(ifNull(s.avg_sales_per_day, 0), 2) AS avg_sales_per_day,
            round(if(s.avg_sales_per_day > 0, st.stock_qty / s.avg_sales_per_day, 0), 1) AS days_cover,
            round(ifNull(p.current_price, 0), 2) AS current_price,
            round(if(p.current_price > 0, ((p.current_price - ifNull(c.cost_per_unit, 0)) / p.current_price) * 100, 0), 2) AS margin_percent,
            round(ifNull(s.turnover, 0), 2) AS turnover_30d
        FROM sales s
        LEFT JOIN stocks st
            ON s.seller_id = st.seller_id
           AND s.seller_art = st.seller_art
        LEFT JOIN prices p
            ON s.seller_id = p.seller_id
           AND s.seller_art = p.seller_art
        LEFT JOIN cogs c
            ON s.seller_id = c.seller_id
           AND s.seller_art = c.seller_art
        ORDER BY turnover_30d DESC
        """,
        {
            "seller_id": seller_id,
            "start_date": start_date,
        },
    ).result_rows

    items: List[Dict] = []

    for row in rows:
        items.append(
            {
                "sku": row[0],
                "stock_qty": row[1],
                "sales_14d": row[2],
                "avg_sales_per_day": row[3],
                "days_cover": row[4],
                "current_price": row[5],
                "margin_percent": row[6],
                "turnover_30d": row[7],
            }
        )

    return {
        "seller_id": seller_id,
        "days": days,
        "items": items,
    }