"""
shop_profit.py — экономика магазина за текущий месяц.

Считает:
- выручку по товарам
- валовую прибыль по товарам
- расходы магазина
- чистую прибыль магазина
"""

from typing import Dict

from app.db import ch


def get_shop_profit(seller_id: str) -> Dict:
    client = ch()

    row = client.query(
        """
        WITH
        sku_profit AS (
            SELECT
                seller_id,
                toStartOfMonth(today()) AS month,
                sum(revenue) AS revenue,
                sum(gross_profit) AS gross_profit
            FROM metrigo.fact_sku_finance_daily
            WHERE seller_id = %(seller_id)s
              AND sale_date >= toStartOfMonth(today())
              AND sale_date < addMonths(toStartOfMonth(today()), 1)
            GROUP BY seller_id
        ),

        shop_expenses AS (
            SELECT
                seller_id,
                month,
                sum(amount) AS expenses
            FROM metrigo.fact_shop_expenses_monthly
            WHERE seller_id = %(seller_id)s
              AND month = toStartOfMonth(today())
            GROUP BY seller_id, month
        )

        SELECT
            toStartOfMonth(today()) AS month,
            ifNull(s.revenue, 0) AS revenue,
            ifNull(s.gross_profit, 0) AS gross_profit,
            ifNull(e.expenses, 0) AS shop_expenses,
            ifNull(s.gross_profit, 0) - ifNull(e.expenses, 0) AS net_profit
        FROM sku_profit s
        FULL OUTER JOIN shop_expenses e
            ON s.seller_id = e.seller_id
           AND s.month = e.month
        LIMIT 1
        """,
        {"seller_id": seller_id},
    ).first_row

    items = client.query(
        """
        SELECT
            expense_type,
            expense_name,
            amount,
            source,
            comment
        FROM metrigo.fact_shop_expenses_monthly
        WHERE seller_id = %(seller_id)s
          AND month = toStartOfMonth(today())
        ORDER BY amount DESC
        """,
        {"seller_id": seller_id},
    ).result_rows

    if not row:
        return {
            "seller_id": seller_id,
            "month": None,
            "revenue": 0,
            "gross_profit": 0,
            "shop_expenses": 0,
            "net_profit": 0,
            "expense_items": [],
        }

    return {
        "seller_id": seller_id,
        "month": row[0].isoformat(),
        "revenue": float(row[1] or 0),
        "gross_profit": float(row[2] or 0),
        "shop_expenses": float(row[3] or 0),
        "net_profit": float(row[4] or 0),
        "expense_items": [
            {
                "expense_type": r[0],
                "expense_name": r[1],
                "amount": float(r[2] or 0),
                "source": r[3],
                "comment": r[4],
            }
            for r in items
        ],
    }