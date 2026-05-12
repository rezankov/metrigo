"""
get_sales_mini_chart.py — мини-график продаж для шапки Metrigo.
"""

from datetime import timedelta
from typing import Dict, List

from app.db import ch


def get_sales_mini_chart(seller_id: str, days: int = 60) -> Dict:
    """
    Получить продажи по дням для мини-графика.

    Бизнес-день считаем по Europe/Moscow.
    """

    client = ch()
    days = max(1, min(int(days or 60), 120))

    date_row = client.query(
        """
        SELECT
            toDate(now('Europe/Moscow')) AS today,
            today - %(days)s + 1 AS start_date
        """,
        {"days": days},
    ).result_rows[0]

    today = date_row[0]
    start_date = date_row[1]

    rows = client.query(
        """
        SELECT
            sale_date,
            round(sumIf(seller_price, op = 'S'), 2) AS revenue
        FROM metrigo.fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date >= %(start_date)s
          AND sale_date <= %(today)s
        GROUP BY sale_date
        ORDER BY sale_date
        """,
        {
            "seller_id": seller_id,
            "start_date": start_date,
            "today": today,
        },
    ).result_rows

    revenue_by_date = {
        row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]): float(row[1] or 0)
        for row in rows
    }

    labels: List[str] = []
    values: List[float] = []

    for i in range(days):
        current_date = start_date + timedelta(days=i)
        key = current_date.isoformat()

        labels.append(key)
        values.append(revenue_by_date.get(key, 0.0))

    return {
        "labels": labels,
        "values": values,
        "max_value": max(values) if values else 0.0,
        "days": days,
    }