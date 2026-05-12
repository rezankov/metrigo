"""
get_header_metrics.py — метрики для верхней шапки Metrigo.
"""

from typing import Dict
from app.db import ch


TAX_RATE = 0.06
MOSCOW_TODAY = "toDate(now('Europe/Moscow'))"


def get_header_metrics(seller_id: str, days: int = 7) -> Dict:
    """
    Получить метрики для шапки за период.

    Возвращает:
    - orders_count: количество заказов
    - buyouts_count: количество выкупов/продаж
    - revenue: выручка
    - tax: налог 6%
    - revenue_after_tax: выручка после налога
    """

    client = ch()
    days = max(1, min(int(days or 7), 90))

    sales_row = client.query(
        f"""
        SELECT
            countIf(op = 'S') AS buyouts_count,
            round(sumIf(seller_price, op = 'S'), 2) AS revenue
        FROM metrigo.fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date >= {MOSCOW_TODAY} - %(days)s + 1
          AND sale_date <= {MOSCOW_TODAY}
        """,
        {
            "seller_id": seller_id,
            "days": days,
        },
    ).result_rows[0]

    buyouts_count = int(sales_row[0] or 0)
    revenue = float(sales_row[1] or 0)

    orders_row = client.query(
        f"""
        SELECT
            countIf(is_cancel = 0) AS orders_count
        FROM metrigo.fact_orders
        WHERE seller_id = %(seller_id)s
          AND toDate(date_time, 'Europe/Moscow') >= {MOSCOW_TODAY} - %(days)s + 1
          AND toDate(date_time, 'Europe/Moscow') <= {MOSCOW_TODAY}
        """,
        {
            "seller_id": seller_id,
            "days": days,
        },
    ).result_rows[0]

    orders_count = int(orders_row[0] or 0)

    tax = round(revenue * TAX_RATE, 2)
    revenue_after_tax = round(revenue - tax, 2)

    return {
        "seller_id": seller_id,
        "days": days,
        "orders_count": orders_count,
        "buyouts_count": buyouts_count,
        "revenue": revenue,
        "tax": tax,
        "revenue_after_tax": revenue_after_tax,
    }