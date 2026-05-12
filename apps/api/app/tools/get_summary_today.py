"""
Инструмент: get_summary_today.

Возвращает сводный отчёт на сегодня по бизнес-дате Europe/Moscow:
- заказы из fact_orders
- выкупы/выручка из fact_sales
- расходы на рекламу
- DRR
- бизнес-здоровье
"""

import os
from typing import Dict

from app.db import ch
from app.tools.get_business_health import get_business_health


SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"


def get_summary_today(seller_id: str = SELLER_ID) -> Dict:
    """
    Сформировать сводный отчёт на сегодня.
    """

    client = ch()

    today = client.query(
        """
        SELECT toDate(now('Europe/Moscow')) AS today
        """
    ).result_rows[0][0]

    sales_rows = client.query(
        """
        SELECT
            countIf(op = 'S') AS sales_count,
            round(sumIf(seller_price, op = 'S'), 2) AS revenue
        FROM metrigo.fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date = %(today)s
        """,
        {
            "seller_id": seller_id,
            "today": today,
        },
    ).result_rows

    sales_count, revenue = sales_rows[0] if sales_rows else (0, 0.0)
    revenue = float(revenue or 0.0)

    orders_rows = client.query(
        """
        SELECT countIf(is_cancel = 0) AS orders_count
        FROM metrigo.fact_orders
        WHERE seller_id = %(seller_id)s
          AND toDate(date_time, 'Europe/Moscow') = %(today)s
        """,
        {
            "seller_id": seller_id,
            "today": today,
        },
    ).result_rows

    orders_count = int(orders_rows[0][0] or 0) if orders_rows else 0

    ads_rows = client.query(
        """
        SELECT round(sum(spend), 2) AS ad_spend
        FROM metrigo.fact_ads_stats_daily
        WHERE seller_id = %(seller_id)s
          AND stat_date = %(today)s
        """,
        {
            "seller_id": seller_id,
            "today": today,
        },
    ).result_rows

    ad_spend = float(ads_rows[0][0] or 0.0) if ads_rows else 0.0

    drr = (ad_spend / revenue * 100) if revenue > 0 else 0

    health_info = get_business_health(seller_id)
    system_status = health_info.get("status", "ok")
    health_comment = health_info.get("comment", "")
    health_details = health_info.get("details", {})

    if sales_count == 0 and orders_count == 0:
        priority = "warning"
        summary_text = (
            "Сегодня пока нет продаж и заказов. "
            "Стоит проверить остатки, рекламу и доступность карточек."
        )
    elif sales_count == 0 and orders_count > 0:
        priority = "warning"
        summary_text = (
            f"Сегодня уже {orders_count} заказов, но выкупов пока нет. "
            "Это нормально для начала дня, но стоит следить за динамикой."
        )
    elif drr >= 20:
        priority = "warning"
        summary_text = (
            f"Сегодня {orders_count} заказов, {sales_count} выкупов "
            f"и {revenue:,.0f} ₽ выручки. ДРР высокий — {drr:.2f}%."
        ).replace(",", " ")
    else:
        priority = "ok"
        summary_text = (
            f"Сегодня {orders_count} заказов, {sales_count} выкупов, "
            f"выручка {revenue:,.0f} ₽, расход рекламы {ad_spend:,.0f} ₽, "
            f"ДРР {drr:.2f}%."
        ).replace(",", " ")

    return {
        "seller_id": seller_id,
        "date": str(today),
        "sales_count": int(sales_count or 0),
        "orders_count": orders_count,
        "revenue": round(revenue, 2),
        "ad_spend": round(ad_spend, 2),
        "drr": round(drr, 2),
        "system_status": system_status,
        "health_comment": health_comment,
        "health_details": health_details,
        "priority": priority,
        "summary_text": summary_text,
        "risks": [],
        "suggested_actions": [
            "Что сегодня важно?",
            "Остатки",
            "Реклама",
            "Что заказать?",
        ],
    }