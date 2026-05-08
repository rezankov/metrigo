"""
Metrigo API entrypoint.
"""

import os
from datetime import date

from fastapi import FastAPI

from app.db import ch


SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"

app = FastAPI(title="Metrigo API")


@app.get("/health")
def health():
    """
    Return API health status.
    """
    return {"status": "ok", "service": "metrigo-api"}


def build_summary(
    sales_count: int,
    orders_count: int,
    revenue: float,
    ad_spend: float,
    drr: float,
) -> dict:
    """
    Build rule-based business summary for chat home.

    Это первый слой intelligence без LLM:
    backend сам формирует summary, risks и suggested actions.
    """
    risks = []
    suggested_actions = [
        "Что сегодня важно?",
        "Остатки",
        "Реклама",
        "Что заказать?",
    ]

    if sales_count == 0 and orders_count == 0:
        priority = "warning"
        summary_text = (
            "Сегодня пока нет продаж и заказов. "
            "Стоит проверить остатки, рекламу и доступность карточек."
        )
        risks.append("Нет продаж и заказов сегодня")
        suggested_actions = [
            "Проверь остатки",
            "Проверь рекламу",
            "Покажи заказы",
            "Найди проблему",
        ]

    elif drr >= 20:
        priority = "warning"
        summary_text = (
            f"Сегодня уже {sales_count} продаж и {revenue:,.0f} ₽ выручки. "
            f"Но реклама потратила {ad_spend:,.0f} ₽, "
            f"ДРР высокий — {drr:.2f}%. Стоит проверить эффективность кампаний."
        ).replace(",", " ")
        risks.append("Высокий ДРР")
        suggested_actions = [
            "Покажи рекламу",
            "Какие кампании дорогие?",
            "Сравни ДРР",
            "Что отключить?",
        ]

    elif sales_count > 0 and orders_count == 0:
        priority = "normal"
        summary_text = (
            f"Сегодня есть {sales_count} продаж на {revenue:,.0f} ₽. "
            "Новых заказов пока нет. Стоит посмотреть динамику по часам."
        ).replace(",", " ")
        suggested_actions = [
            "Покажи продажи",
            "Покажи заказы",
            "Сравни со вчера",
            "Остатки",
        ]

    else:
        priority = "ok"
        summary_text = (
            f"Сегодня уже {sales_count} продаж и {revenue:,.0f} ₽ выручки. "
            f"Заказов сегодня: {orders_count}. "
            f"Реклама потратила {ad_spend:,.0f} ₽. "
            f"Текущий ДРР — {drr:.2f}%."
        ).replace(",", " ")

    return {
        "priority": priority,
        "summary_text": summary_text,
        "risks": risks,
        "suggested_actions": suggested_actions,
    }


@app.get("/summary/today")
def summary_today():
    """
    Return today's top metrics.
    """
    client = ch()

    today = date.today().isoformat()

    sales = client.query(
        """
        SELECT
            countIf(op = 'S') AS sales_count,
            sumIf(seller_price, op = 'S') AS revenue
        FROM fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date = %(today)s
        """,
        {
            "seller_id": SELLER_ID,
            "today": today,
        },
    ).result_rows[0]

    orders = client.query(
        """
        SELECT
            sum(quantity) AS orders_count
        FROM fact_orders
        WHERE seller_id = %(seller_id)s
          AND toDate(date_time) = %(today)s
          AND is_cancel = 0
        """,
        {
            "seller_id": SELLER_ID,
            "today": today,
        },
    ).result_rows[0]

    ads = client.query(
        """
        SELECT
            sum(spend) AS spend
        FROM fact_ads_stats_daily
        WHERE seller_id = %(seller_id)s
          AND stat_date = %(today)s
        """,
        {
            "seller_id": SELLER_ID,
            "today": today,
        },
    ).result_rows[0]

    sales_count, revenue = sales
    orders_count = orders[0]
    ad_spend = ads[0]

    revenue_value = float(revenue or 0)
    ad_spend_value = float(ad_spend or 0)

    drr = (
        (ad_spend_value / revenue_value) * 100
        if revenue_value > 0
        else 0
    )

    sales_count_value = int(sales_count or 0)
    orders_count_value = int(orders_count or 0)
    drr_value = round(drr, 2)

    summary = build_summary(
        sales_count=sales_count_value,
        orders_count=orders_count_value,
        revenue=revenue_value,
        ad_spend=ad_spend_value,
        drr=drr_value,
    )

    return {
        "seller_id": SELLER_ID,
        "sales_count": sales_count_value,
        "orders_count": orders_count_value,
        "revenue": round(revenue_value, 2),
        "ad_spend": round(ad_spend_value, 2),
        "drr": drr_value,
        "system_status": "ok",
                "priority": summary["priority"],
        "summary_text": summary["summary_text"],
        "risks": summary["risks"],
        "suggested_actions": summary["suggested_actions"],
    }