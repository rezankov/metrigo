"""
Инструмент: get_summary_today
Возвращает сводный отчёт на сегодня:
- агрегаты продаж
- расходы на рекламу
- DRR
- бизнес-здоровье
- текстовое summary с приоритетом
"""

import os
from typing import Dict
from app.db import ch
from app.tools.get_business_health import get_business_health

SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"

def get_summary_today(seller_id: str = SELLER_ID) -> Dict:
    """
    Формирует сводный отчёт на сегодня:
    - sales_count, orders_count, revenue
    - ad_spend, DRR
    - system_status, health_comment, health_details
    - priority, summary_text, suggested_actions
    """
    client = ch()

    # --- Продажи сегодня ---
    sales_rows = client.query(
        """
        SELECT
            countIf(op='S') AS sales_count,
            sumIf(seller_price, op='S') AS revenue,
            countIf(op = 'S') AS orders_count
        FROM metrigo.fact_sales
        WHERE seller_id=%(seller_id)s AND sale_date = today()
        """,
        {"seller_id": seller_id},
    ).result_rows

    sales_count, revenue, orders_count = sales_rows[0] if sales_rows else (0, 0.0, 0)

    # --- Расходы на рекламу сегодня ---
    ads_rows = client.query(
        """
        SELECT round(sum(spend),2) AS ad_spend
        FROM metrigo.fact_ads_stats_daily
        WHERE seller_id=%(seller_id)s AND stat_date = today()
        """,
        {"seller_id": seller_id},
    ).result_rows

    ad_spend = float(ads_rows[0][0] or 0.0) if ads_rows else 0.0

    # --- DRR ---
    drr = (ad_spend / revenue * 100) if revenue > 0 else 0

    # --- Получаем бизнес-здоровье ---
    health_info = get_business_health(seller_id)
    system_status = health_info.get("status", "ok")
    health_comment = health_info.get("comment", "")
    health_details = health_info.get("details", {})

    # --- Формируем простой текст summary ---
    if sales_count == 0 and orders_count == 0:
        priority = "warning"
        summary_text = "Сегодня пока нет продаж и заказов. Стоит проверить остатки, рекламу и доступность карточек."
    elif drr >= 20:
        priority = "warning"
        summary_text = f"Сегодня уже {sales_count} продаж и {revenue:,.0f} ₽ выручки. ДРР высокий — {drr:.2f}%.".replace(",", " ")
    else:
        priority = "ok"
        summary_text = f"Сегодня {sales_count} продаж, выручка {revenue:,.0f} ₽, расход рекламы {ad_spend:,.0f} ₽, ДРР {drr:.2f}%.".replace(",", " ")

    return {
        "seller_id": seller_id,
        "sales_count": int(sales_count),
        "orders_count": int(orders_count),
        "revenue": round(float(revenue), 2),
        "ad_spend": round(ad_spend, 2),
        "drr": round(drr, 2),
        "system_status": system_status,
        "health_comment": health_comment,
        "health_details": health_details,
        "priority": priority,
        "summary_text": summary_text,
        "risks": [],
        "suggested_actions": ["Что сегодня важно?", "Остатки", "Реклама", "Что заказать?"],
    }