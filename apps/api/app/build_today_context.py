"""
build_today_context.py — формирует полный контекст для ИИ

- Собирает сегодняшние метрики
- Добавляет доступные инструменты
- Добавляет системный промпт
"""

from datetime import date
from typing import Dict
from app.db import ch
from app.load_tools import ALL_TOOLS
from app.ai_prompts import SYSTEM_PROMPT


def build_today_context(seller_id: str) -> Dict:
    """
    Формирует контекст для ИИ:
    - sales / ads: сегодняшние метрики
    - tools: список доступных инструментов
    - system_prompt: системный промпт для LLM
    """
    client = ch()
    today = date.today().isoformat()

    # --- sales ---
    sales_rows = client.query(
        """
        SELECT
            countIf(op='S') AS sales_count,
            sumIf(seller_price, op='S') AS revenue,
            countIf(sell_id, op='S') AS orders_count
        FROM metrigo.fact_sales
        WHERE seller_id=%(seller_id)s AND sale_date=%(today)s
        """,
        {"seller_id": seller_id, "today": today},
    ).result_rows

    sales_count, revenue, orders_count = sales_rows[0] if sales_rows else (0, 0.0, 0)

    # --- ads ---
    ads_rows = client.query(
        """
        SELECT round(sum(spend),2) AS spend
        FROM metrigo.fact_ads_stats_daily
        WHERE seller_id=%(seller_id)s AND stat_date=%(today)s
        """,
        {"seller_id": seller_id, "today": today},
    ).result_rows
    ad_spend = float(ads_rows[0][0] or 0.0) if ads_rows else 0.0

    # --- итоговый контекст ---
    context = {
        "date": today,
        "sales": {
            "sales_count": int(sales_count or 0),
            "orders_count": int(orders_count or 0),
            "revenue": float(revenue or 0.0),
        },
        "ads": {
            "spend": ad_spend
        },
        "tools": ALL_TOOLS,
        "system_prompt": SYSTEM_PROMPT,
    }

    return context