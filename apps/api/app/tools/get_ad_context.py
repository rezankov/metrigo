"""
Инструмент: get_ad_context
Возвращает контекст рекламных расходов:
- ad_spend за последние N дней
- возможность фильтрации по SKU
- безопасная работа с вложенным JSON в fact_ads_stats_daily
"""

from app.db import ch
from typing import Dict, Optional

def get_ad_context(seller_id: str, sku: Optional[str] = None, days: int = 14) -> Dict:
    """
    Получить рекламные расходы за последние `days` дней.
    Если sku указан, фильтруем по SKU, иначе суммируем по всем SKU.
    """
    client = ch()

    # SQL с безопасной фильтрацией JSON
    sql = """
        SELECT round(sum(spend), 2) AS ad_spend
        FROM metrigo.fact_ads_stats_daily
        WHERE seller_id = %(seller_id)s
          AND stat_date >= today() - %(days)s
    """
    params = {"seller_id": seller_id, "days": days}

    if sku:
        sql += " AND JSONExtractString(payload, 'seller_art') = %(sku)s"
        params["sku"] = sku

    rows = client.query(sql, params).result_rows
    ad_spend = rows[0][0] if rows else 0.0

    return {"seller_id": seller_id, "sku": sku, "ad_spend": float(ad_spend)}

# Метаданные для ИИ
tool_metadata = {
    "name": "get_ad_context",
    "description": (
        "Возвращает рекламные расходы (ad_spend) за последние N дней. "
        "Можно фильтровать по конкретному SKU или получать суммарно по всем."
    ),
    "args": {
        "seller_id": "str, идентификатор продавца",
        "sku": "str, необязательный, фильтр по SKU",
        "days": "int, количество дней для суммирования, по умолчанию 14"
    },
    "return": {
        "seller_id": "ID продавца",
        "sku": "SKU или None",
        "ad_spend": "суммарные расходы на рекламу за период"
    },
    "entrypoint": "get_ad_context"
}