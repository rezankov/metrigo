"""
Инструмент: get_business_health
Возвращает бизнес-здоровье для интерфейса и ИИ:
- status: ok / warning / critical
- comment: пояснение
- details: по категориям (stocks, sales, ads)
"""

from typing import Dict
from app.db import ch

def get_business_health(seller_id: str) -> Dict:
    """
    Сбор бизнес-метрик и формирование статуса:
    - остатки на складе (stocks)
    - продажи (sales)
    - реклама / DRR (ads)
    """
    client = ch()

    # --- Продажи сегодня ---
    sales_rows = client.query(
        """
        SELECT
            countIf(op='S') AS sales_count,
            sumIf(seller_price, op='S') AS revenue
        FROM metrigo.fact_sales
        WHERE seller_id=%(seller_id)s
          AND sale_date = today()
        """,
        {"seller_id": seller_id},
    ).result_rows

    sales_count, revenue = sales_rows[0] if sales_rows else (0, 0.0)

    # --- Расходы на рекламу сегодня ---
    ads_rows = client.query(
        """
        SELECT round(sum(spend),2) AS ad_spend
        FROM metrigo.fact_ads_stats_daily
        WHERE seller_id=%(seller_id)s
          AND stat_date = today()
        """,
        {"seller_id": seller_id},
    ).result_rows

    ad_spend = float(ads_rows[0][0] or 0.0) if ads_rows else 0.0

    # --- Остатки на складе ---
    stocks_rows = client.query(
        """
        SELECT sum(qty) AS total_stock
        FROM metrigo.mart_stocks_latest
        WHERE seller_id=%(seller_id)s
        """,
        {"seller_id": seller_id},
    ).result_rows

    stocks_total = int(stocks_rows[0][0] or 0) if stocks_rows else 0

    # --- Формирование статуса ---
    status = "ok"
    comment = "Все в норме"
    details = {}

    details["stocks"] = "ok" if stocks_total > 0 else "critical"
    details["sales"] = "ok" if sales_count > 0 else "warning"
    details["ads"] = "ok" if revenue == 0 or (ad_spend / revenue * 100) <= 20 else "warning"

    if "critical" in details.values():
        status = "critical"
        comment = "Критические запасы на складе"
    elif "warning" in details.values():
        status = "warning"
        comment = "Есть предупреждения по бизнесу"

    return {"status": status, "comment": comment, "details": details}


# --- Метаданные для ИИ ---
tool_metadata = {
    "name": "get_business_health",
    "description": (
        "Возвращает статус бизнеса: продажи, остатки, расходы на рекламу. "
        "Формирует status (ok/warning/critical), comment и детали по категориям."
    ),
    "args": {
        "seller_id": "str, идентификатор продавца"
    },
    "return": {
        "status": "ok/warning/critical",
        "comment": "пояснение",
        "details": "dict: stocks, sales, ads"
    },
    "entrypoint": "get_business_health"
}