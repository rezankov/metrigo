"""
Инструмент: get_sku_context
Возвращает контекст по конкретному SKU:
- остатки на складе
- средние продажи
- дни покрытия
- продажи за N дней
- расход рекламы (ad_spend)
"""

from app.db import ch
from typing import Dict

def get_sku_context(seller_id: str, sku: str, days: int = 14) -> Dict:
    """
    Получить контекст по SKU:
    - stock_qty: количество на складе
    - avg_sales_per_day: средние продажи за период
    - days_cover: сколько дней хватит запаса
    - sales_count: продажи за период
    - ad_spend: расходы на рекламу по SKU
    """
    client = ch()

    # --- Stock / days_cover ---
    stock_rows = client.query(
        """
        WITH sales_avg AS (
            SELECT seller_art, countIf(op='S') / %(days)s AS avg_sales_per_day
            FROM metrigo.fact_sales
            WHERE seller_id=%(seller_id)s
              AND sale_date >= toDate(now('Europe/Moscow')) - %(days)s
              AND seller_art = %(sku)s
            GROUP BY seller_art
        )
        SELECT
            s.seller_art,
            sum(s.qty) AS stock_qty,
            coalesce(sa.avg_sales_per_day, 0) AS avg_sales_per_day,
            if(coalesce(sa.avg_sales_per_day, 0) > 0, sum(s.qty)/coalesce(sa.avg_sales_per_day, 0), 9999) AS days_cover
        FROM metrigo.mart_stocks_latest AS s
        LEFT JOIN sales_avg AS sa ON s.seller_art = sa.seller_art
        WHERE s.seller_id = %(seller_id)s
          AND s.seller_art = %(sku)s
        GROUP BY s.seller_art, sa.avg_sales_per_day
        """,
        {"seller_id": seller_id, "sku": sku, "days": days},
    ).result_rows

    stock_info = stock_rows[0] if stock_rows else (sku, 0, 0, 9999)
    _, stock_qty, avg_sales_per_day, days_cover = stock_info

    # --- Sales over last N days ---
    sales_rows = client.query(
        """
        SELECT countIf(op='S') AS sales_count
        FROM metrigo.fact_sales
        WHERE seller_id=%(seller_id)s
          AND sale_date >= toDate(now('Europe/Moscow')) - %(days)s
          AND seller_art = %(sku)s
        """,
        {"seller_id": seller_id, "sku": sku, "days": days},
    ).result_rows
    sales_count = sales_rows[0][0] if sales_rows else 0

    # --- Advertising spend over last N days via JSON payload ---
    ads_rows = client.query(
        """
        SELECT round(sum(spend), 2) AS ad_spend
        FROM metrigo.fact_ads_stats_daily
        WHERE seller_id = %(seller_id)s
          AND JSONExtractString(payload, 'seller_art') = %(sku)s
          AND stat_date >= toDate(now('Europe/Moscow')) - %(days)s
        """,
        {"seller_id": seller_id, "sku": sku, "days": days},
    ).result_rows
    ad_spend = float(ads_rows[0][0] or 0.0) if ads_rows else 0.0

    return {
        "seller_art": sku,
        "stock_qty": int(stock_qty or 0),
        "avg_sales_per_day": round(float(avg_sales_per_day or 0), 2),
        "days_cover": round(float(days_cover or 0), 1),
        "sales_count": int(sales_count or 0),
        "ad_spend": float(ad_spend or 0),
    }