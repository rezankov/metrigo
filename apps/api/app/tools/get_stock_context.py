"""
Инструмент: get_stock_context
Возвращает список SKU с остатками и днями покрытия.
"""

from app.db import ch
from typing import List, Dict

def get_stock_context(seller_id: str, limit: int = 5) -> List[Dict]:
    """
    Получить товары с остатками и скоростью продаж.

    Возвращает:
    - SKU
    - остаток на складе
    - продажи за период
    - средние продажи в день
    - количество дней покрытия

    Используется для:
    - поиска товаров, которые заканчиваются
    - анализа остатков
    - рекомендаций по поставкам
    """
    client = ch()
    rows = client.query(
        """
        WITH sales_14d AS (
            SELECT seller_art, countIf(op='S') AS sales_14d, countIf(op='S')/14.0 AS avg_sales_per_day
            FROM metrigo.fact_sales
            WHERE seller_id=%(seller_id)s
              AND sale_date >= toDate(now('Europe/Moscow'))-14
            GROUP BY seller_art
        )
        SELECT s.seller_art, sum(s.qty) AS stock_qty,
               coalesce(any(sa.sales_14d),0) AS sales_14d,
               coalesce(any(sa.avg_sales_per_day),0) AS avg_sales_per_day,
               if(coalesce(any(sa.avg_sales_per_day),0)>0,sum(s.qty)/coalesce(any(sa.avg_sales_per_day),0),9999) AS days_cover
        FROM metrigo.mart_stocks_latest AS s
        LEFT JOIN sales_14d AS sa ON sa.seller_art = s.seller_art
        WHERE s.seller_id=%(seller_id)s
        GROUP BY s.seller_art
        ORDER BY days_cover ASC
        LIMIT %(limit)s
        """,
        {"seller_id": seller_id, "limit": limit},
    ).result_rows

    result = []
    for seller_art, stock_qty, sales_14d, avg_sales_per_day, days_cover in rows:
        result.append({
            "seller_art":seller_art,
            "stock_qty":int(stock_qty),
            "sales_14d":int(sales_14d),
            "avg_sales_per_day":round(avg_sales_per_day,2),
            "days_cover":round(days_cover,1),
        })
    return result