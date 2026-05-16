"""
monthly_profit.py — экономика магазина по месяцам.

Считает:
- выручку по товарам
- валовую прибыль по товарам
- расходы магазина
- чистую прибыль магазина
по каждому месяцу для указанного продавца.
"""

from typing import List, Dict
from fastapi import APIRouter, Request
from app.db import ch  # единый клиент ClickHouse

router = APIRouter()


def get_monthly_profit(seller_id: str) -> List[Dict]:
    """
    Возвращает список по месяцам:
    [{ "month": "2026-05-01", "revenue": 12345, "net_profit": 6789 }, ...]
    """
    client = ch()

    # --- агрегируем оборот и валовую прибыль ---
    sku_rows = client.query(
        """
        SELECT
            toStartOfMonth(sale_date) AS month,
            sum(revenue) AS revenue,
            sum(gross_profit) AS gross_profit
        FROM metrigo.fact_sku_finance_daily
        WHERE seller_id = %(seller_id)s
        GROUP BY month
        ORDER BY month ASC
        """,
        {"seller_id": seller_id},
    ).result_rows

    # --- агрегируем расходы магазина ---
    expense_rows = client.query(
        """
        SELECT
            month,
            sum(amount) AS shop_expenses
        FROM metrigo.fact_shop_expenses_monthly
        WHERE seller_id = %(seller_id)s
        GROUP BY month
        ORDER BY month ASC
        """,
        {"seller_id": seller_id},
    ).result_rows

    # Преобразуем в словари для простого объединения
    sku_dict = {r[0].isoformat(): {"revenue": float(r[1] or 0), "gross_profit": float(r[2] or 0)} for r in sku_rows}
    expense_dict = {r[0].isoformat(): float(r[1] or 0) for r in expense_rows}

    # Собираем итоговый список по месяцам
    months = sorted(set(list(sku_dict.keys()) + list(expense_dict.keys())))
    result = []
    for month in months:
        revenue = sku_dict.get(month, {}).get("revenue", 0)
        gross_profit = sku_dict.get(month, {}).get("gross_profit", 0)
        shop_expenses = expense_dict.get(month, 0)
        net_profit = gross_profit - shop_expenses
        result.append({
            "month": month,
            "revenue": revenue,
            "net_profit": net_profit,
        })

    return result


@router.post("/monthly_profit")
async def monthly_profit_endpoint(request: Request):
    """
    POST /dashboard/monthly_profit
    Тело запроса: { "seller_id": "main" }
    """
    body = await request.json()
    seller_id = body.get("seller_id", "main")
    data = get_monthly_profit(seller_id)
    return {"result": data}