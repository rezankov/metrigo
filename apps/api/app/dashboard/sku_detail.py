"""
sku_detail.py — детальная информация по SKU для dashboard.

Источник:
- agg_sku_snapshot — актуальная юнит-экономика SKU
- fact_sku_finance_daily — история по дням
- mart_stocks_latest — остатки по складам
"""

from typing import Dict

from app.db import ch


def get_sku_detail(seller_id: str, sku: str, days: int = 30) -> Dict:
    client = ch()

    latest_rows = client.query(
        """
        SELECT
            sales_7d,
            revenue_30d,
            avg_price,
            cogs,
            commission,
            acquiring,
            logistics,
            tax,
            profit_per_unit,
            margin_percent
        FROM metrigo.agg_sku_snapshot
        WHERE seller_id = %(seller_id)s
          AND sku = %(sku)s
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        {"seller_id": seller_id, "sku": sku},
    ).result_rows

    if latest_rows:
        row = latest_rows[0]
        sales_count = float(row[0] or 0)
        revenue = float(row[1] or 0)
        avg_price = float(row[2] or 0)
        cogs = float(row[3] or 0)
        commission = float(row[4] or 0)
        acquiring = float(row[5] or 0)
        logistics = float(row[6] or 0)
        tax = float(row[7] or 0)
        profit_per_unit = float(row[8] or 0)
        margin_percent = float(row[9] or 0)
    else:
        sales_count = revenue = avg_price = cogs = 0.0
        commission = acquiring = logistics = tax = 0.0
        profit_per_unit = margin_percent = 0.0

    history_rows = client.query(
        """
        SELECT
            sale_date,
            revenue,
            sales_count,
            gross_profit,
            margin_percent
        FROM metrigo.fact_sku_finance_daily
        WHERE seller_id = %(seller_id)s
          AND sku = %(sku)s
          AND sale_date >= today() - %(days)s
        ORDER BY sale_date
        """,
        {"seller_id": seller_id, "sku": sku, "days": int(days or 30)},
    ).result_rows

    sales_chart = [
        {
            "date": r[0].isoformat(),
            "revenue": float(r[1] or 0),
            "sales_count": float(r[2] or 0),
            "profit_per_unit": float(r[3] or 0),
            "margin_percent": float(r[4] or 0),
        }
        for r in history_rows
    ]

    warehouse_rows = client.query(
        """
        SELECT
            warehouse,
            sum(qty) AS qty,
            sum(qty_full) AS qty_full,
            sum(in_way_to_client) AS in_way_to_client,
            sum(in_way_from_client) AS in_way_from_client
        FROM metrigo.mart_stocks_latest
        WHERE seller_id = %(seller_id)s
          AND seller_art = %(sku)s
        GROUP BY warehouse
        ORDER BY qty_full DESC
        """,
        {"seller_id": seller_id, "sku": sku},
    ).result_rows

    warehouses = [
        {
            "warehouse": r[0],
            "qty": int(r[1] or 0),
            "qty_full": int(r[2] or 0),
            "in_way_to_client": int(r[3] or 0),
            "in_way_from_client": int(r[4] or 0),
            "returns": int(r[4] or 0),
        }
        for r in warehouse_rows
    ]

    warehouse_summary = {
        "qty": sum(w["qty"] for w in warehouses),
        "qty_full": sum(w["qty_full"] for w in warehouses),
        "in_way_to_client": sum(w["in_way_to_client"] for w in warehouses),
        "in_way_from_client": sum(w["in_way_from_client"] for w in warehouses),
        "returns": sum(w["returns"] for w in warehouses),
    }

    unit_items = [
        {"key": "cogs", "label": "COGS", "value": cogs},
        {"key": "commission", "label": "Комиссия WB", "value": commission},
        {"key": "acquiring", "label": "Эквайринг", "value": acquiring},
        {"key": "logistics", "label": "Логистика", "value": logistics},
        {"key": "tax", "label": "Налог", "value": tax},
        {"key": "profit", "label": "Прибыль", "value": profit_per_unit},
    ]

    return {
        "seller_id": seller_id,
        "sku": sku,
        "days": days,
        "summary": {
            "sales_count": sales_count,
            "revenue": revenue,
            "avg_price": avg_price,
            "cogs": cogs,
            "profit_per_unit": profit_per_unit,
            "margin_percent": margin_percent,
        },
        "unit_economics": {
            "price": avg_price,
            "items": unit_items,
            "note": "Юнит-экономика SKU без общих расходов магазина.",
        },
        "sales_chart": sales_chart,
        "warehouses": warehouses,
        "warehouse_summary": warehouse_summary,
    }