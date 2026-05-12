"""
business_rules.py — детерминированные бизнес-правила Metrigo.

LLM не должен сам придумывать критичность.
Этот модуль превращает snapshot бизнеса в:
- risks
- warnings
- recommended_actions
- priority
"""

from typing import Any, Dict, List


CRITICAL_STOCK_DAYS = 25
LOW_STOCK_DAYS = 45
SUPPLY_LEAD_TIME_DAYS = 40

CRITICAL_MARGIN_PERCENT = 5
LOW_MARGIN_PERCENT = 10

HIGH_AD_PERCENT = 15
WATCH_AD_PERCENT = 8


def safe_percent(value: float, total: float) -> float:
    """
    Безопасно посчитать процент value от total.
    """

    if total <= 0:
        return 0.0

    return round((value / total) * 100, 2)


def analyze_business_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    Проанализировать snapshot бизнеса по жёстким правилам.
    """

    risks: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    insights: List[str] = []
    actions: List[str] = []

    net_profit_data = snapshot.get("net_profit") or {}
    stocks = snapshot.get("stocks") or []

    revenue = float(net_profit_data.get("revenue_tax_base") or 0)
    net_profit = float(net_profit_data.get("net_profit") or 0)
    ads_spend = float(net_profit_data.get("ads_spend") or 0)

    margin_percent = safe_percent(net_profit, revenue)
    ad_percent = safe_percent(ads_spend, revenue)

    # --- Маржа ---
    if revenue <= 0:
        warnings.append({
            "code": "NO_REVENUE",
            "level": "warning",
            "message": "Нет выручки для оценки маржинальности.",
        })

    elif margin_percent < 0:
        risks.append({
            "code": "NEGATIVE_MARGIN",
            "level": "critical",
            "message": f"Бизнес убыточен: маржа {margin_percent}%.",
        })
        actions.append("Проверить рекламу, логистику, хранение и себестоимость.")

    elif margin_percent < CRITICAL_MARGIN_PERCENT:
        risks.append({
            "code": "CRITICAL_LOW_MARGIN",
            "level": "critical",
            "message": f"Критически низкая чистая маржа: {margin_percent}%.",
        })
        actions.append("Разобрать P&L: реклама, логистика, хранение, цена, COGS.")

    elif margin_percent < LOW_MARGIN_PERCENT:
        warnings.append({
            "code": "LOW_MARGIN",
            "level": "warning",
            "message": f"Низкая чистая маржа: {margin_percent}%.",
        })
        actions.append("Искать способы поднять маржу: цена, реклама, логистика, хранение.")

    else:
        insights.append(f"Чистая маржа приемлемая: {margin_percent}%.")

    # --- Реклама ---
    if revenue > 0:
        if ad_percent > HIGH_AD_PERCENT:
            warnings.append({
                "code": "HIGH_AD_SPEND",
                "level": "warning",
                "message": f"Высокая доля рекламы: {ad_percent}% от выручки.",
            })
            actions.append("Проверить эффективность рекламных кампаний и отключить слабые.")

        elif ad_percent > WATCH_AD_PERCENT:
            warnings.append({
                "code": "WATCH_AD_SPEND",
                "level": "watch",
                "message": f"Реклама требует контроля: {ad_percent}% от выручки.",
            })
            actions.append("Проверить DRR/ACOS и динамику продаж по рекламным SKU.")

        else:
            insights.append(f"Реклама под контролем: {ad_percent}% от выручки.")

    # --- Остатки ---
    for item in stocks:
        sku = item.get("seller_art")
        days_cover = float(item.get("days_cover") or 0)
        stock_qty = int(item.get("stock_qty") or 0)
        avg_sales_per_day = float(item.get("avg_sales_per_day") or 0)

        if stock_qty <= 0 and avg_sales_per_day > 0:
            risks.append({
                "code": "OUT_OF_STOCK",
                "level": "critical",
                "sku": sku,
                "message": f"{sku}: товар закончился, при этом были продажи.",
            })
            actions.append(f"Срочно проверить наличие и поставку {sku}.")

        elif days_cover <= CRITICAL_STOCK_DAYS and avg_sales_per_day > 0:
            risks.append({
                "code": "CRITICAL_STOCK",
                "level": "critical",
                "sku": sku,
                "message": (
    f"{sku}: высокий риск дефицита до следующей поставки — "
    f"дней покрытия примерно {days_cover}, срок поставки около {SUPPLY_LEAD_TIME_DAYS} дней."
),
            })
            actions.append(
    f"Срочно планировать поставку {sku}: текущего остатка может не хватить до новой поставки."
)

        elif days_cover <= LOW_STOCK_DAYS and avg_sales_per_day > 0:
            warnings.append({
                "code": "LOW_STOCK",
                "level": "warning",
                "sku": sku,
                "message": (
    f"{sku}: дней покрытия {days_cover}, это меньше контрольного порога "
    f"{LOW_STOCK_DAYS} дней при сроке поставки около {SUPPLY_LEAD_TIME_DAYS} дней."
),
            })
            actions.append(f"Планировать поставку {sku}: дней покрытия меньше срока доставки {SUPPLY_LEAD_TIME_DAYS} дней.")

    if risks:
        priority = "critical"
    elif warnings:
        priority = "warning"
    else:
        priority = "ok"

    return {
        "priority": priority,
        "margin_percent": margin_percent,
        "ad_percent": ad_percent,
        "risks": risks,
        "warnings": warnings,
        "insights": insights,
        "recommended_actions": actions,
    }