"""
get_ai_business_insights.py — AI-ready инсайты бизнеса Metrigo.
"""

from typing import Dict, List

from app.tools.get_full_business_snapshot import get_full_business_snapshot


LOW_STOCK_DAYS = 25
LOW_MARGIN_PERCENT = 10


def _safe_percent(value: float, total: float) -> float:
    """
    Безопасно посчитать процент value от total.
    """

    if total <= 0:
        return 0.0

    return round((value / total) * 100, 2)


def get_ai_business_insights(
    seller_id: str,
    days: int = 7,
) -> Dict:
    """
    Получить AI-инсайты по бизнесу.

    Используется AI-агентом для:
    - анализа бизнеса
    - поиска проблем
    - поиска рисков
    - рекомендаций
    """

    days = max(1, min(int(days or 7), 90))

    snapshot = get_full_business_snapshot(
        seller_id=seller_id,
        days=days,
    )

    insights: List[str] = []
    warnings: List[str] = []
    actions: List[str] = []

    profit = snapshot.get("net_profit") or {}
    stocks = snapshot.get("stocks") or {}

    revenue = float(profit.get("revenue_tax_base") or 0)
    net_profit = float(profit.get("net_profit") or 0)
    ads_spend = float(profit.get("ads_spend") or 0)

    margin_percent = _safe_percent(
        net_profit,
        revenue,
    )

    ad_percent = _safe_percent(
        ads_spend,
        revenue,
    )

    # --- Анализ маржи ---
    if revenue <= 0:
        warnings.append(
            "Нет выручки для расчёта маржинальности."
        )

    elif margin_percent < 0:
        warnings.append(
            f"Бизнес убыточен. Чистая маржа: {margin_percent}%."
        )

    elif margin_percent < LOW_MARGIN_PERCENT:
        warnings.append(
            f"Низкая чистая маржа: {margin_percent}%."
        )

    else:
        insights.append(
            f"Бизнес прибыльный. Чистая маржа около {margin_percent}%."
        )

    # --- Анализ рекламы ---
    if revenue <= 0:
        warnings.append(
            "Нельзя оценить долю рекламы: нет выручки."
        )

    elif ad_percent > 15:
        warnings.append(
            f"Высокие расходы на рекламу: {ad_percent}% от выручки."
        )

    elif ad_percent > 8:
        insights.append(
            f"Реклама занимает {ad_percent}% выручки."
        )

    else:
        insights.append(
            f"Реклама под контролем: {ad_percent}% от выручки."
        )

    # --- Остатки ---
    low_stock = []

    for item in stocks:
        days_cover = float(item.get("days_cover") or 0)

        if days_cover <= LOW_STOCK_DAYS:
            low_stock.append(
                {
                    "sku": item.get("seller_art"),
                    "days_cover": days_cover,
                    "stock_qty": item.get("stock_qty"),
                    "avg_sales_per_day": item.get("avg_sales_per_day"),
                }
            )

    if low_stock:
        warnings.append(
            f"{len(low_stock)} SKU имеют низкий остаток."
        )

        for item in low_stock:
            actions.append(
                f"Пополнить {item['sku']} "
                f"(остаток примерно на {item['days_cover']} дней)."
            )

    # --- Продажи ---
    chart = snapshot.get("sales_chart") or {}
    values = chart.get("values") or []

    if len(values) >= 7:
        last_day = float(values[-1] or 0)
        previous_values = [float(value or 0) for value in values[:-1]]
        avg = sum(previous_values) / max(1, len(previous_values))

        if avg > 0 and last_day < avg * 0.5:
            warnings.append(
                "Последний день продаж заметно ниже среднего."
            )

        elif avg > 0 and last_day > avg * 1.5:
            insights.append(
                "Последний день продаж заметно выше среднего."
            )

    return {
        "seller_id": seller_id,
        "days": days,
        "margin_percent": margin_percent,
        "ad_percent": ad_percent,
        "insights": insights,
        "warnings": warnings,
        "recommended_actions": actions,
        "snapshot": snapshot,
    }