"""
get_ai_business_insights.py — AI-ready инсайты бизнеса Metrigo.
"""

from typing import Dict

from app.tools.get_full_business_snapshot import (
    get_full_business_snapshot,
)

from app.business_rules import (
    analyze_business_snapshot,
)


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

    analysis = analyze_business_snapshot(snapshot)

    return {
        "seller_id": seller_id,
        "days": days,

        "priority": analysis["priority"],

        "margin_percent": analysis["margin_percent"],
        "ad_percent": analysis["ad_percent"],

        "risks": analysis["risks"],
        "warnings": analysis["warnings"],
        "insights": analysis["insights"],

        "recommended_actions": analysis["recommended_actions"],

        "snapshot": snapshot,
    }