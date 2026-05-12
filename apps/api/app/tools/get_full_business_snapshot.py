"""
get_full_business_snapshot.py — полный снимок бизнеса Metrigo для AI.

Собирает ключевые данные одним инструментом:
- сводка за сегодня
- остатки и дни покрытия
- здоровье бизнеса
- реклама
- чистая прибыль
- мини-график продаж
"""

from typing import Dict
from app.tools.get_summary_today import get_summary_today
from app.tools.get_stock_context import get_stock_context
from app.tools.get_business_health import get_business_health
from app.tools.get_ad_context import get_ad_context
from app.tools.get_net_profit_context import get_net_profit_context
from app.tools.get_sales_mini_chart import get_sales_mini_chart


def get_full_business_snapshot(seller_id: str, days: int = 7) -> Dict:
    """
    Получить полный снимок бизнеса для AI-анализа.

    Используется, когда пользователь просит:
    - показать все ключевые показатели
    - оценить состояние бизнеса
    - найти проблемы
    - понять, что важно сейчас
    - оценить прибыльность и остатки
    """

    days = max(1, min(int(days or 7), 90))

    return {
        "seller_id": seller_id,
        "days": days,
        "summary_today": get_summary_today(seller_id=seller_id),
        "business_health": get_business_health(seller_id=seller_id),
        "stocks": get_stock_context(seller_id=seller_id, limit=10),
        "ads": get_ad_context(seller_id=seller_id, days=days),
        "net_profit": get_net_profit_context(seller_id=seller_id, days=days),
        "sales_chart": get_sales_mini_chart(seller_id=seller_id, days=60),
        "note": (
            "net_profit считается по последней доступной дате финансового отчета WB. "
            "Операционные продажи, заказы и остатки могут быть свежее финансового отчета."
        ),
    }