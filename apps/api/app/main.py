"""
Metrigo API entrypoint.
"""

import os
from datetime import date

from fastapi import FastAPI

from app.db import ch

from pydantic import BaseModel


SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"

app = FastAPI(title="Metrigo API")


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    type: str = "text"
    text: str


@app.get("/health")
def health():
    """
    Return API health status.
    """
    return {"status": "ok", "service": "metrigo-api"}


def get_today_context() -> dict:
    """
    Load today's business context from ClickHouse.
    """
    client = ch()
    today = date.today().isoformat()

    sales = client.query(
        """
        SELECT
            countIf(op = 'S') AS sales_count,
            sumIf(seller_price, op = 'S') AS revenue
        FROM fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date = %(today)s
        """,
        {"seller_id": SELLER_ID, "today": today},
    ).result_rows[0]

    orders = client.query(
        """
        SELECT sum(quantity) AS orders_count
        FROM fact_orders
        WHERE seller_id = %(seller_id)s
          AND toDate(date_time) = %(today)s
          AND is_cancel = 0
        """,
        {"seller_id": SELLER_ID, "today": today},
    ).result_rows[0]

    ads = client.query(
        """
        SELECT sum(spend) AS spend
        FROM fact_ads_stats_daily
        WHERE seller_id = %(seller_id)s
          AND stat_date = %(today)s
        """,
        {"seller_id": SELLER_ID, "today": today},
    ).result_rows[0]

    sales_count, revenue = sales
    orders_count = orders[0]
    ad_spend = ads[0]

    revenue_value = float(revenue or 0)
    ad_spend_value = float(ad_spend or 0)
    drr = (ad_spend_value / revenue_value) * 100 if revenue_value > 0 else 0

    return {
        "today": today,
        "sales_count": int(sales_count or 0),
        "orders_count": int(orders_count or 0),
        "revenue": round(revenue_value, 2),
        "ad_spend": round(ad_spend_value, 2),
        "drr": round(drr, 2),
    }


def build_summary(
    sales_count: int,
    orders_count: int,
    revenue: float,
    ad_spend: float,
    drr: float,
) -> dict:
    """
    Build rule-based business summary for chat home.

    Это первый слой intelligence без LLM:
    backend сам формирует summary, risks и suggested actions.
    """
    risks = []
    suggested_actions = [
        "Что сегодня важно?",
        "Остатки",
        "Реклама",
        "Что заказать?",
    ]

    if sales_count == 0 and orders_count == 0:
        priority = "warning"
        summary_text = (
            "Сегодня пока нет продаж и заказов. "
            "Стоит проверить остатки, рекламу и доступность карточек."
        )
        risks.append("Нет продаж и заказов сегодня")
        suggested_actions = [
            "Проверь остатки",
            "Проверь рекламу",
            "Покажи заказы",
            "Найди проблему",
        ]

    elif drr >= 20:
        priority = "warning"
        summary_text = (
            f"Сегодня уже {sales_count} продаж и {revenue:,.0f} ₽ выручки. "
            f"Но реклама потратила {ad_spend:,.0f} ₽, "
            f"ДРР высокий — {drr:.2f}%. Стоит проверить эффективность кампаний."
        ).replace(",", " ")
        risks.append("Высокий ДРР")
        suggested_actions = [
            "Покажи рекламу",
            "Какие кампании дорогие?",
            "Сравни ДРР",
            "Что отключить?",
        ]

    elif sales_count > 0 and orders_count == 0:
        priority = "normal"
        summary_text = (
            f"Сегодня есть {sales_count} продаж на {revenue:,.0f} ₽. "
            "Новых заказов пока нет. Стоит посмотреть динамику по часам."
        ).replace(",", " ")
        suggested_actions = [
            "Покажи продажи",
            "Покажи заказы",
            "Сравни со вчера",
            "Остатки",
        ]

    else:
        priority = "ok"
        summary_text = (
            f"Сегодня уже {sales_count} продаж и {revenue:,.0f} ₽ выручки. "
            f"Заказов сегодня: {orders_count}. "
            f"Реклама потратила {ad_spend:,.0f} ₽. "
            f"Текущий ДРР — {drr:.2f}%."
        ).replace(",", " ")

    return {
        "priority": priority,
        "summary_text": summary_text,
        "risks": risks,
        "suggested_actions": suggested_actions,
    }


@app.get("/summary/today")
def summary_today():
    """
    Return today's top metrics.
    """
    context = get_today_context()

    sales_count_value = context["sales_count"]
    orders_count_value = context["orders_count"]
    revenue_value = context["revenue"]
    ad_spend_value = context["ad_spend"]
    drr_value = context["drr"]

    summary = build_summary(
        sales_count=sales_count_value,
        orders_count=orders_count_value,
        revenue=revenue_value,
        ad_spend=ad_spend_value,
        drr=drr_value,
    )

    return {
        "seller_id": SELLER_ID,
        "sales_count": sales_count_value,
        "orders_count": orders_count_value,
        "revenue": round(revenue_value, 2),
        "ad_spend": round(ad_spend_value, 2),
        "drr": drr_value,
        "system_status": "ok",
        "priority": summary["priority"],
        "summary_text": summary["summary_text"],
        "risks": summary["risks"],
        "suggested_actions": summary["suggested_actions"],
    }


def get_stock_context(limit: int = 5) -> list[dict]:
    """
    Return stock cover context by SKU.

    days_cover = current stock / average daily sales for last 14 days.
    """
    client = ch()

    rows = client.query(
        """
        WITH sales_14d AS
        (
            SELECT
                seller_art,
                countIf(op = 'S') AS sales_14d,
                countIf(op = 'S') / 14.0 AS avg_sales_per_day
            FROM fact_sales
            WHERE seller_id = %(seller_id)s
              AND sale_date >= today() - 14
            GROUP BY seller_art
        )
        SELECT
            s.seller_art,
            sum(s.qty) AS stock_qty,
            coalesce(any(sa.sales_14d), 0) AS sales_14d,
            coalesce(any(sa.avg_sales_per_day), 0) AS avg_sales_per_day,
            if(
                coalesce(any(sa.avg_sales_per_day), 0) > 0,
                sum(s.qty) / coalesce(any(sa.avg_sales_per_day), 0),
                9999
            ) AS days_cover
        FROM mart_stocks_latest AS s
        LEFT JOIN sales_14d AS sa ON sa.seller_art = s.seller_art
        WHERE s.seller_id = %(seller_id)s
        GROUP BY s.seller_art
        ORDER BY days_cover ASC
        LIMIT %(limit)s
        """,
        {
            "seller_id": SELLER_ID,
            "limit": limit,
        },
    ).result_rows

    result = []

    for seller_art, stock_qty, sales_14d, avg_sales_per_day, days_cover in rows:
        result.append(
            {
                "seller_art": str(seller_art),
                "stock_qty": int(stock_qty or 0),
                "sales_14d": int(sales_14d or 0),
                "avg_sales_per_day": round(float(avg_sales_per_day or 0), 2),
                "days_cover": round(float(days_cover or 0), 1),
            }
        )

    return result


@app.post("/chat")
def chat(request: ChatRequest):
    """
    Context-aware rule-based chat endpoint v1.
    """
    text = request.message.strip().lower()
    context = get_today_context()

    sales_count = context["sales_count"]
    orders_count = context["orders_count"]
    revenue = context["revenue"]
    ad_spend = context["ad_spend"]
    drr = context["drr"]

    if "остат" in text:
        stocks = get_stock_context(limit=5)

        if not stocks:
            answer = (
                "По остаткам пока нет данных. Нужно проверить сборщик stocks "
                "и таблицу mart_stocks_latest."
            )
        else:
            lines = [
                "По остаткам вижу ближайшие SKU по дням покрытия:",
                "",
            ]

            for item in stocks:
                days_cover = item["days_cover"]

                if days_cover >= 9999:
                    cover_text = "продаж за 14 дней не было"
                else:
                    cover_text = f"хватит примерно на {days_cover} дн."

                lines.append(
                    f"• {item['seller_art']}: "
                    f"{item['stock_qty']} шт, "
                    f"продаж за 14 дней: {item['sales_14d']}, "
                    f"{cover_text}"
                )

            answer = "\n".join(lines)

    elif "реклам" in text or "дрр" in text:
        if revenue > 0:
            answer = (
                f"По рекламе сегодня: расход {ad_spend:,.0f} ₽, "
                f"выручка {revenue:,.0f} ₽, текущий ДРР {drr:.2f}%. "
            ).replace(",", " ")

            if drr >= 20:
                answer += (
                    "Это высокий уровень. Нужно смотреть кампании и SKU, "
                    "где расход есть, а заказов мало."
                )
            elif drr >= 10:
                answer += (
                    "ДРР умеренный. Стоит проверить, не растёт ли расход "
                    "быстрее продаж."
                )
            else:
                answer += (
                    "Сейчас ДРР выглядит спокойно. Можно смотреть, какие "
                    "кампании дают лучшие заказы."
                )
        else:
            answer = (
                f"Реклама сегодня потратила {ad_spend:,.0f} ₽, "
                "но продаж пока нет. Нужно проверить кампании и карточки."
            ).replace(",", " ")

    elif "систем" in text:
        answer = (
            "Система контролируется отдельно: ETL health monitoring проверяет "
            "сборщики каждые 10 минут, а новые аварии и восстановления уходят в MAX."
        )

    elif "важно" in text or "сегодня" in text:
        if sales_count == 0 and orders_count == 0:
            answer = (
                "Сегодня пока нет продаж и заказов. Первое, что стоит проверить: "
                "остатки, активность рекламы и доступность карточек."
            )
        elif drr >= 20:
            answer = (
                f"Сегодня важно проверить рекламу: ДРР уже {drr:.2f}%. "
                f"При выручке {revenue:,.0f} ₽ расход рекламы {ad_spend:,.0f} ₽."
            ).replace(",", " ")
        else:
            answer = (
                f"Сегодня картина спокойная: {sales_count} продаж, "
                f"{orders_count} заказов, выручка {revenue:,.0f} ₽, "
                f"ДРР {drr:.2f}%. Я бы дальше посмотрел остатки лидеров продаж."
            ).replace(",", " ")

    else:
        answer = (
            "Я уже вижу текущий контекст бизнеса: "
            f"{sales_count} продаж, {orders_count} заказов, "
            f"выручка {revenue:,.0f} ₽, ДРР {drr:.2f}%. "
            "Сейчас я работаю в rule-based режиме, следующий шаг — подключить "
            "анализ по SKU, складам и графикам."
        ).replace(",", " ")

    return {"type": "text", "text": answer}
