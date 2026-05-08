#!/usr/bin/env python3
"""
Generate daily Metrigo report from ClickHouse.

Что делает файл:
- строит ежедневный отчёт за вчера;
- добавляет блок здоровья ETL-системы;
- выводит текст в stdout, чтобы его можно было отправить в MAX.

Usage:
    python reports/daily_report.py
"""

import os
from datetime import date, timedelta

import clickhouse_connect


SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"


HEALTH_THRESHOLDS_MINUTES = {
    "sales": 45,
    "orders": 60,
    "stocks": 120,
    "prices": 180,
    "content_cards": 36 * 60,
    "supplies": 48 * 60,
    "fin_report": 36 * 60,
    "tariffs": 36 * 60,
    "ads_campaigns": 90,
    "ads_stats_daily": 180,
}


def ch():
    """
    Create ClickHouse client using the same env contract as workers.
    """
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "metrigo"),
    )


def money(value) -> str:
    """
    Format money-like value.
    """
    return f"{float(value or 0):,.0f}".replace(",", " ")


def build_health_block(client) -> list[str]:
    """
    Build ETL health block with emoji statuses.

    🟢 — всё хорошо;
    🟡 — давно не обновлялось;
    🔴 — свежая ошибка или критически старый запуск.
    """
    rows = client.query(
        """
        SELECT
            source,
            last_run,
            last_status,
            last_loaded,
            last_message,
            dateDiff('minute', last_run, now()) AS age_minutes
        FROM mart_etl_health
        WHERE seller_id = %(seller_id)s
        ORDER BY source
        """,
        {"seller_id": SELLER_ID},
    ).result_rows

    lines = ["Система:"]

    if not rows:
        return [
            "Система:",
            "🔴 Нет данных о запусках ETL",
        ]

    overall = "🟢"

    for source, last_run, last_status, last_loaded, last_message, age_minutes in rows:
        source = str(source)
        age_minutes = int(age_minutes or 0)
        threshold = HEALTH_THRESHOLDS_MINUTES.get(source, 180)
        critical_threshold = threshold * 3

        if str(last_status) == "error":
            icon = "🔴"
            overall = "🔴"
            note = "ошибка"
        elif age_minutes > critical_threshold:
            icon = "🔴"
            overall = "🔴"
            note = f"нет обновления {age_minutes} мин"
        elif age_minutes > threshold:
            icon = "🟡"
            if overall != "🔴":
                overall = "🟡"
            note = f"давно: {age_minutes} мин"
        else:
            icon = "🟢"
            note = "ok"

        lines.append(
            f"{icon} {source}: {note}, last={last_run}, loaded={int(last_loaded or 0)}"
        )

    return [f"Здоровье системы: {overall}", ""] + lines


def main():
    """
    Generate daily report for yesterday.
    """
    client = ch()

    report_day = date.today() - timedelta(days=1)
    report_day_str = report_day.isoformat()

    sales = client.query(
        """
        SELECT
            countIf(op = 'S') AS sales_count,
            countIf(op = 'R') AS returns_count,
            sumIf(seller_price, op = 'S') AS revenue,
            abs(sumIf(seller_price, op = 'R')) AS returns_amount,
            sumIf(transfer_to_seller, op = 'S') AS transfer_to_seller
        FROM fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date = %(report_day)s
        """,
        {"seller_id": SELLER_ID, "report_day": report_day_str},
    ).result_rows[0]

    orders = client.query(
        """
        SELECT
            sum(quantity) AS orders_count,
            sum(total_price) AS orders_amount
        FROM fact_orders
        WHERE seller_id = %(seller_id)s
          AND toDate(date_time) = %(report_day)s
          AND is_cancel = 0
        """,
        {"seller_id": SELLER_ID, "report_day": report_day_str},
    ).result_rows[0]

    ads = client.query(
        """
        SELECT
            sum(views) AS views,
            sum(clicks) AS clicks,
            sum(spend) AS spend,
            sum(orders) AS ad_orders,
            sum(sum_price) AS ad_revenue
        FROM fact_ads_stats_daily
        WHERE seller_id = %(seller_id)s
          AND stat_date = %(report_day)s
        """,
        {"seller_id": SELLER_ID, "report_day": report_day_str},
    ).result_rows[0]

    stock = client.query(
        """
        SELECT
            sum(qty) AS qty_total,
            countDistinct(seller_art) AS sku_count
        FROM mart_stocks_latest
        WHERE seller_id = %(seller_id)s
        """,
        {"seller_id": SELLER_ID},
    ).result_rows[0]

    top_skus = client.query(
        """
        SELECT
            seller_art,
            countIf(op = 'S') AS sales_count,
            sumIf(seller_price, op = 'S') AS revenue
        FROM fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date = %(report_day)s
        GROUP BY seller_art
        HAVING sales_count > 0
        ORDER BY revenue DESC
        LIMIT 5
        """,
        {"seller_id": SELLER_ID, "report_day": report_day_str},
    ).result_rows

    sales_count, returns_count, revenue, returns_amount, transfer = sales
    orders_count, orders_amount = orders
    views, clicks, spend, ad_orders, ad_revenue = ads
    qty_total, sku_count = stock

    drr = (float(spend or 0) / float(revenue or 1)) * 100 if revenue else 0
    ctr = (float(clicks or 0) / float(views or 1)) * 100 if views else 0

    lines = [
        f"📊 Metrigo — отчёт за {report_day_str}",
        "",
        "",
        "Продажи:",
        f"• Продаж: {int(sales_count or 0)}",
        f"• Возвратов: {int(returns_count or 0)}",
        f"• Выручка seller_price: {money(revenue)} ₽",
        f"• Возвраты: {money(returns_amount)} ₽",
        f"• К перечислению: {money(transfer)} ₽",
        "",
        "Заказы:",
        f"• Заказов: {int(orders_count or 0)}",
        f"• Сумма заказов: {money(orders_amount)} ₽",
        "",
        "Реклама:",
        f"• Расход: {money(spend)} ₽",
        f"• Показы: {int(views or 0)}",
        f"• Клики: {int(clicks or 0)}",
        f"• CTR: {ctr:.2f}%",
        f"• ДРР от продаж: {drr:.2f}%",
        f"• Заказы из рекламы: {int(ad_orders or 0)}",
        f"• Сумма заказов из рекламы: {money(ad_revenue)} ₽",
        "",
        "Остатки сейчас:",
        f"• Всего штук: {int(qty_total or 0)}",
        f"• SKU: {int(sku_count or 0)}",
    ]

    if top_skus:
        lines += ["", "Топ SKU за день:"]
        for seller_art, qty, sku_revenue in top_skus:
            lines.append(f"• {seller_art}: {int(qty)} шт / {money(sku_revenue)} ₽")

    lines += [
        "",
        "",
        *build_health_block(client),
    ]
    print("\n".join(lines))


if __name__ == "__main__":
    main()