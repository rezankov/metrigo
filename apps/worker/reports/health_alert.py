#!/usr/bin/env python3
"""
Generate Metrigo ETL health alert.

Что делает файл:
- проверяет свежесть ETL-сборщиков;
- проверяет последние статусы;
- печатает alert только если есть проблема;
- если всё хорошо — ничего не выводит.

Usage:
    python reports/health_alert.py
"""

import os

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
    Create ClickHouse client using worker env.
    """
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "metrigo"),
    )


def main():
    """
    Print alert if ETL has problems.
    """
    client = ch()

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

    problems = []

    for source, last_run, last_status, last_loaded, last_message, age_minutes in rows:
        source = str(source)
        last_status = str(last_status)
        age_minutes = int(age_minutes or 0)

        threshold = HEALTH_THRESHOLDS_MINUTES.get(source, 180)
        critical_threshold = threshold * 3

        if last_status == "error":
            problems.append(
                {
                    "icon": "🔴",
                    "source": source,
                    "reason": "последний запуск завершился ошибкой",
                    "last_run": last_run,
                    "loaded": int(last_loaded or 0),
                    "message": str(last_message),
                }
            )
            continue

        if age_minutes > critical_threshold:
            problems.append(
                {
                    "icon": "🔴",
                    "source": source,
                    "reason": f"нет обновления {age_minutes} мин",
                    "last_run": last_run,
                    "loaded": int(last_loaded or 0),
                    "message": str(last_message),
                }
            )
            continue

        if age_minutes > threshold:
            problems.append(
                {
                    "icon": "🟡",
                    "source": source,
                    "reason": f"давно не обновлялся: {age_minutes} мин",
                    "last_run": last_run,
                    "loaded": int(last_loaded or 0),
                    "message": str(last_message),
                }
            )

    if not rows:
        print("🔴 Metrigo Alert\n\nНет данных о запусках ETL.")
        return

    if not problems:
        return

    lines = [
        "🚨 Metrigo Alert",
        "",
        "Обнаружены проблемы со сборщиками:",
        "",
    ]

    for item in problems:
        lines += [
            f"{item['icon']} {item['source']}",
            f"Причина: {item['reason']}",
            f"Последний запуск: {item['last_run']}",
            f"Loaded: {item['loaded']}",
            f"Message: {item['message'][:500]}",
            "",
        ]

    print("\n".join(lines).strip())


if __name__ == "__main__":
    main()