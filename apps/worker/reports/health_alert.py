#!/usr/bin/env python3
"""
Generate Metrigo ETL health alert with anti-spam and recovery notifications.

Что делает файл:
- проверяет свежесть ETL-сборщиков;
- проверяет последние статусы;
- печатает alert только при новой проблеме;
- если проблема продолжается — молчит;
- если проблема восстановилась — печатает recovery;
- состояние хранит в state/alerts.

Usage:
    python reports/health_alert.py
"""

import hashlib
import os
from pathlib import Path

import clickhouse_connect


SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"

ALERT_STATE_DIR = Path(os.getenv("ALERT_STATE_DIR", "/opt/metrigo/state/alerts"))


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


def incident_hash(source: str, severity: str, reason: str) -> str:
    """
    Build stable incident hash.

    Хэш намеренно не включает last_run/loaded/message,
    чтобы одна и та же проблема не спамила каждые 10 минут.
    """
    text = f"{SELLER_ID}|{source}|{severity}|{reason}"
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def state_file(source: str) -> Path:
    """
    Return state file path for source.
    """
    safe_source = source.replace("/", "_").replace(" ", "_")
    return ALERT_STATE_DIR / f"{safe_source}.alert"


def read_state(source: str) -> str:
    """
    Read active incident hash for source.
    """
    path = state_file(source)

    if not path.exists():
        return ""

    return path.read_text(encoding="utf-8").strip()


def write_state(source: str, value: str) -> None:
    """
    Save active incident hash for source.
    """
    ALERT_STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_file(source).write_text(value, encoding="utf-8")


def clear_state(source: str) -> None:
    """
    Remove active incident state for source.
    """
    path = state_file(source)

    if path.exists():
        path.unlink()


def classify_problem(source, last_status, age_minutes):
    """
    Return problem dict or None.

    severity:
    - red: ошибка или критически старый запуск;
    - yellow: запуск давно не обновлялся.
    """
    source = str(source)
    last_status = str(last_status)
    age_minutes = int(age_minutes or 0)

    threshold = HEALTH_THRESHOLDS_MINUTES.get(source, 180)
    critical_threshold = threshold * 3

    if last_status == "error":
        return {
            "severity": "red",
            "icon": "🔴",
            "reason": "последний запуск завершился ошибкой",
        }

    if age_minutes > critical_threshold:
        return {
            "severity": "red",
            "icon": "🔴",
            "reason": "нет обновления",
            "details": f"{age_minutes} мин",
        }

    if age_minutes > threshold:
        return {
            "severity": "yellow",
            "icon": "🟡",
            "reason": "давно не обновлялся",
            "details": f"{age_minutes} мин",
        }

    return None


def main():
    """
    Print alert/recovery message if state changed.
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

    if not rows:
        source = "_system"
        reason = "нет данных о запусках ETL"
        current_hash = incident_hash(source, "red", reason)
        previous_hash = read_state(source)

        if previous_hash != current_hash:
            write_state(source, current_hash)
            print("🔴 Metrigo Alert\n\nНет данных о запусках ETL.")

        return

    new_alerts = []
    recoveries = []
    seen_sources = set()

    for source, last_run, last_status, last_loaded, last_message, age_minutes in rows:
        source = str(source)
        seen_sources.add(source)

        problem = classify_problem(source, last_status, age_minutes)
        previous_hash = read_state(source)

        if problem:
            current_hash = incident_hash(
                source,
                problem["severity"],
                problem["reason"],
            )

            if previous_hash != current_hash:
                write_state(source, current_hash)

                new_alerts.append(
                    {
                        "icon": problem["icon"],
                        "source": source,
                        "reason": problem["reason"],
                        "details": problem.get("details", ""),
                        "last_run": last_run,
                        "loaded": int(last_loaded or 0),
                        "message": str(last_message)[:500],
                    }
                )
        else:
            if previous_hash:
                clear_state(source)

                recoveries.append(
                    {
                        "source": source,
                        "last_run": last_run,
                        "loaded": int(last_loaded or 0),
                        "message": str(last_message)[:500],
                    }
                )

    # Если раньше была системная ошибка "нет данных", а теперь данные появились.
    if read_state("_system"):
        clear_state("_system")
        recoveries.append(
            {
                "source": "system",
                "last_run": "-",
                "loaded": 0,
                "message": "ETL health data appeared",
            }
        )

    if not new_alerts and not recoveries:
        return

    lines = []

    if new_alerts:
        lines += [
            "🚨 Metrigo Alert",
            "",
            "Новые проблемы со сборщиками:",
            "",
        ]

        for item in new_alerts:
            lines += [
                f"{item['icon']} {item['source']}",
                f"Причина: {item['reason']}"
                f"{' (' + item['details'] + ')' if item.get('details') else ''}",
                f"Последний запуск: {item['last_run']}",
                f"Loaded: {item['loaded']}",
                f"Message: {item['message']}",
                "",
            ]

    if recoveries:
        if lines:
            lines += [""]

        lines += [
            "🟢 Metrigo Recovery",
            "",
            "Восстановились:",
            "",
        ]

        for item in recoveries:
            lines += [
                f"🟢 {item['source']}",
                f"Последний запуск: {item['last_run']}",
                f"Loaded: {item['loaded']}",
                f"Message: {item['message']}",
                "",
            ]

    print("\n".join(lines).strip())


if __name__ == "__main__":
    main()