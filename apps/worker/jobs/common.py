"""
Common utilities for Metrigo worker jobs.

Что делает файл:
- подключается к ClickHouse;
- хранит общие функции для WB API;
- ведёт watermark по каждому seller_id и source;
- пишет технические события ETL в etl_runs;
- сохраняет сырые ответы WB API в raw_events.

Почему это важно:
- worker должен безопасно перезапускаться без дублей;
- каждая строка данных должна принадлежать конкретному seller_id;
- raw_events нужны как "источник истины" для отладки и пересборки fact-таблиц.
"""

import os
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from dateutil import parser as dtparser
import requests
import clickhouse_connect


# ClickHouse connection settings.
# В Docker эти значения приходят из .env через docker-compose.
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_HTTP_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "metrigo")


# Временный single-seller режим.
# Позже worker будет брать seller_id и WB token из Postgres.
DEFAULT_SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip()


# Временный WB token из .env.
# В SaaS-версии токены будут храниться в Postgres в зашифрованном виде.
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()


def ch():
    """
    Create ClickHouse client.

    Используется всеми job-файлами для записи raw/fact/etl таблиц.
    """
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_HTTP_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )


def seller_id() -> str:
    """
    Возвращает текущий seller_id (multi-tenant).
    """
    return os.getenv("DEFAULT_SELLER_ID", "main")


def md5_hex(s: str) -> str:
    """
    Return stable MD5 hash as 32-char hex string.

    Используем для:
    - payload_hash в raw_events;
    - dedup_key в fact-таблицах.
    """
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def parse_dt(v: str) -> datetime:
    """
    Parse datetime from WB API and normalize to UTC-aware datetime.

    WB может отдавать даты с timezone или без неё.
    Внутри worker приводим всё к UTC, чтобы watermark работал стабильно.
    """
    dt = dtparser.parse(v)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def dt_to_ms(dt: datetime) -> int:
    """
    Convert datetime to millisecond timestamp.

    Используется как version для ReplacingMergeTree.
    Чем новее загрузка, тем выше version.
    """
    return int(dt.timestamp() * 1000)


def wb_get_list(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Request list data from WB API.

    Почему есть retry:
    - WB API может возвращать 429 при лимитах;
    - иногда бывают 500/502/503/504;
    - worker не должен падать от временной ошибки API.

    Возвращаем только list, потому что статистические endpoint'ы WB обычно
    возвращают массив объектов.
    """
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN is empty")

    headers = {"Authorization": WB_STATS_TOKEN}

    for attempt in range(10):
        r = requests.get(url, headers=headers, params=params, timeout=120)

        if r.status_code == 429:
            time.sleep(2 + attempt * 2)
            continue

        if r.status_code in (500, 502, 503, 504):
            time.sleep(2 + attempt * 2)
            continue

        r.raise_for_status()

        data = r.json()

        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected response type: {type(data)}")

        return data

    raise RuntimeError("Too many retries from WB API")


def get_watermark(source: str, seller_id: str) -> datetime:
    """
    Получение watermark с учетом seller_id
    """
    client = ch()

    rows = client.query(
        """
        SELECT max(watermark)
        FROM etl_state
        WHERE source = %(s)s AND seller_id = %(sid)s
        """,
        {"s": source, "sid": seller_id},
    ).result_rows

    if not rows or rows[0][0] is None:
        return datetime.now(timezone.utc) - timedelta(days=2)

    wm = rows[0][0]
    if wm.tzinfo is None:
        wm = wm.replace(tzinfo=timezone.utc)
    return wm


def set_watermark(source: str, wm: datetime, seller_id: str):
    """
    Сохраняем watermark с учетом seller_id
    """
    client = ch()

    wm_naive = wm.astimezone(timezone.utc).replace(tzinfo=None)

    client.command(
        """
        INSERT INTO etl_state (source, seller_id, watermark)
        VALUES (%(s)s, %(sid)s, %(w)s)
        """,
        {"s": source, "sid": seller_id, "w": wm_naive},
    )


def etl_run(source: str, status: str, loaded: int, message: str):
    """
    Write ETL run log.

    etl_runs нужен для мониторинга:
    - когда запускался job;
    - сколько строк загрузил;
    - была ли ошибка/пустой ответ;
    - какой watermark или период использовался.
    """
    client = ch()
    sid = seller_id()

    client.command(
        """
        INSERT INTO etl_runs (seller_id, source, status, loaded, message)
        VALUES (%(seller_id)s, %(source)s, %(status)s, %(loaded)s, %(message)s)
        """,
        {
            "seller_id": sid,
            "source": source,
            "status": status,
            "loaded": int(loaded),
            "message": str(message)[:2000],
        },
    )


def insert_raw(source: str, items: Any, event_dt_naive: Optional[datetime], version: int):
    """
    Insert raw WB API payloads into raw_events.

    raw_events — это "источник истины":
    - можно пересобрать fact-таблицы;
    - можно проверить, что реально прислал WB;
    - можно отлаживать изменения API.

    Идемпотентность:
    - payload сериализуется стабильно через sort_keys=True;
    - считаем payload_hash;
    - перед вставкой проверяем, есть ли такой hash для seller_id + source;
    - повторный запуск не плодит одинаковые raw-события.
    """
    client = ch()
    sid = seller_id()

    def json_payload(it: Any) -> str:
        return json.dumps(it, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    objs = items if isinstance(items, list) else [items]

    batch = []
    seen = set()

    # Убираем дубли внутри текущей пачки до запроса в ClickHouse.
    for obj in objs:
        payload = json_payload(obj)
        h = md5_hex(payload)

        if h in seen:
            continue

        seen.add(h)
        batch.append({"payload": payload, "hash": h})

    if not batch:
        return

    existed = set()
    hashes = [x["hash"] for x in batch]

    # Проверяем существующие hash чанками, чтобы не делать слишком длинный IN.
    chunk_size = 1000

    for i in range(0, len(hashes), chunk_size):
        chunk = hashes[i:i + chunk_size]
        in_list = ",".join(f"'{h}'" for h in chunk)

        q = f"""
            SELECT payload_hash
            FROM raw_events
            WHERE seller_id = %(seller_id)s
              AND source = %(source)s
              AND payload_hash IN ({in_list})
        """

        rows = client.query(
            q,
            {"seller_id": sid, "source": source},
        ).result_rows

        for (h,) in rows:
            existed.add(str(h))

    rows_to_insert = []

    for x in batch:
        if x["hash"] in existed:
            continue

        rows_to_insert.append([
            sid,
            source,
            event_dt_naive,
            x["payload"],
            x["hash"],
            int(version),
        ])

    if rows_to_insert:
        client.insert(
            "raw_events",
            rows_to_insert,
            column_names=[
                "seller_id",
                "source",
                "event_dt",
                "payload",
                "payload_hash",
                "version",
            ],
        )