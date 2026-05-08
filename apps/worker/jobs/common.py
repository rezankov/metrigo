"""
Common helpers for Metrigo WB workers.

Что делает файл:
- хранит единый способ подключения к ClickHouse;
- даёт общий seller_id для multi-tenant логики;
- управляет watermark по seller_id + source;
- сохраняет raw_events с payload_hash и dedup_key;
- содержит безопасные функции парсинга дат и чисел;
- выполняет запросы к WB API с повторными попытками.

Почему это важно:
- все worker должны работать по одному контракту;
- схема ClickHouse v1 ожидает одинаковые поля во всех raw/fact таблицах;
- если логика меняется здесь, она меняется централизованно.
"""

import hashlib
import json
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import clickhouse_connect
import requests
from dateutil import parser as dtparser


CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_HTTP_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "metrigo")

WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()
WB_REQUEST_SLEEP = float(os.getenv("WB_REQUEST_SLEEP", "0.35"))
WB_RETRY_BASE_SLEEP = float(os.getenv("WB_RETRY_BASE_SLEEP", "2.0"))
WB_MAX_RETRIES = int(os.getenv("WB_MAX_RETRIES", "10"))


def seller_id() -> str:
    """
    Return current seller_id.

    В v1 берём seller_id из env.
    Позже для SaaS этот seller_id будет приходить из аккаунта/очереди задач.
    """
    return os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"


def ch():
    """
    Create ClickHouse client.

    Клиент создаётся на каждый run/job.
    Это проще и надёжнее для короткоживущих docker compose run процессов.
    """
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_HTTP_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )


def md5_hex(value: str) -> str:
    """
    Return stable md5 hex string.

    Используется для:
    - payload_hash;
    - dedup_key;
    - cogs_key.
    """
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def stable_json(obj: Any) -> str:
    """
    Serialize object to stable JSON.

    Один и тот же payload должен давать один и тот же payload_hash.
    """
    return json.dumps(
        obj,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def parse_dt(value: Any) -> datetime:
    """
    Parse date/time to timezone-aware UTC datetime.

    WB иногда отдаёт строки без timezone.
    Тогда считаем их UTC, чтобы вся аналитика была в едином времени.
    """
    if isinstance(value, datetime):
        dt = value
    else:
        dt = dtparser.parse(str(value))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def safe_parse_dt(value: Any) -> Optional[datetime]:
    """
    Safely parse WB datetime.

    Возвращает None для:
    - пустых значений;
    - 0001-01-01;
    - некорректных дат.
    """
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    if text.startswith("0001-01-01") or text.startswith("0000-"):
        return None

    try:
        dt = parse_dt(text)
    except Exception:
        return None

    if dt.year < 1971:
        return None

    return dt


def to_naive_utc(dt: datetime) -> datetime:
    """
    Convert datetime to naive UTC datetime for ClickHouse DateTime.

    Если datetime пришёл без timezone, считаем его UTC.
    Это защищает от зависимости от timezone сервера.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def dt_to_ms(dt: datetime) -> int:
    """
    Convert datetime to milliseconds timestamp.

    Используем как version для ReplacingMergeTree.
    Чем свежее запуск, тем выше version.
    """
    return int(dt.timestamp() * 1000)


def to_str(value: Any) -> str:
    """
    Safely convert value to string.
    """
    if value is None:
        return ""
    return str(value)


def to_int(value: Any, default: int = 0) -> int:
    """
    Safely convert value to int.
    """
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert value to float.

    WB иногда отдаёт десятичные числа строками с запятой: "78,2".
    """
    try:
        if value is None:
            return default

        if isinstance(value, str):
            value = value.strip().replace(",", ".")

            if not value:
                return default

        return float(value)
    except Exception:
        return default


def get_watermark(source: str, sid: str) -> datetime:
    """
    Read latest watermark for seller_id + source.

    Если watermark отсутствует, стартуем с дефолтного окна.
    Для начального запуска это даёт безопасную загрузку последних данных.
    Исторический бэкфилл будем делать отдельным env-параметром в конкретном worker.
    """
    client = ch()

    rows = client.query(
        """
        SELECT watermark
        FROM etl_state FINAL
        WHERE seller_id = %(sid)s
          AND source = %(source)s
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        {"sid": sid, "source": source},
    ).result_rows

    if not rows or rows[0][0] is None:
        return datetime.now(timezone.utc) - timedelta(days=2)

    wm = rows[0][0]

    if isinstance(wm, str):
        wm = parse_dt(wm)

    if wm.tzinfo is None:
        wm = wm.replace(tzinfo=timezone.utc)

    return wm.astimezone(timezone.utc)


def set_watermark(source: str, wm: datetime, sid: str) -> None:
    """
    Write watermark for seller_id + source.

    ReplacingMergeTree(updated_at) оставит последнее значение.
    """
    client = ch()
    wm_naive = to_naive_utc(wm)

    client.command(
        """
        INSERT INTO etl_state
        (seller_id, source, watermark)
        VALUES (%(sid)s, %(source)s, %(watermark)s)
        """,
        {
            "sid": sid,
            "source": source,
            "watermark": wm_naive,
        },
    )


def etl_run(
    source: str,
    status: str,
    loaded: int,
    message: str,
    sid: Optional[str] = None,
) -> None:
    """
    Write worker run log to etl_runs.
    """
    client = ch()

    client.command(
        """
        INSERT INTO etl_runs
        (seller_id, source, status, loaded, message)
        VALUES (%(sid)s, %(source)s, %(status)s, %(loaded)s, %(message)s)
        """,
        {
            "sid": sid or seller_id(),
            "source": source,
            "status": status,
            "loaded": int(loaded),
            "message": str(message)[:2000],
        },
    )


def insert_raw(
    *,
    sid: str,
    source: str,
    items: Any,
    event_dt: Optional[datetime],
    version: int,
    dedup_key_fn=None,
) -> int:
    """
    Insert raw WB payloads into raw_events.

    Контракт raw_events:
    - seller_id;
    - source;
    - event_dt;
    - payload;
    - payload_hash;
    - dedup_key;
    - version.

    dedup_key_fn нужен, чтобы каждый worker задавал бизнес-ключ события.
    Если функции нет, dedup_key = payload_hash.
    """
    client = ch()

    if isinstance(items, list):
        objects = items
    else:
        objects = [items]

    rows: List[List[Any]] = []
    seen = set()

    event_dt_naive = to_naive_utc(event_dt) if event_dt else None

    for obj in objects:
        payload = stable_json(obj)
        payload_hash = md5_hex(payload)

        if dedup_key_fn:
            raw_key = dedup_key_fn(obj, payload_hash)
            dedup_key = md5_hex(str(raw_key))
        else:
            dedup_key = payload_hash

        unique_key = (sid, source, dedup_key, payload_hash)

        if unique_key in seen:
            continue

        seen.add(unique_key)

        rows.append(
            [
                sid,
                source,
                event_dt_naive,
                payload,
                payload_hash,
                dedup_key,
                int(version),
            ]
        )

    if rows:
        dedup_keys = [row[5] for row in rows]
        existed = set()

        chunk_size = 1000
        for i in range(0, len(dedup_keys), chunk_size):
            chunk = dedup_keys[i:i + chunk_size]
            existing_rows = client.query(
                """
                SELECT
                  toString(dedup_key),
                  toString(payload_hash)
                FROM raw_events
                WHERE seller_id = %(sid)s
                  AND source = %(source)s
                  AND dedup_key IN %(keys)s
                """,
                {
                    "sid": sid,
                    "source": source,
                    "keys": chunk,
                },
            ).result_rows

            existed.update((str(row[0]), str(row[1])) for row in existing_rows)

        rows = [row for row in rows if (row[5], row[4]) not in existed]

    if not rows:
        return 0

    client.insert(
        "raw_events",
        rows,
        column_names=[
            "seller_id",
            "source",
            "event_dt",
            "payload",
            "payload_hash",
            "dedup_key",
            "version",
        ],
    )

    return len(rows)


def wb_get_list(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    GET WB endpoint that returns a list.

    Делает retry на:
    - 429 rate limit;
    - временные 5xx ошибки WB.
    """
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN is empty")

    headers = {"Authorization": WB_STATS_TOKEN}

    for attempt in range(WB_MAX_RETRIES):
        time.sleep(WB_REQUEST_SLEEP)

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=180,
        )

        if response.status_code == 429:
            time.sleep(WB_RETRY_BASE_SLEEP + attempt * WB_RETRY_BASE_SLEEP)
            continue

        if response.status_code in (500, 502, 503, 504):
            time.sleep(WB_RETRY_BASE_SLEEP + attempt * WB_RETRY_BASE_SLEEP)
            continue

        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected WB response type: {type(data)}")

        return data

    raise RuntimeError(f"Too many retries from WB API: {url}")


def wb_get_json(url: str, params: Dict[str, Any]) -> Any:
    """
    GET WB endpoint that returns any JSON object.
    """
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN is empty")

    headers = {"Authorization": WB_STATS_TOKEN}

    for attempt in range(WB_MAX_RETRIES):
        time.sleep(WB_REQUEST_SLEEP)

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=180,
        )

        if response.status_code == 429:
            time.sleep(WB_RETRY_BASE_SLEEP + attempt * WB_RETRY_BASE_SLEEP)
            continue

        if response.status_code in (500, 502, 503, 504):
            time.sleep(WB_RETRY_BASE_SLEEP + attempt * WB_RETRY_BASE_SLEEP)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Too many retries from WB API: {url}")


def wb_post_json(url: str, params: Dict[str, Any], payload: Any) -> Any:
    """
    POST WB endpoint that returns any JSON object.
    """
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN is empty")

    headers = {"Authorization": WB_STATS_TOKEN}

    for attempt in range(WB_MAX_RETRIES):
        time.sleep(WB_REQUEST_SLEEP)

        response = requests.post(
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=180,
        )

        if response.status_code == 429:
            time.sleep(WB_RETRY_BASE_SLEEP + attempt * WB_RETRY_BASE_SLEEP)
            continue

        if response.status_code in (500, 502, 503, 504):
            time.sleep(WB_RETRY_BASE_SLEEP + attempt * WB_RETRY_BASE_SLEEP)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Too many retries from WB API: {url}")