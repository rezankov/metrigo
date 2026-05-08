"""
WB Stocks ETL job for Metrigo.

Что делает файл:
- забирает текущие остатки из WB Statistics API;
- сохраняет сырой payload в raw_events;
- нормализует данные в fact_stock_snapshot;
- пишет лог запуска в etl_runs.

Важно:
- stocks — это snapshot, а не событие;
- dedup_key включает snapshot_dt, чтобы разные снимки не схлопывались;
- все записи обязательно содержат seller_id.
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from jobs.common import (
    ch,
    dt_to_ms,
    etl_run,
    insert_raw,
    md5_hex,
    parse_dt,
    seller_id,
    stable_json,
    to_int,
    to_naive_utc,
    to_str,
    wb_get_list,
)


WB_STOCKS_URL = os.getenv(
    "WB_STOCKS_URL",
    "https://statistics-api.wildberries.ru/api/v1/supplier/stocks",
)

WB_STOCKS_FROM = os.getenv("WB_STOCKS_FROM", "").strip()


def stocks_date_from() -> str:
    """
    Return dateFrom for stocks API.

    WB stocks требует dateFrom.
    Для обычного запуска используем сегодня.
    Для ручного backfill/проверки можно задать WB_STOCKS_FROM.
    """
    if WB_STOCKS_FROM:
        return parse_dt(WB_STOCKS_FROM).astimezone(timezone.utc).strftime("%Y-%m-%d")

    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def stocks_dedup_key(item: Dict[str, Any], payload_hash: str, snapshot_dt: datetime) -> str:
    """
    Build stable key for one stock row inside one snapshot.
    """
    warehouse = to_str(item.get("warehouseName") or item.get("warehouse"))
    seller_art = to_str(item.get("supplierArticle") or item.get("vendorCode"))
    barcode = to_str(item.get("barcode"))
    nm_id = to_str(item.get("nmId") or item.get("nm_id"))

    return (
        "stocks|"
        f"{snapshot_dt.isoformat()}|{warehouse}|{seller_art}|{barcode}|{nm_id}"
    )


def run() -> None:
    """
    Run stocks ETL.
    """
    source = "stocks"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)
    snapshot_dt = started_at
    snapshot_dt_naive = to_naive_utc(snapshot_dt)
    snapshot_date = snapshot_dt_naive.date()
    version = dt_to_ms(started_at)

    try:
        items: List[Dict[str, Any]] = wb_get_list(
            WB_STOCKS_URL,
            params={"dateFrom": stocks_date_from()},
        )

        insert_raw(
            sid=sid,
            source=source,
            items=items,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: stocks_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
            ),
        )

        rows: List[List[Any]] = []

        for item in items:
            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            dedup_key = md5_hex(stocks_dedup_key(item, payload_hash, snapshot_dt))

            rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    to_str(item.get("warehouseName") or item.get("warehouse")),
                    to_str(item.get("supplierArticle") or item.get("vendorCode")),
                    to_str(item.get("barcode")),
                    to_int(item.get("quantity") or item.get("qty")),
                    version,
                    to_int(item.get("inWayToClient") or item.get("in_way_to_client")),
                    to_int(item.get("inWayFromClient") or item.get("in_way_from_client")),
                    to_int(item.get("quantityFull") or item.get("qty_full")),
                    payload_hash,
                    dedup_key,
                ]
            )

        if rows:
            dedup_keys = [row[12] for row in rows]
            existed = set()

            chunk_size = 1000
            for i in range(0, len(dedup_keys), chunk_size):
                chunk = dedup_keys[i:i + chunk_size]
                existing_rows = client.query(
                    """
                    SELECT
                        toString(dedup_key),
                        toString(payload_hash)
                    FROM fact_stock_snapshot
                    WHERE seller_id = %(sid)s
                      AND dedup_key IN %(keys)s
                    """,
                    {"sid": sid, "keys": chunk},
                ).result_rows

                existed.update((str(row[0]), str(row[1])) for row in existing_rows)

            rows = [row for row in rows if (row[12], row[11]) not in existed]

        if rows:
            client.insert(
                "fact_stock_snapshot",
                rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "warehouse",
                    "seller_art",
                    "barcode",
                    "qty",
                    "version",
                    "in_way_to_client",
                    "in_way_from_client",
                    "qty_full",
                    "payload_hash",
                    "dedup_key",
                ],
            )

        message = (
            f"seller_id={sid} loaded={len(items)} "
            f"fact_stock_snapshot={len(rows)} snapshot={snapshot_dt.isoformat()}"
        )
        etl_run(source, "empty" if not items else "ok", len(items), message, sid=sid)

        print(f"stocks | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise