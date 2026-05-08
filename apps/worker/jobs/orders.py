"""
WB Orders ETL job for Metrigo.

Что делает файл:
- забирает заказы из WB Statistics API;
- сохраняет сырой payload в raw_events;
- нормализует данные в fact_orders;
- ведёт watermark по seller_id + source;
- пишет лог запуска в etl_runs.

Важно:
- watermark ведём по lastChangeDate;
- dedup_key строим стабильно;
- все записи обязательно содержат seller_id.
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from jobs.common import (
    ch,
    dt_to_ms,
    etl_run,
    get_watermark,
    insert_raw,
    md5_hex,
    parse_dt,
    safe_parse_dt,
    seller_id,
    set_watermark,
    stable_json,
    to_float,
    to_int,
    to_naive_utc,
    to_str,
    wb_get_list,
)


WB_ORDERS_URL = os.getenv(
    "WB_ORDERS_URL",
    "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
)

WB_ORDERS_FROM = os.getenv("WB_ORDERS_FROM", "").strip()


def orders_dedup_key(item: Dict[str, Any], payload_hash: str) -> str:
    """
    Build stable business key for order event.

    Приоритет:
    - srid;
    - odid;
    - gNumber;
    - fallback из бизнес-полей.
    """
    srid = to_str(item.get("srid"))
    if srid:
        return f"orders|srid|{srid}"

    odid = to_str(item.get("odid"))
    if odid:
        return f"orders|odid|{odid}"

    g_number = to_str(item.get("gNumber") or item.get("g_number"))
    if g_number:
        return f"orders|g_number|{g_number}"

    date_value = to_str(item.get("lastChangeDate") or item.get("date"))
    supplier_article = to_str(item.get("supplierArticle") or item.get("vendorCode"))
    nm_id = to_str(item.get("nmId") or item.get("nm_id"))
    barcode = to_str(item.get("barcode"))
    warehouse = to_str(item.get("warehouseName") or item.get("warehouse"))

    return (
        "orders|fallback|"
        f"{date_value}|{supplier_article}|{nm_id}|{barcode}|{warehouse}|{payload_hash}"
    )


def order_quantity(item: Dict[str, Any]) -> int:
    """
    Return order quantity.

    WB orders обычно идут построчно, поэтому если quantity нет — считаем 1.
    """
    qty = to_int(
        item.get("quantity")
        or item.get("qty")
        or item.get("count"),
        default=1,
    )

    return max(1, qty)


def to_bool(value: Any) -> bool:
    """
    Safely convert WB boolean-like values to bool.
    """
    if isinstance(value, bool):
        return value

    if value is None:
        return False

    text = str(value).strip().lower()

    return text in ("1", "true", "yes", "y", "да")


def run() -> None:
    """
    Run orders ETL.
    """
    source = "orders"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)
    version = dt_to_ms(started_at)

    wm = get_watermark(source, sid)

    if WB_ORDERS_FROM:
        backfill_dt = parse_dt(WB_ORDERS_FROM)
        date_from = backfill_dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_from = wm.astimezone(timezone.utc).strftime("%Y-%m-%d")

    try:
        items: List[Dict[str, Any]] = wb_get_list(
            WB_ORDERS_URL,
            params={"dateFrom": date_from},
        )

        insert_raw(
            sid=sid,
            source=source,
            items=items,
            event_dt=started_at,
            version=version,
            dedup_key_fn=orders_dedup_key,
        )

        rows: List[List[Any]] = []
        max_dt = wm

        for item in items:
            order_dt = (
                safe_parse_dt(item.get("date"))
                or safe_parse_dt(item.get("dateTime"))
                or started_at
            )

            last_change_dt = (
                safe_parse_dt(item.get("lastChangeDate"))
                or order_dt
            )

            if last_change_dt > max_dt:
                max_dt = last_change_dt

            cancel_dt = safe_parse_dt(
                item.get("cancelDate")
                or item.get("cancel_dt")
            )

            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            dedup_key = md5_hex(orders_dedup_key(item, payload_hash))

            rows.append(
                [
                    sid,
                    to_naive_utc(order_dt),
                    to_naive_utc(last_change_dt),
                    to_str(item.get("supplierArticle") or item.get("vendorCode")),
                    to_int(item.get("nmId") or item.get("nm_id")),
                    to_str(item.get("barcode")),
                    to_str(item.get("warehouseName") or item.get("warehouse")),
                    order_quantity(item),
                    to_float(item.get("totalPrice") or item.get("priceWithDisc") or item.get("finishedPrice") or item.get("total_price")),
                    to_int(item.get("discountPercent") or item.get("discount_percent")),
                    to_bool(item.get("isCancel") if item.get("isCancel") is not None else item.get("is_cancel")),
                      to_naive_utc(cancel_dt) if cancel_dt else None,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            dedup_keys = [row[13] for row in rows]
            existed = set()

            chunk_size = 1000
            for i in range(0, len(dedup_keys), chunk_size):
                chunk = dedup_keys[i:i + chunk_size]
                existing_rows = client.query(
                    """
                    SELECT
                        toString(dedup_key),
                        toString(payload_hash)
                    FROM fact_orders
                    WHERE seller_id = %(sid)s
                      AND dedup_key IN %(keys)s
                    """,
                    {"sid": sid, "keys": chunk},
                ).result_rows

                existed.update((str(row[0]), str(row[1])) for row in existing_rows)

            rows = [row for row in rows if (row[13], row[12]) not in existed]

        if rows:
            client.insert(
                "fact_orders",
                rows,
                column_names=[
                    "seller_id",
                    "date_time",
                    "last_change_date",
                    "supplier_article",
                    "nm_id",
                    "barcode",
                    "warehouse_name",
                    "quantity",
                    "total_price",
                    "discount_percent",
                    "is_cancel",
                    "cancel_dt",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        if max_dt > wm:
            set_watermark(source, max_dt, sid)

        message = f"seller_id={sid} loaded={len(items)} fact_orders={len(rows)} wm={max_dt.isoformat()}"
        etl_run(source, "empty" if not items else "ok", len(items), message, sid=sid)

        print(f"orders | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise