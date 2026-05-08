"""
WB Prices and Discounts ETL job for Metrigo.

Что делает файл:
- забирает текущие цены и скидки товаров WB;
- сохраняет snapshot в fact_prices_discounts_snapshot;
- пишет raw payload в raw_events;
- пишет лог запуска в etl_runs.

Важно:
- цены и скидки храним как snapshot во времени;
- это нужно для анализа влияния скидок на продажи, маржу и выкуп;
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
    seller_id,
    stable_json,
    to_float,
    to_int,
    to_naive_utc,
    to_str,
    wb_get_json,
)


WB_PRICES_LIST_URL = os.getenv(
    "WB_PRICES_LIST_URL",
    "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter",
)

WB_PRICES_LIMIT = int(os.getenv("WB_PRICES_LIMIT", "1000"))


def as_list(data: Any) -> List[Dict[str, Any]]:
    """
    Convert WB response to list.

    WB prices endpoint обычно возвращает data.listGoods,
    но оставляем гибкий парсер для изменений формата.
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        data_obj = data.get("data")
        if isinstance(data_obj, dict):
            value = data_obj.get("listGoods")
            if isinstance(value, list):
                return value

        for key in ("listGoods", "goods", "items", "data", "result"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def price_rows_from_item(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize WB price response item to rows.

    У товара могут быть размеры, и цена может быть на уровне size.
    Если sizes нет, возвращаем одну строку по товару.
    """
    sizes = item.get("sizes")

    if not isinstance(sizes, list) or not sizes:
        return [item]

    rows: List[Dict[str, Any]] = []

    for size in sizes:
        if isinstance(size, dict):
            merged = {**item, **size}
            merged["_parent"] = item
            rows.append(merged)

    return rows or [item]


def nm_id_value(item: Dict[str, Any]) -> int:
    """
    Extract WB nm_id.
    """
    return to_int(item.get("nmID") or item.get("nmId") or item.get("nm_id"))


def vendor_code_value(item: Dict[str, Any]) -> str:
    """
    Extract vendor code.
    """
    parent = item.get("_parent") if isinstance(item.get("_parent"), dict) else {}

    return to_str(
        item.get("vendorCode")
        or item.get("supplierArticle")
        or parent.get("vendorCode")
        or parent.get("supplierArticle")
    )


def barcode_value(item: Dict[str, Any]) -> str:
    """
    Extract barcode.
    """
    parent = item.get("_parent") if isinstance(item.get("_parent"), dict) else {}

    return to_str(
        item.get("barcode")
        or item.get("techSizeName")
        or parent.get("barcode")
    )


def size_id_value(item: Dict[str, Any]) -> int:
    """
    Extract WB size ID.
    """
    return to_int(
        item.get("sizeID")
        or item.get("sizeId")
        or item.get("chrtID")
        or item.get("chrtId")
    )


def price_value(item: Dict[str, Any]) -> float:
    """
    Extract base price.
    """
    return to_float(
        item.get("price")
        or item.get("priceU")
        or item.get("basicPrice")
    )


def discount_value(item: Dict[str, Any]) -> int:
    """
    Extract seller discount percent.
    """
    return to_int(
        item.get("discount")
        or item.get("discountPercent")
        or item.get("discountedPercent")
    )


def discounted_price_value(item: Dict[str, Any]) -> float:
    """
    Extract discounted/current price.
    """
    return to_float(
        item.get("discountedPrice")
        or item.get("discounted_price")
        or item.get("priceWithDiscount")
        or item.get("priceWithDisc")
    )


def club_discount_value(item: Dict[str, Any]) -> int:
    """
    Extract WB Club discount.
    """
    return to_int(
        item.get("clubDiscount")
        or item.get("clubDiscountPercent")
    )


def price_dedup_key(item: Dict[str, Any], payload_hash: str, snapshot_dt: datetime) -> str:
    """
    Build snapshot key for price row.
    """
    nm_id = nm_id_value(item)
    vendor_code = vendor_code_value(item)
    barcode = barcode_value(item)
    size_id = size_id_value(item)

    if any((nm_id, vendor_code, barcode, size_id)):
        return (
            "prices|"
            f"{snapshot_dt.isoformat()}|{nm_id}|{vendor_code}|{barcode}|{size_id}"
        )

    return f"prices|{snapshot_dt.isoformat()}|fallback|{payload_hash}"


def load_prices() -> List[Dict[str, Any]]:
    """
    Load all prices/discounts with limit/offset pagination.
    """
    all_items: List[Dict[str, Any]] = []
    offset = 0

    while True:
        data = wb_get_json(
            WB_PRICES_LIST_URL,
            params={
                "limit": WB_PRICES_LIMIT,
                "offset": offset,
            },
        )

        page = as_list(data)

        if not page:
            break

        all_items.extend(page)

        if len(page) < WB_PRICES_LIMIT:
            break

        offset += WB_PRICES_LIMIT

    return all_items


def run() -> None:
    """
    Run prices ETL.
    """
    source = "prices"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)
    snapshot_dt = started_at
    snapshot_dt_naive = to_naive_utc(snapshot_dt)
    snapshot_date = snapshot_dt_naive.date()
    version = dt_to_ms(started_at)

    try:
        items = load_prices()

        normalized_rows: List[Dict[str, Any]] = []
        for item in items:
            normalized_rows.extend(price_rows_from_item(item))

        insert_raw(
            sid=sid,
            source=source,
            items=normalized_rows,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: price_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
            ),
        )

        rows: List[List[Any]] = []

        for item in normalized_rows:
            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            dedup_key = md5_hex(price_dedup_key(item, payload_hash, snapshot_dt))

            rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    nm_id_value(item),
                    vendor_code_value(item),
                    barcode_value(item),
                    size_id_value(item),
                    price_value(item),
                    discount_value(item),
                    discounted_price_value(item),
                    club_discount_value(item),
                    payload,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            client.insert(
                "fact_prices_discounts_snapshot",
                rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "nm_id",
                    "vendor_code",
                    "barcode",
                    "size_id",
                    "price",
                    "discount",
                    "discounted_price",
                    "club_discount",
                    "payload",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        message = (
            f"seller_id={sid} goods={len(items)} "
            f"price_rows={len(rows)} snapshot={snapshot_dt.isoformat()}"
        )
        etl_run(source, "empty" if not items else "ok", len(items), message, sid=sid)

        print(f"prices | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise