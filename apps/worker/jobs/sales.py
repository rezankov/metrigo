"""
WB Sales ETL job for Metrigo.

Что делает файл:
- забирает продажи и возвраты из WB Statistics API;
- сохраняет сырой payload в raw_events;
- нормализует данные в fact_sales;
- ведёт watermark по seller_id + source;
- пишет лог запуска в etl_runs.

Важно:
- это эталонный worker-паттерн для остальных сборщиков;
- все ключи дедупликации строятся стабильно;
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
    to_naive_utc,
    to_str,
    wb_get_list,
)


WB_SALES_URL = os.getenv(
    "WB_SALES_URL",
    "https://statistics-api.wildberries.ru/api/v1/supplier/sales",
)

WB_SALES_FROM = os.getenv("WB_SALES_FROM", "").strip()


def sales_dedup_key(item: Dict[str, Any], payload_hash: str) -> str:
    """
    Build stable business key for raw sales event.

    saleID — главный идентификатор WB для продажи/возврата.
    Если его нет, используем fallback из основных бизнес-полей.
    """
    sell_id = to_str(
        item.get("saleID")
        or item.get("saleId")
        or item.get("sellId")
        or item.get("sell_id")
    )

    if sell_id:
        return f"sales|sale_id|{sell_id}"
    srid = to_str(item.get("srid"))
    if srid:
        return f"sales|srid|{srid}"

    date_value = to_str(item.get("lastChangeDate") or item.get("date"))
    seller_art = to_str(item.get("supplierArticle") or item.get("vendorCode"))
    warehouse = to_str(item.get("warehouseName") or item.get("warehouse"))
    barcode = to_str(item.get("barcode"))

    return f"sales|fallback|{date_value}|{seller_art}|{warehouse}|{barcode}|{payload_hash}"


def sale_op(sell_id: str) -> str:
    """
    Return operation marker from sell_id.

    В старых данных WB:
    - S... обычно продажа;
    - R... обычно возврат.
    """
    if not sell_id:
        return ""
    return sell_id[:1]


def run() -> None:
    """
    Run sales ETL.
    """
    source = "sales"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)
    version = dt_to_ms(started_at)

    wm = get_watermark(source, sid)

    if WB_SALES_FROM:
        backfill_dt = parse_dt(WB_SALES_FROM)
        date_from = backfill_dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_from = wm.astimezone(timezone.utc).strftime("%Y-%m-%d")

    try:
        items: List[Dict[str, Any]] = wb_get_list(
            WB_SALES_URL,
            params={"dateFrom": date_from},
        )

        insert_raw(
            sid=sid,
            source=source,
            items=items,
            event_dt=started_at,
            version=version,
            dedup_key_fn=sales_dedup_key,
        )

        rows: List[List[Any]] = []
        max_dt = wm

        for item in items:
            dt_value = (
                item.get("lastChangeDate")
                or item.get("date")
                or item.get("Date")
                or item.get("dateTime")
            )

            parsed_dt = safe_parse_dt(dt_value) or started_at

            if parsed_dt > max_dt:
                max_dt = parsed_dt

            date_time = to_naive_utc(parsed_dt)
            sale_date = date_time.date()

            sell_id = to_str(
                item.get("saleID")
                or item.get("saleId")
                or item.get("sellId")
                or item.get("sell_id")
            )

            seller_art = to_str(item.get("supplierArticle") or item.get("vendorCode"))
            warehouse = to_str(item.get("warehouseName") or item.get("warehouse"))

            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            dedup_key = md5_hex(sales_dedup_key(item, payload_hash))

            rows.append(
                [
                    sid,
                    date_time,
                    sale_date,
                    sell_id,
                    sale_op(sell_id),
                    to_str(item.get("orderNumber") or item.get("order_number") or item.get("gNumber")),
                    to_str(item.get("deliveryNumber") or item.get("delivery_number") or item.get("sticker")),
                    seller_art,
                    to_str(item.get("wbArticle") or item.get("wb_art") or item.get("nmId") or item.get("nm_id")),
                    to_str(item.get("barcode")),
                    warehouse,
                    to_str(item.get("oblastOkrugName") or item.get("warehouseDistrict") or item.get("districtName")),
                    to_str(item.get("regionName") or item.get("warehouseRegion")),
                    to_float(item.get("totalPrice") or item.get("fullPrice") or item.get("full_price")),
                    to_float(item.get("discountPercent") or item.get("sellerDiscount") or item.get("seller_discount")),
                    to_float(item.get("spp") or item.get("wbDiscount") or item.get("wb_discount")),
                    to_float(item.get("finishedPrice") or item.get("buyerPrice") or item.get("buyers_price")),
                    to_float(item.get("priceWithDisc") or item.get("sellerPrice") or item.get("seller_price")),
                    to_float(item.get("forPay") or item.get("transferToSeller") or item.get("transfer_to_seller")),
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            dedup_keys = [row[20] for row in rows]
            existed = set()

            chunk_size = 1000
            for i in range(0, len(dedup_keys), chunk_size):
                chunk = dedup_keys[i:i + chunk_size]
                existing_rows = client.query(
                    """
                    SELECT
                      toString(dedup_key),
                      toString(payload_hash)
                    FROM fact_sales
                    WHERE seller_id = %(sid)s
                      AND dedup_key IN %(keys)s
                    """,
                    {"sid": sid, "keys": chunk},
                ).result_rows

                existed.update((str(row[0]), str(row[1])) for row in existing_rows)

            rows = [row for row in rows if (row[20], row[19]) not in existed]

        if rows:
            client.insert(
                "fact_sales",
                rows,
                column_names=[
                    "seller_id",
                    "date_time",
                    "sale_date",
                    "sell_id",
                    "op",
                    "order_number",
                    "delivery_number",
                    "seller_art",
                    "wb_art",
                    "barcode",
                    "warehouse",
                    "warehouse_district",
                    "warehouse_region",
                    "full_price",
                    "seller_discount",
                    "wb_discount",
                    "buyers_price",
                    "seller_price",
                    "transfer_to_seller",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        if max_dt > wm:
            set_watermark(source, max_dt, sid)

        message = f"seller_id={sid} loaded={len(items)} fact_sales={len(rows)} wm={max_dt.isoformat()}"
        etl_run(source, "empty" if not items else "ok", len(items), message, sid=sid)

        print(f"sales | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise