"""
WB Supplies ETL job for Metrigo.

Что делает файл:
- забирает список поставок WB;
- забирает товары внутри каждой поставки;
- сохраняет данные в fact_supplies и fact_supply_items;
- пишет raw payload в raw_events;
- пишет лог запуска в etl_runs.

Важно:
- это новый основной источник поставок;
- старая fact_incomes больше не используется;
- поставки и товары поставок храним как snapshot во времени.
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
    safe_parse_dt,
    seller_id,
    stable_json,
    to_int,
    to_naive_utc,
    to_str,
    wb_get_json,
    wb_post_json,
)


WB_SUPPLIES_LIST_URL = os.getenv(
    "WB_SUPPLIES_LIST_URL",
    "https://supplies-api.wildberries.ru/api/v1/supplies",
)

WB_SUPPLY_DETAILS_URL_TEMPLATE = os.getenv(
    "WB_SUPPLY_DETAILS_URL_TEMPLATE",
    "https://supplies-api.wildberries.ru/api/v1/supplies/{supply_id}",
)

WB_SUPPLY_GOODS_URL_TEMPLATE = os.getenv(
    "WB_SUPPLY_GOODS_URL_TEMPLATE",
    "https://supplies-api.wildberries.ru/api/v1/supplies/{supply_id}/goods",
)

WB_SUPPLIES_FROM = os.getenv("WB_SUPPLIES_FROM", "").strip()
WB_SUPPLIES_TO = os.getenv("WB_SUPPLIES_TO", "").strip()
WB_SUPPLIES_DATE_TYPE = os.getenv("WB_SUPPLIES_DATE_TYPE", "createDate").strip()
WB_SUPPLIES_LIMIT = int(os.getenv("WB_SUPPLIES_LIMIT", "1000"))


def period() -> tuple[str, str]:
    """
    Return supplies date filter period.
    """
    now = datetime.now(timezone.utc)

    if WB_SUPPLIES_FROM:
        date_from = parse_dt(WB_SUPPLIES_FROM).astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_from = now.replace(day=1).strftime("%Y-%m-%d")

    if WB_SUPPLIES_TO:
        date_to = parse_dt(WB_SUPPLIES_TO).astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_to = now.strftime("%Y-%m-%d")

    return date_from, date_to


def as_list(data: Any) -> List[Dict[str, Any]]:
    """
    Convert WB response to list.

    WB endpoints обычно возвращают list, но иногда встречаются обёртки result/data.
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("result", "data", "items", "supplies", "goods"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def supply_id_from(item: Dict[str, Any]) -> str:
    """
    Extract WB supply ID.
    """
    return to_str(
        item.get("supplyID")
        or item.get("supplyId")
        or item.get("supply_id")
        or item.get("ID")
        or item.get("id")
        or item.get("preorderID")
        or item.get("preorderId")
    )


def supply_dedup_key(item: Dict[str, Any], payload_hash: str, snapshot_dt: datetime) -> str:
    """
    Build snapshot key for one supply row.
    """
    supply_id = supply_id_from(item)

    if supply_id:
        return f"supplies|{snapshot_dt.isoformat()}|{supply_id}"

    return f"supplies|{snapshot_dt.isoformat()}|fallback|{payload_hash}"


def supply_item_dedup_key(
    item: Dict[str, Any],
    payload_hash: str,
    snapshot_dt: datetime,
    supply_id: str,
) -> str:
    """
    Build snapshot key for one supply item row.
    """
    nm_id = to_str(item.get("nmID") or item.get("nmId") or item.get("nm_id"))
    barcode = to_str(item.get("barcode"))
    seller_art = to_str(item.get("vendorCode") or item.get("supplierArticle"))

    if not any((nm_id, barcode, seller_art)):
        return f"supply_items|{snapshot_dt.isoformat()}|{supply_id}|fallback|{payload_hash}"

    return (
        "supply_items|"
        f"{snapshot_dt.isoformat()}|{supply_id}|{nm_id}|{barcode}|{seller_art}"
    )


def status_value(item: Dict[str, Any]) -> str:
    """
    Extract supply status.
    """
    return to_str(
        item.get("statusName")
        or item.get("status")
        or item.get("statusID")
        or item.get("statusId")
    )


def supply_type_value(item: Dict[str, Any]) -> str:
    """
    Extract supply type.
    """
    return to_str(
        item.get("supplyType")
        or item.get("boxTypeName")
        or item.get("boxTypeID")
        or item.get("crossBorderType")
    )


def warehouse_id_value(item: Dict[str, Any]) -> int:
    """
    Extract warehouse ID.
    """
    return to_int(
        item.get("warehouseID")
        or item.get("warehouseId")
        or item.get("actualWarehouseID")
        or item.get("actualWarehouseId")
    )


def warehouse_name_value(item: Dict[str, Any]) -> str:
    """
    Extract warehouse name.
    """
    return to_str(
        item.get("warehouseName")
        or item.get("actualWarehouseName")
        or item.get("transitWarehouseName")
    )


def created_at_value(item: Dict[str, Any]):
    """
    Extract supply create date.
    """
    return safe_parse_dt(
        item.get("createDate")
        or item.get("createdAt")
        or item.get("created_at")
    )


def closed_at_value(item: Dict[str, Any]):
    """
    Extract supply close/fact date.
    """
    return safe_parse_dt(
        item.get("factDate")
        or item.get("closedAt")
        or item.get("closed_at")
        or item.get("updatedDate")
    )


def load_supplies(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """
    Load supplies list with limit/offset pagination.
    """
    all_items: List[Dict[str, Any]] = []
    offset = 0

    body = {
        "dates": [
            {
                "from": date_from,
                "till": date_to,
                "type": WB_SUPPLIES_DATE_TYPE,
            }
        ]
    }

    while True:
        data = wb_post_json(
            WB_SUPPLIES_LIST_URL,
            params={"limit": WB_SUPPLIES_LIMIT, "offset": offset},
            payload=body,
        )

        page = as_list(data)

        if not page:
            break

        all_items.extend(page)

        if len(page) < WB_SUPPLIES_LIMIT:
            break

        offset += WB_SUPPLIES_LIMIT

    return all_items


def load_supply_details(supply_id: str) -> Dict[str, Any]:
    """
    Load supply details by supply ID.
    """
    url = WB_SUPPLY_DETAILS_URL_TEMPLATE.format(supply_id=supply_id)
    data = wb_get_json(url, params={})
    return data if isinstance(data, dict) else {}


def load_supply_goods(supply_id: str) -> List[Dict[str, Any]]:
    """
    Load supply goods with limit/offset pagination.
    """
    all_items: List[Dict[str, Any]] = []
    offset = 0
    limit = 1000
    url = WB_SUPPLY_GOODS_URL_TEMPLATE.format(supply_id=supply_id)

    while True:
        data = wb_get_json(
            url,
            params={
                "limit": limit,
                "offset": offset,
                "isPreorderID": "false",
            },
        )

        page = as_list(data)

        if not page:
            break

        all_items.extend(page)

        if len(page) < limit:
            break

        offset += limit

    return all_items


def run() -> None:
    """
    Run supplies ETL.
    """
    source = "supplies"
    items_source = "supply_items"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)
    snapshot_dt = started_at
    snapshot_dt_naive = to_naive_utc(snapshot_dt)
    snapshot_date = snapshot_dt_naive.date()
    version = dt_to_ms(started_at)

    date_from, date_to = period()

    try:
        supplies = load_supplies(date_from, date_to)

        enriched_supplies: List[Dict[str, Any]] = []
        all_goods: List[Dict[str, Any]] = []

        for supply in supplies:
            supply_id = supply_id_from(supply)

            details = load_supply_details(supply_id) if supply_id else {}
            merged_supply = {**supply, **details}

            enriched_supplies.append(merged_supply)

            goods = load_supply_goods(supply_id) if supply_id else []

            for good in goods:
                good["_supply_id"] = supply_id
                good["_warehouse_id"] = warehouse_id_value(merged_supply)
                good["_warehouse_name"] = warehouse_name_value(merged_supply)
                all_goods.append(good)

        insert_raw(
            sid=sid,
            source=source,
            items=enriched_supplies,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: supply_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
            ),
        )

        insert_raw(
            sid=sid,
            source=items_source,
            items=all_goods,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: supply_item_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
                to_str(item.get("_supply_id")),
            ),
        )

        supply_rows: List[List[Any]] = []

        for item in enriched_supplies:
            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            dedup_key = md5_hex(supply_dedup_key(item, payload_hash, snapshot_dt))

            created_at = created_at_value(item)
            closed_at = closed_at_value(item)

            supply_rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    supply_id_from(item),
                    status_value(item),
                    supply_type_value(item),
                    warehouse_id_value(item),
                    warehouse_name_value(item),
                    
                    to_naive_utc(created_at) if created_at else None,
                    to_naive_utc(closed_at) if closed_at else None,
                    payload,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        item_rows: List[List[Any]] = []

        for item in all_goods:
            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            supply_id = to_str(item.get("_supply_id"))
            dedup_key = md5_hex(
                supply_item_dedup_key(item, payload_hash, snapshot_dt, supply_id)
            )

            quantity = to_int(item.get("quantity"))
            quantity_plan = to_int(item.get("supplierBoxAmount") or item.get("quantity"))
            quantity_fact = to_int(
                item.get("acceptedQuantity")
                or item.get("readyForSaleQuantity")
                or item.get("quantityFact")
            )

            item_rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    supply_id,
                    to_int(item.get("nmID") or item.get("nmId") or item.get("nm_id")),
                    to_str(item.get("barcode")),
                    to_str(item.get("vendorCode") or item.get("supplierArticle")),
                    quantity,
                    quantity_plan,
                    quantity_fact,
                    to_str(item.get("status") or item.get("itemStatus")),
                    to_int(item.get("_warehouse_id")),
                    to_str(item.get("_warehouse_name")),
                    payload,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if supply_rows:
            client.insert(
                "fact_supplies",
                supply_rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "supply_id",
                    "supply_status",
                    "supply_type",
                    "warehouse_id",
                    "warehouse_name",
                    "created_at",
                    "closed_at",
                    "payload",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        if item_rows:
            client.insert(
                "fact_supply_items",
                item_rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "supply_id",
                    "nm_id",
                    "barcode",
                    "seller_art",
                    "quantity",
                    "quantity_plan",
                    "quantity_fact",
                    "item_status",
                    "warehouse_id",
                    "warehouse_name",
                    "payload",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        message = (
            f"seller_id={sid} supplies={len(enriched_supplies)} "
            f"supply_items={len(all_goods)} period={date_from}..{date_to} "
            f"snapshot={snapshot_dt.isoformat()}"
        )
        etl_run(source, "empty" if not enriched_supplies else "ok", len(enriched_supplies), message, sid=sid)

        print(f"supplies | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise