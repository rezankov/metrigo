"""
WB Tariffs ETL job for Metrigo.

Что делает файл:
- забирает тарифы хранения, логистики и приёмки WB;
- сохраняет snapshot тарифов в fact_tariffs;
- пишет raw payload в raw_events;
- пишет лог запуска в etl_runs.

Важно:
- тарифы WB меняются со временем;
- snapshot нужен для исторического расчёта unit-экономики;
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
    safe_parse_dt,
    wb_get_json,
)


WB_TARIFFS_BOX_URL = os.getenv(
    "WB_TARIFFS_BOX_URL",
    "https://common-api.wildberries.ru/api/v1/tariffs/box",
)

WB_TARIFFS_PALLET_URL = os.getenv(
    "WB_TARIFFS_PALLET_URL",
    "https://common-api.wildberries.ru/api/v1/tariffs/pallet",
)

WB_TARIFFS_ACCEPTANCE_URL = os.getenv(
    "WB_TARIFFS_ACCEPTANCE_URL",
    "https://common-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients",
)

WB_TARIFFS_DATE = os.getenv("WB_TARIFFS_DATE", "").strip()


def as_list(data: Any) -> List[Dict[str, Any]]:
    """
    Convert WB response to list.
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in (
            "response",
            "data",
            "warehouseList",
            "warehouses",
            "tariffs",
        ):
            value = data.get(key)

            if isinstance(value, list):
                return value

            if isinstance(value, dict):
                for nested_key in (
                    "warehouseList",
                    "warehouses",
                    "tariffs",
                ):
                    nested = value.get(nested_key)

                    if isinstance(nested, list):
                        return nested

    return []


def warehouse_id_value(item: Dict[str, Any]) -> int:
    """
    Extract warehouse ID.
    """
    return to_int(
        item.get("warehouseID")
        or item.get("warehouseId")
        or item.get("officeID")
        or item.get("officeId")
    )


def warehouse_name_value(item: Dict[str, Any]) -> str:
    """
    Extract warehouse name.
    """
    return to_str(
        item.get("warehouseName")
        or item.get("officeName")
        or item.get("name")
    )


def box_type_name_value(item: Dict[str, Any]) -> str:
    """
    Extract box type.
    """
    return to_str(
        item.get("boxTypeName")
        or item.get("boxType")
        or item.get("cargoType")
        or item.get("tariffType")
    )


def box_type_id_value(item: Dict[str, Any]) -> int:
    """
    Extract box type ID.
    """
    return to_int(
        item.get("boxTypeID")
        or item.get("boxTypeId")
        or item.get("box_type_id")
    )


def coefficient_value(item: Dict[str, Any]) -> float:
    """
    Extract coefficient.
    """
    return to_float(
        item.get("coefficient")
        or item.get("coef")
    )


def delivery_base_value(item: Dict[str, Any]) -> float:
    """
    Extract base delivery tariff.
    """
    return to_float(
        item.get("deliveryBase")
        or item.get("delivery")
    )


def delivery_liter_value(item: Dict[str, Any]) -> float:
    """
    Extract delivery per liter tariff.
    """
    return to_float(
        item.get("deliveryLiter")
        or item.get("deliveryAdditionalLiter")
        or item.get("deliveryBaseLiter")
    )


def storage_base_value(item: Dict[str, Any]) -> float:
    """
    Extract base storage tariff.
    """
    return to_float(
        item.get("storageBase")
        or item.get("storage")
    )


def storage_liter_value(item: Dict[str, Any]) -> float:
    """
    Extract storage per liter tariff.
    """
    return to_float(
        item.get("storageLiter")
        or item.get("storageAdditionalLiter")
        or item.get("storageBaseLiter")
    )


def acceptance_base_value(item: Dict[str, Any]) -> float:
    """
    Extract acceptance tariff.
    """
    return to_float(
        item.get("acceptance")
        or item.get("acceptanceBase")
    )


def tariff_dedup_key(
    item: Dict[str, Any],
    payload_hash: str,
    snapshot_dt: datetime,
    tariff_type: str,
) -> str:
    """
    Build tariff snapshot key.
    """
    warehouse_id = warehouse_id_value(item)
    warehouse_name = warehouse_name_value(item)
    box_type_name = box_type_name_value(item)
    box_type_id = box_type_id_value(item)
    tariff_date = to_str(item.get("date"))

    if any((warehouse_id, warehouse_name, box_type_name, box_type_id, tariff_date)):
        return (
            "tariffs|"
            f"{snapshot_dt.isoformat()}|"
            f"{tariff_type}|"
            f"{tariff_date}|"
            f"{warehouse_id}|"
            f"{warehouse_name}|"
            f"{box_type_name}|"
            f"{box_type_id}"
        )

    return (
        "tariffs|"
        f"{snapshot_dt.isoformat()}|"
        f"{tariff_type}|fallback|{payload_hash}"
    )


def tariff_params() -> Dict[str, Any]:
    """
    Return tariff request params.
    """
    if WB_TARIFFS_DATE:
        return {"date": WB_TARIFFS_DATE}

    return {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d")}


def tariff_date_value(item: Dict[str, Any], snapshot_date):
    """
    Extract tariff effective date.
    """
    dt = safe_parse_dt(item.get("date"))

    if dt:
        return to_naive_utc(dt).date()

    return snapshot_date


def load_box_tariffs() -> List[Dict[str, Any]]:
    """
    Load box tariffs.
    """
    data = wb_get_json(WB_TARIFFS_BOX_URL, params=tariff_params())
    return as_list(data)


def load_pallet_tariffs() -> List[Dict[str, Any]]:
    """
    Load pallet tariffs.
    """
    data = wb_get_json(WB_TARIFFS_PALLET_URL, params=tariff_params())
    return as_list(data)


def load_acceptance_tariffs() -> List[Dict[str, Any]]:
    """
    Load acceptance tariffs.
    """
    data = wb_get_json(WB_TARIFFS_ACCEPTANCE_URL, params=tariff_params())
    return as_list(data)


def run() -> None:
    """
    Run tariffs ETL.
    """
    source = "tariffs"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)

    snapshot_dt = started_at
    snapshot_dt_naive = to_naive_utc(snapshot_dt)
    snapshot_date = snapshot_dt_naive.date()

    version = dt_to_ms(started_at)

    try:
        box_tariffs = load_box_tariffs()
        pallet_tariffs = load_pallet_tariffs()
        acceptance_tariffs = load_acceptance_tariffs()

        all_rows: List[Dict[str, Any]] = []

        for item in box_tariffs:
            row = dict(item)
            row["_tariff_type"] = "box"
            all_rows.append(row)

        for item in pallet_tariffs:
            row = dict(item)
            row["_tariff_type"] = "pallet"
            all_rows.append(row)

        for item in acceptance_tariffs:
            row = dict(item)
            row["_tariff_type"] = "acceptance"
            all_rows.append(row)

        insert_raw(
            sid=sid,
            source=source,
            items=all_rows,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: tariff_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
                to_str(item.get("_tariff_type")),
            ),
        )

        rows: List[List[Any]] = []

        for item in all_rows:
            payload = stable_json(item)
            payload_hash = md5_hex(payload)

            tariff_type = to_str(item.get("_tariff_type"))

            dedup_key = md5_hex(
                tariff_dedup_key(
                    item,
                    payload_hash,
                    snapshot_dt,
                    tariff_type,
                )
            )

            rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    tariff_date_value(item, snapshot_date),
                    tariff_type,
                    warehouse_id_value(item),
                    warehouse_name_value(item),
                    box_type_name_value(item),
                    coefficient_value(item),
                    delivery_base_value(item),
                    delivery_liter_value(item),
                    storage_base_value(item),
                    storage_liter_value(item),
                    acceptance_base_value(item),
                    payload,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            client.insert(
                "fact_tariffs",
                rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "tariff_date",
                    "tariff_type",
                    "warehouse_id",
                    "warehouse_name",
                    "box_type_name",
                    "coefficient",
                    "delivery_base",
                    "delivery_liter",
                    "storage_base",
                    "storage_liter",
                    "acceptance_base",
                    "payload",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        message = (
            f"seller_id={sid} "
            f"box_tariffs={len(box_tariffs)} "
            f"pallet_tariffs={len(pallet_tariffs)} "
            f"acceptance_tariffs={len(acceptance_tariffs)} "
            f"snapshot={snapshot_dt.isoformat()}"
        )

        etl_run(
            source,
            "empty" if not rows else "ok",
            len(rows),
            message,
            sid=sid,
        )

        print(f"tariffs | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise