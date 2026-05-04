"""
WB Orders ETL job

Что делает:
- Забирает заказы из WB API
- Пишет raw_events (идемпотентно по payload_hash)
- Пишет факт в fact_orders
- Обновляет watermark по lastChangeDate

Особенности:
- Multi-tenant: используется seller_id
- Дедупликация через dedup_key
- quantity нормализуется (WB иногда не отдаёт)
"""

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from .common import (
    ch,
    wb_get_list,
    get_watermark,
    set_watermark,
    dt_to_ms,
    parse_dt,
    etl_run,
    md5_hex,
    insert_raw,
    seller_id,
)

WB_ORDERS_URL = os.getenv(
    "WB_ORDERS_URL",
    "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
)


def _get_quantity(it: Dict[str, Any]) -> int:
    """WB иногда не отдаёт quantity → ставим минимум 1"""
    for k in ("quantity", "qty", "count"):
        v = it.get(k)
        if v is not None:
            try:
                return max(1, int(v))
            except Exception:
                return 1
    return 1


def _dedup_key(it: Dict[str, Any], dt: datetime) -> str:
    """Стабильный ключ события"""
    srid = str(it.get("srid") or "").strip()
    if srid:
        return md5_hex(f"srid|{srid}")

    return md5_hex(
        f"{dt.isoformat()}|{it.get('barcode')}|{it.get('supplierArticle')}|{it.get('warehouseName')}"
    )


def run():
    """
    Основной ETL-процесс orders
    """
    source = "orders"
    sid = seller_id()
    client = ch()

    # Получаем watermark (с учётом seller_id)
    wm = get_watermark(source, sid)
    date_from = wm.strftime("%Y-%m-%dT%H:%M:%S")

    # Запрос в WB
    items: List[Dict[str, Any]] = wb_get_list(
        WB_ORDERS_URL,
        {"dateFrom": date_from},
    )

    snapshot_dt = datetime.now(timezone.utc)
    version = dt_to_ms(snapshot_dt)

    # --- RAW EVENTS ---
    insert_raw(
      source=source,
      items=items,
			event_dt_naive=snapshot_dt.replace(tzinfo=None),
      version=version,
    )

    # --- FACT ---
    rows = []
    max_dt = wm

    for it in items:
        # date_time
        try:
            dt = parse_dt(it.get("date"))
        except Exception:
            dt = snapshot_dt

        # last_change_date (для watermark)
        try:
            lcd = parse_dt(it.get("lastChangeDate"))
            if lcd > max_dt:
                max_dt = lcd
        except Exception:
            lcd = dt

        rows.append(
            [
                sid,
                dt.replace(tzinfo=None),
                lcd.replace(tzinfo=None),
                str(it.get("supplierArticle") or ""),
                int(it.get("nmId") or 0),
                str(it.get("barcode") or ""),
                str(it.get("warehouseName") or ""),
                _get_quantity(it),
                float(it.get("totalPrice") or 0),
                int(it.get("discountPercent") or 0),
                bool(it.get("isCancel") or False),
                None,  # cancel_dt (пока не используем)
                md5_hex(json.dumps(it, sort_keys=True)),
                _dedup_key(it, dt),
                version,
            ]
        )

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

    # --- WATERMARK ---
    if max_dt > wm:
        set_watermark(source, max_dt, sid)

    # --- LOG ---
    etl_run(
        source,
        "empty" if len(items) == 0 else "ok",
        len(items),
        f"seller_id={sid}",
    )

    print(f"orders | seller_id={sid} loaded={len(items)}")