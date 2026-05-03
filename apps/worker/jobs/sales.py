"""
Sales loader for Metrigo worker.

Что делает файл:
- загружает продажи и возвраты из WB Statistics API;
- сохраняет полный сырой ответ в raw_events;
- нормализует строки в fact_sales;
- ведёт watermark по lastChangeDate;
- добавляет seller_id к каждой строке для multi-tenant архитектуры.

Почему sales важны:
- это основной источник выручки;
- по этим данным строятся продажи по дням, SKU, складам;
- seller_price используется как база для налога 6%;
- transfer_to_seller показывает сумму к перечислению после комиссии WB.
"""

import os
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


WB_SALES_URL = os.getenv(
    "WB_SALES_URL",
    "https://statistics-api.wildberries.ru/api/v1/supplier/sales",
)


def _s(it: Dict[str, Any], *keys: str, default: str = "") -> str:
    """
    Safely read string value from WB item by possible key names.

    WB API иногда меняет или дублирует названия полей.
    Поэтому для важных полей поддерживаем несколько вариантов ключей.
    """
    for k in keys:
        v = it.get(k)

        if v is None:
            continue

        return str(v)

    return default


def _f(it: Dict[str, Any], *keys: str, default: float = 0.0) -> float:
    """
    Safely read float value from WB item by possible key names.

    Нужен для денежных полей, где иногда приходят null,
    пустые значения или поля с разными названиями.
    """
    for k in keys:
        v = it.get(k)

        if v is None:
            continue

        try:
            return float(v)
        except Exception:
            continue

    return default


def _op_from_sell_id(sell_id: str) -> str:
    """
    Extract operation marker from sale ID.

    В WB возвраты обычно имеют saleID, начинающийся с "R".
    Мы сохраняем первый символ в op, чтобы потом проще отличать
    продажи от возвратов в аналитике.
    """
    if not sell_id:
        return ""

    return sell_id[:1]


def run():
    """
    Run sales loading job.

    Алгоритм:
    1. Берём watermark для seller_id + source=sales.
    2. Запрашиваем WB sales начиная с watermark.
    3. Сохраняем сырой JSON в raw_events.
    4. Преобразуем каждую строку в fact_sales.
    5. Обновляем watermark по максимальному lastChangeDate.
    6. Записываем результат запуска в etl_runs.

    Важно:
    - WB endpoint принимает dateFrom только как дату YYYY-MM-DD.
    - Поэтому при повторных загрузках возможны пересечения по дням.
    - Дедупликация обеспечивается через dedup_key + ReplacingMergeTree(version).
    """
    source = "sales"
    client = ch()
    sid = seller_id()

    wm = get_watermark(source)

    # WB sales endpoint принимает dateFrom как YYYY-MM-DD.
    # Из-за этого мы можем получать уже виденные строки за тот же день,
    # но это нормально: raw_events и fact_sales рассчитаны на повторы.
    date_from = wm.astimezone(timezone.utc).strftime("%Y-%m-%d")

    items: List[Dict[str, Any]] = wb_get_list(
        WB_SALES_URL,
        params={"dateFrom": date_from},
    )

    snapshot_dt = datetime.now(timezone.utc)
    snapshot_dt_naive = snapshot_dt.replace(tzinfo=None)
    version = dt_to_ms(snapshot_dt)

    # Сохраняем полный сырой ответ до нормализации.
    # Это позволяет позже пересобрать fact_sales, если изменится логика.
    insert_raw(source, items, event_dt_naive=snapshot_dt_naive, version=version)

    rows: List[List[Any]] = []
    max_dt = wm

    for it in items:
        # Для watermark используем lastChangeDate, потому что WB может
        # менять старые события после даты продажи.
        dt_val = (
            it.get("lastChangeDate")
            or it.get("date")
            or it.get("Date")
            or it.get("dateTime")
        )

        if dt_val:
            try:
                dtp = parse_dt(str(dt_val))

                if dtp > max_dt:
                    max_dt = dtp

                date_time = dtp.replace(tzinfo=None)
            except Exception:
                # Если WB прислал неожиданную дату, не роняем весь job.
                # Строку сохраняем со временем текущей загрузки.
                date_time = snapshot_dt_naive
        else:
            date_time = snapshot_dt_naive

        sale_date = date_time.date()

        sell_id = _s(it, "saleID", "saleId", "sellId", "sell_id", "srid", default="")
        op = _op_from_sell_id(sell_id)

        order_number = _s(it, "orderNumber", "order_number", "gNumber", default="")
        delivery_number = _s(it, "deliveryNumber", "delivery_number", "sticker", default="")

        seller_art = _s(it, "supplierArticle", "vendorCode", default="")
        wb_art = _s(it, "wbArticle", "wb_art", "nmId", "nm_id", default="")
        barcode = _s(it, "barcode", default="")

        warehouse = _s(it, "warehouseName", "warehouse", default="")
        warehouse_district = _s(
            it,
            "oblastOkrugName",
            "warehouseDistrict",
            "districtName",
            default="",
        )
        warehouse_region = _s(it, "regionName", "warehouseRegion", default="")

        # Денежные поля WB:
        # full_price — цена до скидок;
        # seller_price / priceWithDisc — цена продавца после скидки, база для налога;
        # buyers_price / finishedPrice — цена покупателя с учётом WB-скидок;
        # transfer_to_seller / forPay — сумма к перечислению продавцу.
        full_price = _f(it, "totalPrice", "fullPrice", "full_price", default=0.0)
        seller_discount = _f(
            it,
            "discountPercent",
            "sellerDiscount",
            "seller_discount",
            default=0.0,
        )
        wb_discount = _f(it, "spp", "wbDiscount", "wb_discount", default=0.0)

        buyers_price = _f(it, "finishedPrice", "buyerPrice", "buyers_price", default=0.0)
        seller_price = _f(it, "priceWithDisc", "sellerPrice", "seller_price", default=0.0)
        transfer_to_seller = _f(
            it,
            "forPay",
            "transferToSeller",
            "transfer_to_seller",
            default=0.0,
        )

        # dedup_key должен включать seller_id.
        # Иначе одинаковые saleID у разных кабинетов могли бы конфликтовать.
        dedup_key = md5_hex(f"{sid}|{sale_date}|{seller_art}|{warehouse}|{sell_id}")

        rows.append(
            [
                sid,
                date_time,
                sale_date,
                sell_id,
                op,
                order_number,
                delivery_number,
                seller_art,
                wb_art,
                barcode,
                warehouse,
                warehouse_district,
                warehouse_region,
                full_price,
                seller_discount,
                wb_discount,
                buyers_price,
                seller_price,
                transfer_to_seller,
                dedup_key,
                version,
            ]
        )

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
                "dedup_key",
                "version",
            ],
        )

    # Watermark обновляем только если реально увидели более свежий lastChangeDate.
    if max_dt > wm:
        set_watermark(source, max_dt)

    msg = f"loaded={len(items)} fact_sales={len(rows)} wm={max_dt.isoformat()}"

    etl_run(
        source,
        "empty" if len(items) == 0 else "ok",
        len(items),
        msg,
    )

    print(f"sales | seller_id={sid} | {msg}")