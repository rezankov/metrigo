"""
WB Financial Report ETL job for Metrigo.

Что делает файл:
- забирает финансовый отчёт WB;
- сохраняет сырой payload в raw_events;
- нормализует данные в fact_fin_report;
- ведёт watermark по seller_id + source;
- пишет лог запуска в etl_runs.

Важно:
- финансовый отчёт — главный источник комиссий, логистики, штрафов и выплат;
- endpoint может отдавать данные постранично через rrdid;
- dedup_key строим по rrd_id, fallback — по srid или payload_hash.
"""

import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

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


WB_FIN_REPORT_URL = os.getenv(
    "WB_FIN_REPORT_URL",
    "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
)

WB_FIN_FROM = os.getenv("WB_FIN_FROM", "").strip()
WB_FIN_TO = os.getenv("WB_FIN_TO", "").strip()
WB_FIN_LIMIT = int(os.getenv("WB_FIN_LIMIT", "100000"))
WB_FIN_OVERLAP_DAYS = int(os.getenv("WB_FIN_OVERLAP_DAYS", "14"))


def fin_report_dedup_key(item: Dict[str, Any], payload_hash: str) -> str:
    """
    Build stable business key for financial report row.
    """
    rrd_id = to_str(item.get("rrd_id") or item.get("rrdId") or item.get("rrdid"))
    if rrd_id:
        return f"fin_report|rrd_id|{rrd_id}"

    srid = to_str(item.get("srid"))
    if srid:
        oper_name = to_str(item.get("supplier_oper_name"))
        rr_dt = to_str(item.get("rr_dt"))
        return f"fin_report|srid|{srid}|{oper_name}|{rr_dt}"

    return f"fin_report|fallback|{payload_hash}"


def report_period(wm: datetime) -> tuple[str, str]:
    """
    Return dateFrom/dateTo for WB financial report.

    Для backfill:
    - WB_FIN_FROM
    - WB_FIN_TO

    Для обычного запуска:
    - от watermark
    - до текущего дня
    """
    if WB_FIN_FROM:
        date_from = parse_dt(WB_FIN_FROM).astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        overlapped_wm = wm - timedelta(days=WB_FIN_OVERLAP_DAYS)
        date_from = overlapped_wm.astimezone(timezone.utc).strftime("%Y-%m-%d")

    if WB_FIN_TO:
        date_to = parse_dt(WB_FIN_TO).astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_to = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return date_from, date_to


def fin_report_date(item: Dict[str, Any], fallback: datetime) -> datetime:
    """
    Pick the best date for report_date/watermark.
    """
    return (
        safe_parse_dt(item.get("rr_dt"))
        or safe_parse_dt(item.get("sale_dt"))
        or safe_parse_dt(item.get("order_dt"))
        or safe_parse_dt(item.get("supplier_oper_dt"))
        or fallback
    )


def wb_fin_report_items(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """
    Load financial report with rrdid pagination.

    WB usually returns up to limit rows per request.
    If page length is lower than limit, pagination is complete.
    """
    all_items: List[Dict[str, Any]] = []
    rrdid = 0

    while True:
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "limit": WB_FIN_LIMIT,
            "rrdid": rrdid,
        }

        page = wb_get_list(WB_FIN_REPORT_URL, params=params)

        if not page:
            break

        all_items.extend(page)

        if len(page) < WB_FIN_LIMIT:
            break

        max_rrd_id = max(
            to_int(item.get("rrd_id") or item.get("rrdId") or item.get("rrdid"))
            for item in page
        )

        if max_rrd_id <= rrdid:
            break

        rrdid = max_rrd_id

    return all_items


def nullable_dt(value: Any) -> Optional[datetime]:
    """
    Convert optional WB datetime to naive UTC datetime for ClickHouse Nullable(DateTime).
    """
    dt = safe_parse_dt(value)
    return to_naive_utc(dt) if dt else None


def run() -> None:
    """
    Run financial report ETL.
    """
    source = "fin_report"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)
    version = dt_to_ms(started_at)

    wm = get_watermark(source, sid)
    date_from, date_to = report_period(wm)

    try:
        items = wb_fin_report_items(date_from, date_to)

        insert_raw(
            sid=sid,
            source=source,
            items=items,
            event_dt=started_at,
            version=version,
            dedup_key_fn=fin_report_dedup_key,
        )

        rows: List[List[Any]] = []
        max_dt = wm

        for item in items:
            report_dt = fin_report_date(item, started_at)

            if report_dt > max_dt:
                max_dt = report_dt

            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            dedup_key = md5_hex(fin_report_dedup_key(item, payload_hash))

            rows.append(
                [
                    sid,
                    to_naive_utc(report_dt).date(),
                    nullable_dt(item.get("rr_dt")),
                    to_int(item.get("realizationreport_id") or item.get("rr_id")),
                    to_int(item.get("rrd_id") or item.get("rrdId") or item.get("rrdid")),
                    to_str(item.get("doc_type_name")),
                    to_str(item.get("supplier_oper_name")),
                    to_str(item.get("operation_type")),
                    to_str(item.get("operation_type_name")),
                    to_str(item.get("srid")),
                    to_int(item.get("nm_id") or item.get("nmId")),
                    to_str(item.get("sa_name")),
                    to_str(item.get("ts_name")),
                    to_str(item.get("brand_name")),
                    to_str(item.get("subject_name")),
                    to_str(item.get("supplier_article") or item.get("supplierArticle")),
                    to_str(item.get("barcode")),
                    to_int(item.get("quantity")),
                    to_float(item.get("retail_price")),
                    to_float(item.get("retail_amount")),
                    to_float(item.get("sale_percent")),
                    to_float(item.get("commission_percent")),
                    to_str(item.get("office_name")),
                    to_str(item.get("warehouse_name") or item.get("warehouse")),
                    nullable_dt(item.get("supplier_oper_dt")),
                    nullable_dt(item.get("order_dt")),
                    nullable_dt(item.get("sale_dt")),
                    to_str(item.get("shk_id")),
                    to_float(item.get("retail_price_withdisc_rub")),
                    to_int(item.get("delivery_amount")),
                    to_int(item.get("return_amount")),
                    to_float(item.get("delivery_rub")),
                    to_str(item.get("gi_box_type_name")),
                    to_float(item.get("product_discount_for_report")),
                    to_float(item.get("supplier_promo")),
                    to_str(item.get("rid")),
                    to_float(item.get("ppvz_spp_prc")),
                    to_float(item.get("ppvz_kvw_prc_base")),
                    to_float(item.get("ppvz_kvw_prc")),
                    to_float(item.get("sup_rating_prc_up")),
                    to_float(item.get("is_kgvp_v2")),
                    to_float(item.get("ppvz_sales_commission")),
                    to_float(item.get("ppvz_for_pay")),
                    to_float(item.get("ppvz_reward")),
                    to_float(item.get("acquiring_fee")),
                    to_float(item.get("acquiring_percent")),
                    to_str(item.get("acquiring_bank")),
                    to_float(item.get("ppvz_vw")),
                    to_float(item.get("ppvz_vw_nds")),
                    to_int(item.get("ppvz_office_id")),
                    to_str(item.get("ppvz_office_name")),
                    to_int(item.get("ppvz_supplier_id")),
                    to_str(item.get("ppvz_supplier_name")),
                    to_str(item.get("ppvz_inn")),
                    to_str(item.get("declaration_number")),
                    to_str(item.get("bonus_type_name")),
                    to_str(item.get("sticker_id")),
                    to_str(item.get("site_country")),
                    to_float(item.get("penalty")),
                    to_float(item.get("additional_payment")),
                    to_float(item.get("rebill_logistic_cost")),
                    to_float(item.get("rebill_logistic_org")),
                    to_str(item.get("kiz")),
                    to_float(item.get("storage_fee")),
                    to_float(item.get("deduction")),
                    to_float(item.get("acceptance")),
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            dedup_keys = [row[67] for row in rows]
            existed = set()

            chunk_size = 1000
            for i in range(0, len(dedup_keys), chunk_size):
                chunk = dedup_keys[i:i + chunk_size]
                existing_rows = client.query(
                    """
                    SELECT
                        toString(dedup_key),
                        toString(payload_hash)
                    FROM fact_fin_report
                    WHERE seller_id = %(sid)s
                      AND dedup_key IN %(keys)s
                    """,
                    {"sid": sid, "keys": chunk},
                ).result_rows

                existed.update((str(row[0]), str(row[1])) for row in existing_rows)

            rows = [row for row in rows if (row[67], row[66]) not in existed]

        if rows:
            client.insert(
                "fact_fin_report",
                rows,
                column_names=[
                    "seller_id",
                    "report_date",
                    "rr_dt",
                    "rr_id",
                    "rrd_id",
                    "doc_type_name",
                    "supplier_oper_name",
                    "operation_type",
                    "operation_type_name",
                    "srid",
                    "nm_id",
                    "sa_name",
                    "ts_name",
                    "brand_name",
                    "subject_name",
                    "supplier_article",
                    "barcode",
                    "quantity",
                    "retail_price",
                    "retail_amount",
                    "sale_percent",
                    "commission_percent",
                    "office_name",
                    "warehouse",
                    "supplier_oper_dt",
                    "order_dt",
                    "sale_dt",
                    "shk_id",
                    "retail_price_withdisc_rub",
                    "delivery_amount",
                    "return_amount",
                    "delivery_rub",
                    "gi_box_type_name",
                    "product_discount_for_report",
                    "supplier_promo",
                    "rid",
                    "ppvz_spp_prc",
                    "ppvz_kvw_prc_base",
                    "ppvz_kvw_prc",
                    "sup_rating_prc_up",
                    "is_kgvp_v2",
                    "ppvz_sales_commission",
                    "ppvz_for_pay",
                    "ppvz_reward",
                    "acquiring_fee",
                    "acquiring_percent",
                    "acquiring_bank",
                    "ppvz_vw",
                    "ppvz_vw_nds",
                    "ppvz_office_id",
                    "ppvz_office_name",
                    "ppvz_supplier_id",
                    "ppvz_supplier_name",
                    "ppvz_inn",
                    "declaration_number",
                    "bonus_type_name",
                    "sticker_id",
                    "site_country",
                    "penalty",
                    "additional_payment",
                    "rebill_logistic_cost",
                    "rebill_logistic_org",
                    "kiz",
                    "storage_fee",
                    "deduction",
                    "acceptance",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        if max_dt > wm:
            set_watermark(source, max_dt, sid)

        message = (
            f"seller_id={sid} loaded={len(items)} "
            f"fact_fin_report={len(rows)} period={date_from}..{date_to} "
            f"wm={max_dt.isoformat()}"
        )
        etl_run(source, "empty" if not items else "ok", len(items), message, sid=sid)

        print(f"fin_report | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise