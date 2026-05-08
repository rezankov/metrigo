"""
WB Ads Daily Stats ETL job for Metrigo.

Что делает файл:
- забирает дневную статистику рекламных кампаний WB;
- нормализует данные в fact_ads_stats_daily;
- пишет raw payload в raw_events;
- пишет лог запуска в etl_runs.

Важно:
- статистика рекламы нужна для ROMI, ДРР, CPC, CTR и связки рекламы с заказами;
- данные храним по дням;
- для Promotion API нужен токен с доступом к категории Promotion/Реклама;
- все записи обязательно содержат seller_id.
"""

import os
from datetime import datetime, timezone, timedelta
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
    to_float,
    to_int,
    to_naive_utc,
    to_str,
    wb_get_json,
)


WB_ADS_STATS_URL = os.getenv(
    "WB_ADS_STATS_URL",
    "https://advert-api.wildberries.ru/adv/v3/fullstats",
)

WB_ADS_STATS_FROM = os.getenv("WB_ADS_STATS_FROM", "").strip()
WB_ADS_STATS_TO = os.getenv("WB_ADS_STATS_TO", "").strip()
WB_ADS_STATS_OVERLAP_DAYS = int(os.getenv("WB_ADS_STATS_OVERLAP_DAYS", "14"))


def period() -> tuple[str, str]:
    """
    Return ads stats period.

    Для ручного backfill используем WB_ADS_STATS_FROM/WB_ADS_STATS_TO.
    Для обычного запуска берём последние N дней, потому что рекламная
    статистика может доезжать с задержкой.
    """
    now = datetime.now(timezone.utc)

    if WB_ADS_STATS_FROM:
        date_from = parse_dt(WB_ADS_STATS_FROM).astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_from = (now - timedelta(days=WB_ADS_STATS_OVERLAP_DAYS)).strftime("%Y-%m-%d")

    if WB_ADS_STATS_TO:
        date_to = parse_dt(WB_ADS_STATS_TO).astimezone(timezone.utc).strftime("%Y-%m-%d")
    else:
        date_to = now.strftime("%Y-%m-%d")

    return date_from, date_to


def as_list(data: Any) -> List[Dict[str, Any]]:
    """
    Convert WB response to list.
    """
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("data", "result", "adverts", "items", "stats"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def advert_id_value(item: Dict[str, Any]) -> int:
    """
    Extract advert ID.
    """
    return to_int(
        item.get("advertId")
        or item.get("advertID")
        or item.get("advert_id")
        or item.get("id")
    )


def nm_id_value(item: Dict[str, Any]) -> int:
    """
    Extract nm_id.
    """
    return to_int(
        item.get("nmID")
        or item.get("nmId")
        or item.get("nm_id")
    )


def stat_date_value(item: Dict[str, Any], fallback_date):
    """
    Extract statistic date.
    """
    dt = safe_parse_dt(
        item.get("date")
        or item.get("statDate")
        or item.get("day")
    )

    if dt:
        return to_naive_utc(dt).date()

    return fallback_date


def views_value(item: Dict[str, Any]) -> int:
    """
    Extract views.
    """
    return to_int(item.get("views"))


def clicks_value(item: Dict[str, Any]) -> int:
    """
    Extract clicks.
    """
    return to_int(item.get("clicks"))


def ctr_value(item: Dict[str, Any]) -> float:
    """
    Extract CTR.
    """
    return to_float(item.get("ctr"))


def cpc_value(item: Dict[str, Any]) -> float:
    """
    Extract CPC.
    """
    return to_float(item.get("cpc"))


def spend_value(item: Dict[str, Any]) -> float:
    """
    Extract ad spend.
    """
    return to_float(
        item.get("sum")
        or item.get("spend")
        or item.get("cost")
    )


def orders_value(item: Dict[str, Any]) -> int:
    """
    Extract orders.
    """
    return to_int(item.get("orders"))


def shks_value(item: Dict[str, Any]) -> int:
    """
    Extract shks.
    """
    return to_int(item.get("shks"))


def sum_price_value(item: Dict[str, Any]) -> float:
    """
    Extract order amount.
    """
    return to_float(
        item.get("sum_price")
        or item.get("sumPrice")
        or item.get("sum_price_rub")
    )


def canceled_value(item: Dict[str, Any]) -> int:
    """
    Extract canceled count.
    """
    return to_int(
        item.get("canceled")
        or item.get("cancellations")
    )


def stat_dedup_key(
    item: Dict[str, Any],
    payload_hash: str,
    snapshot_dt: datetime,
    fallback_date,
) -> str:
    """
    Build stable key for daily ad statistics row.
    """
    stat_date = stat_date_value(item, fallback_date)
    advert_id = advert_id_value(item) or to_int(item.get("_advert_id"))
    nm_id = nm_id_value(item)

    if any((stat_date, advert_id, nm_id)):
        return f"ads_stats_daily|{stat_date}|{advert_id}|{nm_id}"

    return f"ads_stats_daily|fallback|{payload_hash}"


def flatten_stats_item(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten WB fullstats item.

    WB может отдавать:
    - дневные строки на уровне кампании;
    - app/nm breakdown внутри days/apps/nms.
    """
    advert_id = advert_id_value(item)

    days = item.get("days")

    if not isinstance(days, list) or not days:
        return [item]

    rows: List[Dict[str, Any]] = []

    for day in days:
        if not isinstance(day, dict):
            continue

        apps = day.get("apps")

        if isinstance(apps, list) and apps:
            for app in apps:
                if not isinstance(app, dict):
                    continue

                nms = app.get("nms")

                if isinstance(nms, list) and nms:
                    for nm in nms:
                        if not isinstance(nm, dict):
                            continue

                        row = {**item, **day, **app, **nm}
                        row["_advert_id"] = advert_id
                        row["_day_payload"] = day
                        row["_app_payload"] = app
                        row["_nm_payload"] = nm
                        rows.append(row)
                else:
                    row = {**item, **day, **app}
                    row["_advert_id"] = advert_id
                    row["_day_payload"] = day
                    row["_app_payload"] = app
                    rows.append(row)
        else:
            row = {**item, **day}
            row["_advert_id"] = advert_id
            row["_day_payload"] = day
            rows.append(row)

    return rows or [item]


def load_campaign_ids() -> List[int]:
    """
    Load advert IDs from already collected fact_ads_campaigns.
    """
    client = ch()

    rows = client.query(
        """
        SELECT DISTINCT advert_id
        FROM fact_ads_campaigns FINAL
        WHERE seller_id = %(sid)s
          AND advert_id > 0
        ORDER BY advert_id
        """,
        {"sid": seller_id()},
    ).result_rows

    return [int(row[0]) for row in rows]


def load_ads_stats(advert_ids: List[int], date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """
    Load daily stats for campaign IDs.

    fullstats принимает список campaign ids и период.
    """
    if not advert_ids:
        return []

    data = wb_get_json(
        WB_ADS_STATS_URL,
        params={
            "ids": ",".join(str(x) for x in advert_ids),
            "beginDate": date_from,
            "endDate": date_to,
        },
    )

    return as_list(data)


def run() -> None:
    """
    Run ads daily stats ETL.
    """
    source = "ads_stats_daily"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)

    snapshot_dt = started_at
    snapshot_dt_naive = to_naive_utc(snapshot_dt)
    snapshot_date = snapshot_dt_naive.date()

    version = dt_to_ms(started_at)

    date_from, date_to = period()

    try:
        advert_ids = load_campaign_ids()
        items = load_ads_stats(advert_ids, date_from, date_to)

        flat_rows: List[Dict[str, Any]] = []
        for item in items:
            flat_rows.extend(flatten_stats_item(item))

        insert_raw(
            sid=sid,
            source=source,
            items=flat_rows,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: stat_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
                snapshot_date,
            ),
        )

        rows: List[List[Any]] = []

        for item in flat_rows:
            payload = stable_json(item)
            payload_hash = md5_hex(payload)

            dedup_key = md5_hex(
                stat_dedup_key(
                    item,
                    payload_hash,
                    snapshot_dt,
                    snapshot_date,
                )
            )

            advert_id = advert_id_value(item) or to_int(item.get("_advert_id"))
            stat_date = stat_date_value(item, snapshot_date)

            rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    stat_date,
                    advert_id,
                    nm_id_value(item),
                    views_value(item),
                    clicks_value(item),
                    ctr_value(item),
                    cpc_value(item),
                    spend_value(item),
                    orders_value(item),
                    shks_value(item),
                    sum_price_value(item),
                    canceled_value(item),
                    payload,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            client.insert(
                "fact_ads_stats_daily",
                rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "stat_date",
                    "advert_id",
                    "nm_id",
                    "views",
                    "clicks",
                    "ctr",
                    "cpc",
                    "spend",
                    "orders",
                    "shks",
                    "sum_price",
                    "canceled",
                    "payload",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        message = (
            f"seller_id={sid} campaigns={len(advert_ids)} "
            f"raw_items={len(items)} rows={len(rows)} "
            f"period={date_from}..{date_to} snapshot={snapshot_dt.isoformat()}"
        )

        etl_run(
            source,
            "empty" if not rows else "ok",
            len(rows),
            message,
            sid=sid,
        )

        print(f"ads_stats_daily | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise