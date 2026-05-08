"""
WB Ads Campaigns ETL job for Metrigo.

Что делает файл:
- забирает список рекламных кампаний WB;
- сохраняет snapshot кампаний в fact_ads_campaigns;
- пишет raw payload в raw_events;
- пишет лог запуска в etl_runs.

Важно:
- кампании храним как snapshot во времени;
- это основа для будущей связки рекламы с продажами, заказами и финрезультатом;
- для Promotion API нужен токен с доступом к категории Promotion;
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
    safe_parse_dt,
    seller_id,
    stable_json,
    to_float,
    to_int,
    to_naive_utc,
    to_str,
    wb_get_json,
)


WB_ADS_CAMPAIGNS_URL = os.getenv(
    "WB_ADS_CAMPAIGNS_URL",
    "https://advert-api.wildberries.ru/adv/v1/promotion/count",
)


def as_groups(data: Any) -> List[Dict[str, Any]]:
    """
    Extract grouped campaign lists from WB response.

    Endpoint обычно возвращает adverts/groups внутри response или напрямую.
    """
    if isinstance(data, list):
        return data

    if not isinstance(data, dict):
        return []

    for key in ("adverts", "groups", "data", "response", "result"):
        value = data.get(key)

        if isinstance(value, list):
            return value

        if isinstance(value, dict):
            nested = as_groups(value)
            if nested:
                return nested

    return []


def campaigns_from_group(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract campaign rows from one WB group.

    WB groups campaigns by type/status.
    """
    campaigns = (
        group.get("advert_list")
        or group.get("advertList")
        or group.get("adverts")
        or group.get("campaigns")
    )

    if not isinstance(campaigns, list):
        return []

    rows: List[Dict[str, Any]] = []

    group_type = to_str(
        group.get("type")
        or group.get("advert_type")
        or group.get("advertType")
    )
    group_status = to_str(
        group.get("status")
        or group.get("advert_status")
        or group.get("advertStatus")
    )

    for campaign in campaigns:
        if not isinstance(campaign, dict):
            continue

        row = dict(campaign)

        if group_type and not row.get("_group_type"):
            row["_group_type"] = group_type

        if group_status and not row.get("_group_status"):
            row["_group_status"] = group_status

        rows.append(row)

    return rows


def advert_id_value(item: Dict[str, Any]) -> int:
    """
    Extract advert/campaign ID.
    """
    return to_int(
        item.get("advertId")
        or item.get("advertID")
        or item.get("advert_id")
        or item.get("id")
    )


def advert_name_value(item: Dict[str, Any]) -> str:
    """
    Extract advert/campaign name.
    """
    return to_str(
        item.get("name")
        or item.get("advertName")
        or item.get("campaignName")
    )


def advert_type_value(item: Dict[str, Any]) -> str:
    """
    Extract advert/campaign type.
    """
    return to_str(
        item.get("type")
        or item.get("advertType")
        or item.get("advert_type")
        or item.get("_group_type")
    )


def status_value(item: Dict[str, Any]) -> str:
    """
    Extract campaign status.
    """
    return to_str(
        item.get("status")
        or item.get("advertStatus")
        or item.get("advert_status")
        or item.get("_group_status")
    )


def payment_type_value(item: Dict[str, Any]) -> str:
    """
    Extract payment/bid type.
    """
    return to_str(
        item.get("paymentType")
        or item.get("payment_type")
        or item.get("bid_type")
        or item.get("bidType")
    )


def daily_budget_value(item: Dict[str, Any]) -> float:
    """
    Extract daily budget if present.
    """
    return to_float(
        item.get("dailyBudget")
        or item.get("daily_budget")
        or item.get("budget")
    )


def created_at_value(item: Dict[str, Any]):
    """
    Extract campaign creation date.
    """
    return safe_parse_dt(
        item.get("createTime")
        or item.get("createdAt")
        or item.get("created_at")
    )


def started_at_value(item: Dict[str, Any]):
    """
    Extract campaign start date.
    """
    return safe_parse_dt(
        item.get("startTime")
        or item.get("startedAt")
        or item.get("startDate")
    )


def ended_at_value(item: Dict[str, Any]):
    """
    Extract campaign end date.
    """
    return safe_parse_dt(
        item.get("endTime")
        or item.get("endedAt")
        or item.get("endDate")
    )


def campaign_dedup_key(
    item: Dict[str, Any],
    payload_hash: str,
    snapshot_dt: datetime,
) -> str:
    """
    Build snapshot dedup key for campaign.
    """
    advert_id = advert_id_value(item)

    if advert_id:
        return f"ads_campaigns|{snapshot_dt.isoformat()}|{advert_id}"

    return f"ads_campaigns|{snapshot_dt.isoformat()}|fallback|{payload_hash}"


def load_campaigns() -> List[Dict[str, Any]]:
    """
    Load WB campaigns list.
    """
    data = wb_get_json(WB_ADS_CAMPAIGNS_URL, params={})
    groups = as_groups(data)

    rows: List[Dict[str, Any]] = []

    for group in groups:
        if not isinstance(group, dict):
            continue

        rows.extend(campaigns_from_group(group))

    return rows


def run() -> None:
    """
    Run ads campaigns ETL.
    """
    source = "ads_campaigns"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)

    snapshot_dt = started_at
    snapshot_dt_naive = to_naive_utc(snapshot_dt)
    snapshot_date = snapshot_dt_naive.date()

    version = dt_to_ms(started_at)

    try:
        campaigns = load_campaigns()

        insert_raw(
            sid=sid,
            source=source,
            items=campaigns,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: campaign_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
            ),
        )

        rows: List[List[Any]] = []

        for item in campaigns:
            payload = stable_json(item)
            payload_hash = md5_hex(payload)
            dedup_key = md5_hex(campaign_dedup_key(item, payload_hash, snapshot_dt))

            created_at = created_at_value(item)
            started_at = started_at_value(item)
            ended_at = ended_at_value(item)

            rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    advert_id_value(item),
                    advert_name_value(item),
                    advert_type_value(item),
                    status_value(item),
                    payment_type_value(item),
                    daily_budget_value(item),
                    to_naive_utc(created_at) if created_at else None,
                    to_naive_utc(started_at) if started_at else None,
                    to_naive_utc(ended_at) if ended_at else None,
                    payload,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            client.insert(
                "fact_ads_campaigns",
                rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "advert_id",
                    "advert_name",
                    "advert_type",
                    "status",
                    "payment_type",
                    "daily_budget",
                    "created_at",
                    "started_at",
                    "ended_at",
                    "payload",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        message = (
            f"seller_id={sid} campaigns={len(campaigns)} "
            f"snapshot={snapshot_dt.isoformat()}"
        )

        etl_run(
            source,
            "empty" if not campaigns else "ok",
            len(campaigns),
            message,
            sid=sid,
        )

        print(f"ads_campaigns | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise