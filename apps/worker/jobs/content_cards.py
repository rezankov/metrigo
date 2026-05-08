"""
WB Content Cards ETL job for Metrigo.

Что делает файл:
- забирает карточки товаров WB;
- сохраняет snapshot карточек в fact_content_cards_snapshot;
- пишет raw payload в raw_events;
- пишет лог запуска в etl_runs.

Важно:
- карточки храним как snapshot во времени;
- это основа для анализа контента, SEO и изменений карточек;
- одна карточка = один nm_id;
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
    safe_parse_dt,
    to_int,
    to_naive_utc,
    to_str,
    wb_post_json,
)


WB_CONTENT_CARDS_URL = os.getenv(
    "WB_CONTENT_CARDS_URL",
    "https://content-api.wildberries.ru/content/v2/get/cards/list",
)

WB_CONTENT_LIMIT = int(os.getenv("WB_CONTENT_LIMIT", "100"))


def as_cards(data: Any) -> List[Dict[str, Any]]:
    """
    Extract cards list from WB response.
    """
    if not isinstance(data, dict):
        return []

    cards = (
        data.get("cards")
        or data.get("data", {}).get("cards")
        or data.get("data", {}).get("items")
    )

    if isinstance(cards, list):
        return cards

    return []


def nm_id_value(card: Dict[str, Any]) -> int:
    """
    Extract nm_id.
    """
    return to_int(card.get("nmID") or card.get("nmId"))


def imt_id_value(card: Dict[str, Any]) -> int:
    """
    Extract imt_id.
    """
    return to_int(card.get("imtID") or card.get("imtId"))


def vendor_code_value(card: Dict[str, Any]) -> str:
    """
    Extract vendor code.
    """
    return to_str(card.get("vendorCode"))


def subject_id_value(card: Dict[str, Any]) -> int:
    """
    Extract subject ID.
    """
    subject = card.get("subject") if isinstance(card.get("subject"), dict) else {}

    return to_int(
        subject.get("subjectID")
        or subject.get("subjectId")
        or card.get("subjectID")
    )


def subject_name_value(card: Dict[str, Any]) -> str:
    """
    Extract subject name.
    """
    subject = card.get("subject") if isinstance(card.get("subject"), dict) else {}

    return to_str(
        subject.get("name")
        or subject.get("subjectName")
        or card.get("subjectName")
    )


def brand_value(card: Dict[str, Any]) -> str:
    """
    Extract brand.
    """
    return to_str(card.get("brand"))


def title_value(card: Dict[str, Any]) -> str:
    """
    Extract title.
    """
    return to_str(
        card.get("title")
        or card.get("imtName")
        or card.get("name")
    )


def media_count_value(card: Dict[str, Any]) -> int:
    """
    Extract media count.
    """
    media = card.get("mediaFiles")

    if isinstance(media, list):
        return len(media)

    photos = card.get("photos")

    if isinstance(photos, list):
        return len(photos)

    return 0


def updated_at_value(card: Dict[str, Any]):
    """
    Extract card update datetime.
    """
    return safe_parse_dt(
        card.get("updatedAt")
        or card.get("updateAt")
        or card.get("createdAt")
    )


def card_dedup_key(
    card: Dict[str, Any],
    payload_hash: str,
    snapshot_dt: datetime,
) -> str:
    """
    Build snapshot dedup key for content card.
    """
    nm_id = nm_id_value(card)

    if nm_id:
        return f"content_cards|{snapshot_dt.isoformat()}|{nm_id}"

    vendor_code = vendor_code_value(card)

    if vendor_code:
        return f"content_cards|{snapshot_dt.isoformat()}|{vendor_code}"

    return f"content_cards|{snapshot_dt.isoformat()}|fallback|{payload_hash}"


def load_cards() -> List[Dict[str, Any]]:
    """
    Load all WB content cards using cursor pagination.
    """
    all_cards: List[Dict[str, Any]] = []

    cursor = {
        "limit": WB_CONTENT_LIMIT
    }

    while True:
        data = wb_post_json(
            WB_CONTENT_CARDS_URL,
            params={},
            payload={
                "settings": {
                    "cursor": cursor,
                    "filter": {
                        "withPhoto": -1
                    }
                }
            },
        )

        cards = as_cards(data)

        if not cards:
            break

        all_cards.extend(cards)

        cursor_data = (
            data.get("cursor")
            or data.get("data", {}).get("cursor")
            or {}
        )

        updated_at = cursor_data.get("updatedAt")
        nm_id = cursor_data.get("nmID")

        if not updated_at or not nm_id:
            break

        cursor = {
            "limit": WB_CONTENT_LIMIT,
            "updatedAt": updated_at,
            "nmID": nm_id,
        }

        if len(cards) < WB_CONTENT_LIMIT:
            break

    return all_cards


def run() -> None:
    """
    Run content cards ETL.
    """
    source = "content_cards"
    sid = seller_id()
    client = ch()

    started_at = datetime.now(timezone.utc)

    snapshot_dt = started_at
    snapshot_dt_naive = to_naive_utc(snapshot_dt)
    snapshot_date = snapshot_dt_naive.date()

    version = dt_to_ms(started_at)

    try:
        cards = load_cards()

        insert_raw(
            sid=sid,
            source=source,
            items=cards,
            event_dt=snapshot_dt,
            version=version,
            dedup_key_fn=lambda item, payload_hash: card_dedup_key(
                item,
                payload_hash,
                snapshot_dt,
            ),
        )

        rows: List[List[Any]] = []

        for card in cards:
            payload = stable_json(card)
            payload_hash = md5_hex(payload)

            dedup_key = md5_hex(
                card_dedup_key(card, payload_hash, snapshot_dt)
            )

            updated_at = updated_at_value(card)

            rows.append(
                [
                    sid,
                    snapshot_dt_naive,
                    snapshot_date,
                    nm_id_value(card),
                    imt_id_value(card),
                    vendor_code_value(card),
                    subject_id_value(card),
                    subject_name_value(card),
                    brand_value(card),
                    title_value(card),
                    media_count_value(card),
                    to_naive_utc(updated_at) if updated_at else None,
                    payload,
                    payload_hash,
                    dedup_key,
                    version,
                ]
            )

        if rows:
            client.insert(
                "fact_content_cards_snapshot",
                rows,
                column_names=[
                    "seller_id",
                    "snapshot_dt",
                    "snapshot_date",
                    "nm_id",
                    "imt_id",
                    "vendor_code",
                    "subject_id",
                    "subject_name",
                    "brand",
                    "title",
                    "media_count",
                    "updated_at",
                    "payload",
                    "payload_hash",
                    "dedup_key",
                    "version",
                ],
            )

        message = (
            f"seller_id={sid} cards={len(cards)} "
            f"snapshot={snapshot_dt.isoformat()}"
        )

        etl_run(
            source,
            "empty" if not cards else "ok",
            len(cards),
            message,
            sid=sid,
        )

        print(f"content_cards | {message}")

    except Exception as exc:
        message = f"seller_id={sid} error={type(exc).__name__}: {exc}"
        etl_run(source, "error", 0, message, sid=sid)
        raise