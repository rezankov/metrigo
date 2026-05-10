"""
tool_log.py — логирование вызовов AI-инструментов.
"""

import json
from app.db_pg import pg


def save_tool_call(
    seller_id: str,
    tool_name: str,
    args: dict,
    result: dict,
):
    """
    Сохранить вызов инструмента в PostgreSQL.
    """

    ok = bool(result.get("ok"))
    error = result.get("error")

    with pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai_tool_calls (
                    seller_id,
                    tool_name,
                    args_json,
                    result_json,
                    ok,
                    error
                )
                VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s)
                """,
                (
                    seller_id,
                    tool_name,
                    json.dumps(args, ensure_ascii=False),
                    json.dumps(result, ensure_ascii=False),
                    ok,
                    error,
                ),
            )

            conn.commit()