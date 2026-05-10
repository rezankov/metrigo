"""
chat_store.py — хранение истории чата
"""

from app.db_pg import pg


def get_or_create_thread(seller_id: str) -> int:
    """
    Получить основной thread для seller_id.
    """

    with pg() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT id
                FROM chat_threads
                WHERE seller_id = %s
                ORDER BY id
                LIMIT 1
                """,
                (seller_id,),
            )

            row = cur.fetchone()

            if row:
                return row[0]

            cur.execute(
                """
                INSERT INTO chat_threads (
                    seller_id,
                    title
                )
                VALUES (%s, %s)
                RETURNING id
                """,
                (seller_id, "Main chat"),
            )

            thread_id = cur.fetchone()[0]

            conn.commit()

            return thread_id


def save_message(
    thread_id: int,
    seller_id: str,
    role: str,
    content: str,
):
    """
    Сохранить сообщение в историю.
    """

    with pg() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO chat_messages (
                    thread_id,
                    seller_id,
                    role,
                    content
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    thread_id,
                    seller_id,
                    role,
                    content,
                ),
            )

            conn.commit()


def get_last_messages(
    thread_id: int,
    limit: int = 30,
):
    """
    Получить последние сообщения.
    """

    with pg() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT role, content, created_at
                FROM chat_messages
                WHERE thread_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (
                    thread_id,
                    limit,
                ),
            )

            rows = cur.fetchall()

    rows.reverse()

    return [
        {
            "role": row[0],
            "content": row[1],
            "created_at": row[2].isoformat(),
        }
        for row in rows
    ]