"""
db_pg.py — подключение к PostgreSQL

Используется для:
- chat history
- AI memory
- threads
- future notifications/reports
"""

import os
import psycopg


def pg():
    """
    Создать подключение к PostgreSQL.
    """

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "metrigo"),
        user=os.getenv("POSTGRES_USER", "metrigo"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )