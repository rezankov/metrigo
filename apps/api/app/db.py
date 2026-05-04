"""
Database health checks for Metrigo API.

Что делает файл:
- проверяет подключение к Postgres;
- проверяет подключение к ClickHouse;
- используется endpoint'ом /health/db.

Важно:
- здесь только лёгкие ping-запросы;
- бизнес-запросы к данным будут в отдельных модулях.
"""

import psycopg
import clickhouse_connect

from app.config import settings


def check_postgres() -> bool:
    """
    Check Postgres availability with simple SELECT 1.
    """
    try:
        conninfo = (
            f"host={settings.postgres_host} "
            f"port={settings.postgres_port} "
            f"dbname={settings.postgres_db} "
            f"user={settings.postgres_user} "
            f"password={settings.postgres_password}"
        )

        with psycopg.connect(conninfo, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                return result[0] == 1
    except Exception:
        return False


def check_clickhouse() -> bool:
    """
    Check ClickHouse availability with simple SELECT 1.
    """
    try:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
        )

        result = client.query("SELECT 1").result_rows
        return result[0][0] == 1
    except Exception:
        return False