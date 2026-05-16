"""
build_fact_shop_expenses_monthly.py

Строит месячные расходы магазина.

Сюда попадают расходы, которые нельзя честно привязать к конкретному SKU:
- хранение
- штрафы
- удержания
- приемка
- неразнесенная логистика
- прочие магазинные расходы
"""

import argparse
import os

from app.db import ch


DEFAULT_SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"


def rebuild(seller_id: str) -> None:
    client = ch()

    print("[start] rebuild fact_shop_expenses_monthly")

    client.command(
        """
        TRUNCATE TABLE metrigo.fact_shop_expenses_monthly
        """
    )

    client.query(
        """
        INSERT INTO metrigo.fact_shop_expenses_monthly
        (
            seller_id,
            month,
            expense_type,
            expense_name,
            amount,
            source,
            comment,
            created_at
        )
        SELECT
            seller_id,
            month,
            expense_type,
            expense_name,
            toDecimal64(amount, 2) AS amount,
            source,
            comment,
            now() AS created_at
        FROM
        (
            SELECT
                seller_id,
                month,
                expense_type,
                expense_name,
                sum(raw_amount) AS amount,
                source,
                comment
            FROM
            (
                SELECT
                    seller_id,
                    toStartOfMonth(report_date) AS month,
                    'storage' AS expense_type,
                    'Хранение WB' AS expense_name,
                    ifNull(storage_fee, 0) AS raw_amount,
                    'fact_fin_report' AS source,
                    'Общий расход магазина из WB фин. отчета' AS comment
                FROM metrigo.fact_fin_report
                WHERE seller_id = %(seller_id)s

                UNION ALL

                SELECT
                    seller_id,
                    toStartOfMonth(report_date) AS month,
                    'penalty' AS expense_type,
                    'Штрафы WB' AS expense_name,
                    ifNull(penalty, 0) AS raw_amount,
                    'fact_fin_report' AS source,
                    'Общий расход магазина из WB фин. отчета' AS comment
                FROM metrigo.fact_fin_report
                WHERE seller_id = %(seller_id)s

                UNION ALL

                SELECT
                    seller_id,
                    toStartOfMonth(report_date) AS month,
                    'deduction' AS expense_type,
                    'Удержания WB' AS expense_name,
                    ifNull(deduction, 0) AS raw_amount,
                    'fact_fin_report' AS source,
                    'Общий расход магазина из WB фин. отчета' AS comment
                FROM metrigo.fact_fin_report
                WHERE seller_id = %(seller_id)s

                UNION ALL

                SELECT
                    seller_id,
                    toStartOfMonth(report_date) AS month,
                    'acceptance' AS expense_type,
                    'Платная приемка WB' AS expense_name,
                    ifNull(acceptance, 0) AS raw_amount,
                    'fact_fin_report' AS source,
                    'Общий расход магазина из WB фин. отчета' AS comment
                FROM metrigo.fact_fin_report
                WHERE seller_id = %(seller_id)s

                UNION ALL

                SELECT
                    seller_id,
                    toStartOfMonth(report_date) AS month,
                    'unallocated_logistics' AS expense_type,
                    'Неразнесенная логистика WB' AS expense_name,
                    if(
                        sa_name = '',
                        ifNull(delivery_rub, 0) + ifNull(rebill_logistic_cost, 0),
                        0
                    ) AS raw_amount,
                    'fact_fin_report' AS source,
                    'Логистика без привязки к SKU' AS comment
                FROM metrigo.fact_fin_report
                WHERE seller_id = %(seller_id)s

                UNION ALL

                SELECT
                    seller_id,
                    toStartOfMonth(report_date) AS month,
                    'additional_payment' AS expense_type,
                    'Дополнительные платежи WB' AS expense_name,
                    ifNull(additional_payment, 0) AS raw_amount,
                    'fact_fin_report' AS source,
                    'Дополнительные платежи из WB фин. отчета' AS comment
                FROM metrigo.fact_fin_report
                WHERE seller_id = %(seller_id)s
            )
            GROUP BY
                seller_id,
                month,
                expense_type,
                expense_name,
                source,
                comment
        )
        WHERE amount != 0
        """,
        {"seller_id": seller_id},
    )

    rows = client.query(
        """
        SELECT count()
        FROM metrigo.fact_shop_expenses_monthly
        WHERE seller_id = %(seller_id)s
        """,
        {"seller_id": seller_id},
    ).result_rows[0][0]

    print(f"[ok] inserted rows: {rows}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seller-id", default=DEFAULT_SELLER_ID)

    args = parser.parse_args()

    rebuild(seller_id=args.seller_id)


if __name__ == "__main__":
    main()