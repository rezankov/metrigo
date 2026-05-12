"""
get_net_profit_context.py — расчет чистой прибыли Metrigo.
"""

from typing import Dict
from app.db import ch


TAX_RATE = 0.06


def get_net_profit_context(seller_id: str, days: int = 7) -> Dict:
    """
    Рассчитать чистую прибыль за период по данным финансового отчета WB.

    Период автоматически ограничивается последней датой, доступной
    в fact_fin_report, чтобы не смешивать свежие продажи с еще не
    пришедшим финансовым отчетом.
    """

    client = ch()
    days = max(1, min(int(days or 7), 90))

    date_window = client.query(
        """
        SELECT
            max(report_date) AS data_until,
            data_until - %(days)s + 1 AS data_from
        FROM metrigo.fact_fin_report
        WHERE seller_id = %(seller_id)s
        """,
        {
            "seller_id": seller_id,
            "days": days,
        },
    ).result_rows[0]

    data_until = date_window[0]
    data_from = date_window[1]

    if data_until is None:
        return {
            "seller_id": seller_id,
            "days": days,
            "error": "Нет данных финансового отчета WB",
        }

    sales = client.query(
        """
        SELECT
            round(sumIf(seller_price, op = 'S'), 2) AS revenue_tax_base,
            countIf(op = 'S') AS buyouts_count
        FROM metrigo.fact_sales
        WHERE seller_id = %(seller_id)s
          AND sale_date >= %(data_from)s
          AND sale_date <= %(data_until)s
        """,
        {
            "seller_id": seller_id,
            "data_from": data_from,
            "data_until": data_until,
        },
    ).result_rows[0]

    revenue_tax_base = float(sales[0] or 0)
    buyouts_count = int(sales[1] or 0)

    fin = client.query(
        """
        SELECT
            round(sum(ppvz_for_pay), 2) AS wb_for_pay,
            round(sum(delivery_rub), 2) AS delivery,
            round(sum(storage_fee), 2) AS storage,
            round(sum(deduction), 2) AS deduction,
            round(sum(acceptance), 2) AS acceptance,
            round(sum(penalty), 2) AS penalty,
            round(sum(additional_payment), 2) AS additional_payment,
            round(sum(rebill_logistic_cost), 2) AS rebill_logistic,
            round(sum(acquiring_fee), 2) AS acquiring,
            count() AS rows
        FROM metrigo.fact_fin_report
        WHERE seller_id = %(seller_id)s
          AND report_date >= %(data_from)s
          AND report_date <= %(data_until)s
        """,
        {
            "seller_id": seller_id,
            "data_from": data_from,
            "data_until": data_until,
        },
    ).result_rows[0]

    wb_for_pay = float(fin[0] or 0)

    ads = client.query(
        """
        SELECT round(sum(spend), 2) AS ads_spend
        FROM metrigo.fact_ads_stats_daily
        WHERE seller_id = %(seller_id)s
          AND stat_date >= %(data_from)s
          AND stat_date <= %(data_until)s
        """,
        {
            "seller_id": seller_id,
            "data_from": data_from,
            "data_until": data_until,
        },
    ).result_rows[0]

    ads_spend = float(ads[0] or 0)

    cogs = client.query(
        """
        SELECT
            round(sum(cost_per_unit), 2) AS cogs,
            countIf(cost_per_unit = 0) AS missing_cogs_rows
        FROM (
            SELECT
                s.dedup_key,
                argMaxIf(
                    c.cost_per_unit,
                    c.valid_from,
                    c.is_active = 1
                    AND c.valid_from <= s.sale_date
                    AND (c.valid_to IS NULL OR c.valid_to >= s.sale_date)
                ) AS cost_per_unit
            FROM metrigo.fact_sales AS s
            LEFT JOIN metrigo.dim_cogs AS c
                ON c.seller_id = s.seller_id
               AND c.seller_art = s.seller_art
               AND c.barcode = s.barcode
            WHERE s.seller_id = %(seller_id)s
              AND s.op = 'S'
              AND s.sale_date >= %(data_from)s
              AND s.sale_date <= %(data_until)s
            GROUP BY s.dedup_key
        )
        """,
        {
            "seller_id": seller_id,
            "data_from": data_from,
            "data_until": data_until,
        },
    ).result_rows[0]

    cogs_total = float(cogs[0] or 0)
    missing_cogs_rows = int(cogs[1] or 0)

    tax = round(revenue_tax_base * TAX_RATE, 2)
    net_profit = round(wb_for_pay - ads_spend - tax - cogs_total, 2)

    return {
        "seller_id": seller_id,
        "days_requested": days,
        "data_from": str(data_from),
        "data_until": str(data_until),
        "buyouts_count": buyouts_count,
        "revenue_tax_base": revenue_tax_base,
        "wb_for_pay": wb_for_pay,
        "ads_spend": ads_spend,
        "tax_rate": TAX_RATE,
        "tax": tax,
        "cogs": cogs_total,
        "net_profit": net_profit,
        "fin_report_rows": int(fin[9] or 0),
        "missing_cogs_rows": missing_cogs_rows,
        "note": (
            "Расчет ограничен последней датой финансового отчета WB. "
            "COGS выбирается по seller_art/barcode и периоду действия valid_from/valid_to."
        ),
        "components": {
            "delivery": float(fin[1] or 0),
            "storage": float(fin[2] or 0),
            "deduction": float(fin[3] or 0),
            "acceptance": float(fin[4] or 0),
            "penalty": float(fin[5] or 0),
            "additional_payment": float(fin[6] or 0),
            "rebill_logistic": float(fin[7] or 0),
            "acquiring": float(fin[8] or 0),
        },
    }