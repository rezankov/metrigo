"""
Инструмент: get_sales_chart_insight
Возвращает текстовый вывод по графику продаж за 14 дней:
- сравнение с предыдущим днем
- сравнение со средним
- выявление пиков
"""

from app.db import ch

def get_sales_chart_insight() -> str:
    """
    Проанализировать график продаж за последние 14 дней.
    Возвращает:
    - сравнение с предыдущим днем
    - сравнение со средним уровнем
    - наличие пиков продаж
    Используется для:
    - AI-комментариев к графику
    - ежедневных инсайтов
    - кратких аналитических выводов
    """

    client = ch()
    rows = client.query(
        """
        SELECT sale_date, round(sumIf(seller_price, op='S'),2) AS revenue
        FROM metrigo.fact_sales
        WHERE seller_id='main'
          AND sale_date >= toDate(now('Europe/Moscow'))-13
        GROUP BY sale_date
        ORDER BY sale_date
        """,
    ).result_rows

    if len(rows)<2:
        return "Пока мало данных для анализа динамики."

    values = [float(r[1] or 0) for r in rows]
    last_value = values[-1]
    prev_value = values[-2]
    avg_value = sum(values)/len(values)
    max_value = max(values)

    delta_prev = ((last_value-prev_value)/prev_value*100) if prev_value>0 else 0
    delta_avg = ((last_value-avg_value)/avg_value*100) if avg_value>0 else 0

    level_text = f"Сегодня выручка {'выше' if last_value>=avg_value else 'ниже'} среднего за 14 дней на {abs(delta_avg):.1f}%."
    trend_text = f"Рост к предыдущему дню {delta_prev:.1f}%." if abs(delta_prev)>10 else "Динамика примерно стабильная."
    peak_text = "Выраженный пик на графике." if max_value>avg_value*1.6 else "Пиков нет."

    return f"**Короткий вывод:**\n\n{level_text}\n{trend_text}\n{peak_text}"