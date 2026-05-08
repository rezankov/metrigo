"""
Metrigo worker entrypoint.

Что делает файл:
- принимает имя задачи из командной строки;
- запускает нужный WB collector;
- не содержит бизнес-логики загрузки данных.

Пример:
    python main.py sales
    python main.py orders
    python main.py stocks
    python main.py fin_report

Почему так:
- main.py остаётся простым роутером;
- вся логика конкретного сборщика живёт в jobs/<source>.py;
- все сборщики используют общий контракт из jobs/common.py.
"""

import sys


AVAILABLE_JOBS = {
    "sales": "jobs.sales",
    "orders": "jobs.orders",
    "stocks": "jobs.stocks",
    "supplies": "jobs.supplies",
		"prices": "jobs.prices",
    "content_cards": "jobs.content_cards",
    "tariffs": "jobs.tariffs",
    "ads_campaigns": "jobs.ads_campaigns",
    "ads_stats_daily": "jobs.ads_stats_daily",
    "fin_report": "jobs.fin_report",
}


def main() -> None:
    """
    Read job name from CLI and run selected collector.
    """
    if len(sys.argv) < 2:
        available = "|".join(AVAILABLE_JOBS.keys())
        print(f"Usage: python main.py {available}")
        raise SystemExit(2)

    job_name = sys.argv[1].strip().lower()

    if job_name not in AVAILABLE_JOBS:
        available = ", ".join(AVAILABLE_JOBS.keys())
        raise SystemExit(f"Unknown job: {job_name}. Available jobs: {available}")

    module_name = AVAILABLE_JOBS[job_name]

    module = __import__(module_name, fromlist=["run"])
    module.run()


if __name__ == "__main__":
    main()