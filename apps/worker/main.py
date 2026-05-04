"""
Entry point для worker.

Назначение:
- Запуск ETL джобов через аргумент командной строки
- Поддержка разных источников (sales, orders, stocks и т.д.)

Пример:
    python main.py sales
    python main.py orders
"""

import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py sales|orders|stocks|incomes|fin_report")
        raise SystemExit(2)

    cmd = sys.argv[1].strip().lower()

    if cmd == "sales":
        from jobs.sales import run
        run()
        return

    if cmd == "orders":
        from jobs.orders import run
        run()
        return

    if cmd == "stocks":
        from jobs.stocks import run
        run()
        return

    if cmd == "incomes":
        from jobs.incomes import run
        run()
        return

    if cmd == "fin_report":
        from jobs.fin_report import run
        run()
        return

    raise SystemExit(f"Unknown job: {cmd}")


if __name__ == "__main__":
    main()