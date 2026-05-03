"""
Entry point for Metrigo worker.

Что делает файл:
- принимает имя job из командной строки;
- запускает нужный загрузчик данных;
- пока поддерживает sales;
- позже сюда добавим stocks, orders, incomes, fin_report и другие фоновые задачи.

Пример запуска:
python main.py sales
"""

import sys


def main():
    """
    Parse command line arguments and run selected worker job.
    """
    if len(sys.argv) < 2:
        print("Usage: python main.py sales")
        raise SystemExit(2)

    cmd = sys.argv[1].strip().lower()

    if cmd == "sales":
        from jobs.sales import run

        run()
        return

    raise SystemExit(f"Unknown job: {cmd}")


if __name__ == "__main__":
    main()