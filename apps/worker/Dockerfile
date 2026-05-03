# Worker image for Metrigo background jobs.
#
# This container runs one selected job, for example:
#   python main.py sales
#
# The same image will later be reused for orders, stocks,
# incomes, finance reports, and scheduled ETL tasks.

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY jobs ./jobs

CMD ["python", "main.py"]