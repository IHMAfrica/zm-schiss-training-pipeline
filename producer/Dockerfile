FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mysql_to_kafka.py /app/mysql_to_kafka.py
COPY query.sql /app/query.sql

# Directory to persist producer watermark across restarts
RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/mysql_to_kafka.py"]
