FROM python:3.12-slim

WORKDIR /app

# TLS certs only (small image; no compiler toolchain)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mysql_to_kafka.py /app/mysql_to_kafka.py

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/mysql_to_kafka.py"]
