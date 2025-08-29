# Slim + wheels only; no compiler toolchain needed
FROM python:3.13-slim

WORKDIR /app

# Keep it tiny but include certs for TLS brokers if needed
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_to_postgres.py /app/kafka_to_postgres.py

# Default: run the consumer
CMD ["python", "/app/kafka_to_postgres.py"]
