FROM python:3.13-slim
WORKDIR /app

# Add this line; you can pass --build-arg CACHE_BUST=$(date +%s) to force re-run of pip
ARG CACHE_BUST=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_to_postgres.py /app/kafka_to_postgres.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/kafka_to_postgres.py"]
