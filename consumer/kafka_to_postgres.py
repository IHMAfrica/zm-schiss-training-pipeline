import os, json, logging, time, pathlib
from typing import List, Dict, Any

from dotenv import load_dotenv
from confluent_kafka import Consumer as CKConsumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

import psycopg
from psycopg import sql
from psycopg.extras import execute_values

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- secrets helper -----------------------------------------------------------
def read_secret(path_env: str) -> str:
    """Read a secret file path from env (e.g., PG_PASSWORD_FILE) and return its contents."""
    p = os.getenv(path_env)
    if not p or not pathlib.Path(p).exists():
        raise RuntimeError(f"Secret file not found for {path_env}: {p}")
    return pathlib.Path(p).read_text(encoding="utf-8").strip()

# --- kafka topic ensure -------------------------------------------------------
def ensure_topic(broker: str, topic: str, partitions: int = 1, rf: int = 1):
    admin = AdminClient({"bootstrap.servers": broker})
    md = admin.list_topics(timeout=5)
    if topic in md.topics:
        logging.info("Kafka topic '%s' already exists.", topic)
        return
    logging.info("Creating Kafka topic '%s'...", topic)
    fs = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=rf)])
    for _, f in fs.items():
        f.result()  # raises on failure
    logging.info("Kafka topic '%s' created.", topic)

# --- postgres -----------------------------------------------------------------
def pg_conn():
    pw = read_secret("PG_PASSWORD_FILE")
    return psycopg.connect(
        host=os.getenv("PG_HOST", "host.docker.internal"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "site_ops"),
        user=os.getenv("PG_USER", "postgres"),
        password=pw,
        autocommit=False,
    )

def init_schema(cur, table: str):
    q = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            hostid BIGINT,
            host_name TEXT,
            eventid BIGINT,
            problem_start TIMESTAMP NULL,
            problem_end TIMESTAMP NULL,
            problem_duration_seconds INT,
            problem_status TEXT,
            trigger_description TEXT,
            severity INT,
            total_downtime_events INT,
            uptime_percentage NUMERIC(10,2),
            interfaceid BIGINT,
            province TEXT,
            port TEXT,
            available INT,
            error TEXT,
            ip TEXT,
            problemstatus TEXT,
            availabilitystatus TEXT,
            avg_signal_strength DOUBLE PRECISION,
            signal_strength TEXT,
            PRIMARY KEY (eventid, interfaceid)
        );
    """).format(sql.Identifier(table))
    cur.execute(q)

def upsert_batch(cur, table: str, rows: List[Dict[str, Any]]):
    if not rows:
        return

    cols = [
        "hostid","host_name","eventid","problem_start","problem_end","problem_duration_seconds","problem_status",
        "trigger_description","severity","total_downtime_events","uptime_percentage","interfaceid","province","port",
        "available","error","ip","problemstatus","availabilitystatus","avg_signal_strength","signal_strength"
    ]

    insert = sql.SQL("""
        INSERT INTO {} ({cols})
        VALUES %s
        ON CONFLICT (eventid, interfaceid) DO UPDATE SET
        {updates}
    """).format(
        sql.Identifier(table),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        updates=sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in cols if c not in ("eventid", "interfaceid")
        )
    )

    values = [tuple(r.get(c) for c in cols) for r in rows]
    # psycopg3's execute_values will expand VALUES %s efficiently
    execute_values(cur, insert, values, page_size=500)

# --- main ---------------------------------------------------------------------
def main():
    broker = os.getenv("KAFKA_BROKER", "zbx-kafka:9092")
    topic = os.getenv("CURATED_TOPIC", "zabbix_curated")
    group = os.getenv("CONSUMER_GROUP", "zbx_sitevisibility_consumer")
    table = os.getenv("PG_TABLE", "site_visibility")
    batch_size = int(os.getenv("UPSERT_BATCH_SIZE", "1000"))
    init_schema_flag = os.getenv("INIT_SCHEMA", "1") == "1"

    # Ensure (or noop) topic
    try:
        ensure_topic(broker, topic)
    except Exception as e:
        logging.warning("Topic ensure failed (continuing): %s", e)

    # Postgres
    logging.info("Connecting to Postgres...")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if init_schema_flag:
                init_schema(cur, table)
                conn.commit()

        # Kafka
        logging.info("Starting Kafka consumer on topic %s", topic)
        consumer = CKConsumer({
            "bootstrap.servers": broker,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 900_000,
        })
        consumer.subscribe([topic])

        buf: List[Dict[str, Any]] = []
        last_flush = time.time()

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    # flush on idle
                    if buf:
                        with conn.cursor() as cur:
                            upsert_batch(cur, table, buf)
                        conn.commit()
                        consumer.commit()
                        buf.clear()
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logging.error("Consumer error: %s", msg.error())
                        raise KafkaException(msg.error())
                    continue

                try:
                    payload = json.loads(msg.value())
                    # If Flink wrapped payloads like {"value": {...}}, unwrap them
                    if isinstance(payload, dict) and "value" in payload and isinstance(payload["value"], dict):
                        payload = payload["value"]
                    if isinstance(payload, dict):
                        buf.append(payload)
                    else:
                        logging.warning("Skipping non-dict JSON: %r", payload)
                except Exception as e:
                    logging.warning("Bad JSON skipped: %s", e)

                if len(buf) >= batch_size or (time.time() - last_flush) > 10:
                    with conn.cursor() as cur:
                        upsert_batch(cur, table, buf)
                    conn.commit()
                    consumer.commit()
                    buf.clear()
                    last_flush = time.time()

        except KeyboardInterrupt:
            pass
        finally:
            if buf:
                with conn.cursor() as cur:
                    upsert_batch(cur, table, buf)
                conn.commit()
                consumer.commit()
            consumer.close()

if __name__ == "__main__":
    main()
