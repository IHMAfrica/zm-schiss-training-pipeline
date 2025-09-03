import os, json, time, logging
from dotenv import load_dotenv
import mysql.connector
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def get_env(name: str, default: str | None = None, required: bool = False) -> str:
    file_var = os.getenv(f"{name}_FILE")
    if file_var:
        with open(file_var, "r", encoding="utf-8") as f:
            val = f.read().strip()
            if val:
                return val
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def truthy(v: str | None) -> bool:
    return str(v).lower() in {"1", "true", "t", "yes", "y", "on"}

# ---------- MySQL ----------
MYSQL_CFG = {
    "host": get_env("MYSQL_HOST", "host.docker.internal"),
    "port": int(get_env("MYSQL_PORT", "3306")),
    "database": get_env("MYSQL_DB", required=True),
    "user": get_env("MYSQL_USER", required=True),
    "password": get_env("MYSQL_PASSWORD", default=""),
}

QUERY_FILE = os.getenv("MYSQL_QUERY_FILE", "/app/query.sql")
QUERY = os.getenv("MYSQL_QUERY")  # optional override
PAGE_SIZE = int(os.getenv("MYSQL_PAGE_SIZE", "5000"))  
SLEEP_BETWEEN_PAGES = float(os.getenv("MYSQL_PAGE_DELAY_SEC", "0"))  

# ---------- Kafka ----------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "training_raw")
KAFKA_AUTH_ENABLED = truthy(os.getenv("KAFKA_AUTH_ENABLED", "0"))

def build_kafka_client_config() -> dict:
    cfg = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": os.getenv("KAFKA_CLIENT_ID", "schiss-producer"),
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 25,
        "batch.num.messages": 10000,
    }
    if KAFKA_AUTH_ENABLED:
        sec = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
        mech = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
        user = os.getenv("KAFKA_SASL_USERNAME")
        pwd  = os.getenv("KAFKA_SASL_PASSWORD")
        cfg["security.protocol"] = sec
        cfg["sasl.mechanisms"] = mech
        if user and pwd:
            cfg["sasl.username"] = user
            cfg["sasl.password"] = pwd
        logging.info("Kafka auth enabled: %s, protocol=%s, mechanism=%s", KAFKA_AUTH_ENABLED, sec, mech)
    else:
        logging.info("Kafka auth enabled: %s (using PLAINTEXT)", KAFKA_AUTH_ENABLED)
    return cfg

def ensure_topic(topic: str, partitions: int = 1, rf: int = 1):
    admin = AdminClient(build_kafka_client_config())
    md = admin.list_topics(timeout=10)
    if topic in md.topics and md.topics[topic].error is None:
        logging.info("Kafka topic '%s' already exists.", topic)
        return
    logging.info("Creating Kafka topic '%s'...", topic)
    fs = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=rf)])
    try:
        fs[topic].result(15)
        logging.info("Kafka topic '%s' created.", topic)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" in str(e):
            logging.info("Kafka topic '%s' exists (raced).", topic)
        else:
            raise

def load_query() -> str:
    if QUERY and QUERY.strip():
        return QUERY
    with open(QUERY_FILE, "r", encoding="utf-8") as f:
        return f.read()

def stream_rows_to_kafka(cursor, producer):
    rows = 0
    def _delivered(err, msg):
        if err:
            logging.error("Delivery failed: %s", err)
    for row in cursor:
        producer.poll(0)
        producer.produce(RAW_TOPIC, json.dumps(row, default=str).encode("utf-8"), callback=_delivered)
        rows += 1
        if rows % 1000 == 0:
            logging.info("Queued %d messages...", rows)
    producer.flush(60)
    return rows

def main():
    ensure_topic(RAW_TOPIC, partitions=1, rf=1)

    logging.info("Connecting to MySQL at %s:%s / db=%s",
                 MYSQL_CFG["host"], MYSQL_CFG["port"], MYSQL_CFG["database"])
    conn = mysql.connector.connect(**MYSQL_CFG)
    cur = conn.cursor(dictionary=True)

    sql = load_query()
    has_placeholders = ("%(limit)s" in sql) and ("%(offset)s" in sql)

    prod = Producer(build_kafka_client_config())

    logging.info("Running query and streaming to Kafka topic '%s'...", RAW_TOPIC)

    total = 0
    if has_placeholders:
        logging.info("Detected LIMIT/OFFSET placeholders -> paginated export (page size: %d)", PAGE_SIZE)
        offset = 0
        page = 1
        while True:
            params = {"limit": PAGE_SIZE, "offset": offset}
            cur.execute(sql, params)  
            sent = stream_rows_to_kafka(cur, prod)
            total += sent
            logging.info("Page %d: sent %d rows (total %d)", page, sent, total)
            if sent < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            page += 1
            if SLEEP_BETWEEN_PAGES > 0:
                time.sleep(SLEEP_BETWEEN_PAGES)
    else:
        cur.execute(sql)
        total = stream_rows_to_kafka(cur, prod)

    logging.info("Finished. Sent %d messages.", total)
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Producer stopped by user.")
    except Exception as e:
        logging.exception("Producer error: %s", e)
