import os, time, json, logging, pathlib
from dotenv import load_dotenv
import mysql.connector
from confluent_kafka import Producer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def read_secret(path_env: str) -> str:
    p = os.getenv(path_env)
    if not p or not pathlib.Path(p).exists():
        raise RuntimeError(f"Secret file not found for {path_env}: {p}")
    return pathlib.Path(p).read_text(encoding="utf-8").strip()

def get_mysql_conn():
    pw = read_secret("MYSQL_PASSWORD_FILE")
    cnx = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "host.docker.internal"),
        port=int(os.getenv("MYSQL_PORT", "7070")),
        database=os.getenv("MYSQL_DB", "zabbix"),
        user=os.getenv("MYSQL_USER", "superset"),
        password=pw,
    )
    return cnx

def get_kafka_producer():
    return Producer({
        "bootstrap.servers": os.getenv("KAFKA_BROKER", "zbx-kafka:9092"),
        "client.id": "zbx-producer"
    })

def delivery_report(err, msg):
    if err:
        logging.error(f"Delivery failed: {err}")
    # else: ok; keep quiet to avoid chatty logs

def produce_once():
    topic = os.getenv("RAW_TOPIC", "zabbix_raw")
    batch_size = int(os.getenv("BATCH_SIZE", "1000"))

    # load SQL
    with open("/app/query.sql", "r", encoding="utf-8") as f:
        sql = f.read()

    cnx = get_mysql_conn()
    cur = cnx.cursor(dictionary=True)

    logging.info("Starting extraction from MySQL...")
    cur.execute(sql)

    p = get_kafka_producer()
    sent = 0

    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            key = f"{r.get('eventid','')}-{r.get('interfaceid','')}".encode("utf-8")
            val = json.dumps(r, default=str).encode("utf-8")
            p.produce(topic=topic, key=key, value=val, callback=delivery_report)
            sent += 1
            if sent % 1000 == 0:
                logging.info(f"Produced {sent} rows so far...")
        p.flush()

    p.flush(10)
    cur.close()
    cnx.close()
    logging.info(f"Done. Total produced: {sent}")

def main():
    one_shot = os.getenv("PRODUCER_ONE_SHOT", "1") == "1"
    interval = int(os.getenv("POLL_INTERVAL_SEC", "900"))
    while True:
        try:
            produce_once()
        except Exception as e:
            logging.exception("Producer iteration failed: %s", e)
        if one_shot:
            break
        time.sleep(interval)

if __name__ == "__main__":
    main()
