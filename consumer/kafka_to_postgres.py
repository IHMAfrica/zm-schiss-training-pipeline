import os, json, time, logging, psycopg
from dotenv import load_dotenv
from confluent_kafka import Consumer as CKConsumer, KafkaError
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

KAFKA_AUTH_ENABLED = truthy(os.getenv("KAFKA_AUTH_ENABLED", "0"))

def build_kafka_client_config(group_id: str | None = None) -> dict:
    cfg = {
        "bootstrap.servers": os.getenv("KAFKA_BROKER", "kafka:9092"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    if group_id:
        cfg["group.id"] = group_id

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

CURATED_TOPIC  = os.getenv("CURATED_TOPIC", "training_curated")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "schiss_training_consumer")

PG_DSN = os.getenv("PG_DSN") or os.getenv("DATABASE_URL")
if not PG_DSN:
    PG_HOST = get_env("PG_HOST", "host.docker.internal")
    PG_PORT = get_env("PG_PORT", "5432")
    PG_DB   = get_env("PG_DB",   required=True)
    PG_USER = get_env("PG_USER", required=True)
    PG_PASS = get_env("PG_PASSWORD", required=True)
    PG_DSN = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"

UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "100"))
INIT_SCHEMA = os.getenv("INIT_SCHEMA", "1") == "1"

UPSERT_SQL = """
INSERT INTO capacity_building (
    staffid, fullname, reg_date, decision, training_type, staff_type, nrc, sex, age, job_role, education,
    used_smartcare, currently_using, stakeholder_name, facility_name, province_name, district_name,
    financialyear, quarters, isocode,
    pretestdate, pretestscore, posttestdate, posttestscore,
    mgr_pretestdate, mgr_pretestscore, mgr_posttestdate, mgr_posttestscore, updated_at
) VALUES (
    %(staffid)s, %(fullname)s, %(reg_date)s, %(decision)s, %(training_type)s, %(staff_type)s, %(nrc)s, %(sex)s, %(age)s, %(job_role)s, %(education)s,
    %(used_smartcare)s, %(currently_using)s, %(stakeholder_name)s, %(facility_name)s, %(province_name)s, %(district_name)s,
    %(financialyear)s, %(quarters)s, %(isocode)s,
    %(pretestdate)s, %(pretestscore)s, %(posttestdate)s, %(posttestscore)s,
    %(mgr_pretestdate)s, %(mgr_pretestscore)s, %(mgr_posttestdate)s, %(mgr_posttestscore)s, NOW()
)
ON CONFLICT (staffid) DO UPDATE SET
    fullname = EXCLUDED.fullname,
    reg_date = EXCLUDED.reg_date,
    decision = EXCLUDED.decision,
    training_type = EXCLUDED.training_type,
    staff_type = EXCLUDED.staff_type,
    nrc = EXCLUDED.nrc,
    sex = EXCLUDED.sex,
    age = EXCLUDED.age,
    job_role = EXCLUDED.job_role,
    education = EXCLUDED.education,
    used_smartcare = EXCLUDED.used_smartcare,
    currently_using = EXCLUDED.currently_using,
    stakeholder_name = EXCLUDED.stakeholder_name,
    facility_name = EXCLUDED.facility_name,
    province_name = EXCLUDED.province_name,
    district_name = EXCLUDED.district_name,
    financialyear = EXCLUDED.financialyear,
    quarters = EXCLUDED.quarters,
    isocode = EXCLUDED.isocode,
    pretestdate = EXCLUDED.pretestdate,
    pretestscore = EXCLUDED.pretestscore,
    posttestdate = EXCLUDED.posttestdate,
    posttestscore = EXCLUDED.posttestscore,
    mgr_pretestdate = EXCLUDED.mgr_pretestdate,
    mgr_pretestscore = EXCLUDED.mgr_pretestscore,
    mgr_posttestdate = EXCLUDED.mgr_posttestdate,
    mgr_posttestscore = EXCLUDED.mgr_posttestscore,
    updated_at = NOW();
"""

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS capacity_building (
            staffid               BIGINT PRIMARY KEY,
            fullname              VARCHAR(255),
            reg_date              TIMESTAMP NULL,
            decision              VARCHAR(64),
            training_type         VARCHAR(128),
            staff_type            VARCHAR(128),
            nrc                   VARCHAR(64),
            sex                   VARCHAR(16),
            age                   VARCHAR(64),
            job_role              VARCHAR(128),
            education             VARCHAR(128),
            used_smartcare        VARCHAR(32),
            currently_using       VARCHAR(32),
            stakeholder_name      VARCHAR(255),
            facility_name         VARCHAR(255),
            province_name         VARCHAR(128),
            district_name         VARCHAR(128),
            financialyear         VARCHAR(8),
            quarters              VARCHAR(4),
            isocode               VARCHAR(8),
            pretestdate           TIMESTAMP NULL,
            pretestscore          INT,
            posttestdate          TIMESTAMP NULL,
            posttestscore         INT,
            mgr_pretestdate       TIMESTAMP NULL,
            mgr_pretestscore      INT,
            mgr_posttestdate      TIMESTAMP NULL,
            mgr_posttestscore     INT,
            inserted_at           TIMESTAMP DEFAULT NOW(),
            updated_at            TIMESTAMP DEFAULT NOW()
        );
        """)

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

def to_record(msg_value: dict):
    keys = [
        "staffid","fullname","reg_date","decision","training_type","staff_type","nrc","sex","age","job_role","education",
        "used_smartcare","currently_using","stakeholder_name","facility_name","province_name","district_name",
        "financialyear","quarters","isocode",
        "pretestdate","pretestscore","posttestdate","posttestscore",
        "mgr_pretestdate","mgr_pretestscore","mgr_posttestdate","mgr_posttestscore"
    ]
    return {k: msg_value.get(k) for k in keys}

def main():
    logging.info("Connecting to Postgres...")
    with psycopg.connect(PG_DSN, autocommit=False) as conn:
        if INIT_SCHEMA:
            ensure_schema(conn)
            conn.commit()

        ensure_topic(CURATED_TOPIC, partitions=1, rf=1)

        logging.info("Starting Kafka consumer on topic %s", CURATED_TOPIC)
        consumer = CKConsumer(build_kafka_client_config(group_id=CONSUMER_GROUP))
        consumer.subscribe([CURATED_TOPIC])

        batch = []
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    code = msg.error().code()
                    if code in (KafkaError.UNKNOWN_TOPIC_OR_PARTITION, KafkaError._ALL_BROKERS_DOWN, KafkaError._TRANSPORT):
                        logging.warning("Kafka not ready (%s). Retrying...", msg.error())
                        time.sleep(2)
                        continue
                    logging.error("Kafka consumer error: %s", msg.error())
                    continue

                value = json.loads(msg.value().decode("utf-8"))
                batch.append(to_record(value))

                if len(batch) >= UPSERT_BATCH_SIZE:
                    with conn.cursor() as cur:
                        cur.executemany(UPSERT_SQL, batch)
                    conn.commit()
                    consumer.commit()
                    logging.info("Upserted %d rows", len(batch))
                    batch = []
        finally:
            if batch:
                with conn.cursor() as cur:
                    cur.executemany(UPSERT_SQL, batch)
                conn.commit()
                consumer.commit()
                logging.info("Upserted %d rows (final)", len(batch))
            consumer.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.exception("Consumer error: %s", e)
