schiss-training-pipeline

A fully containerized stateful data streaming pipeline:

MySQL (Windows host) → Kafka topic training_raw (Producer)

Flink (PyFlink SQL) training_raw → training_curated

Kafka topic training_curated → Postgres table capacity_building (Consumer)

Everything runs with Docker Compose, Python 3.13, Flink 1.19, and Docker secrets for credentials.

Architecture
+----------------+         +----------------------+         +-------------------+
|  MySQL (host)  |  --->   | Producer (mysql→raw) |  --->   |  Kafka (raw)      |
+----------------+         +----------------------+         +-------------------+
                                                                |
                                                                |  Flink SQL job
                                                                v
                                                         +-------------------+
                                                         |  Kafka (curated)  |
                                                         +-------------------+
                                                                |
                                                                v
                                                   +----------------------------+
                                                   | Consumer (curated→Postgres)|
                                                   +----------------------------+
                                                                |
                                                                v
                                                    Postgres table: capacity_building

Tech stack

Kafka: Bitnami bitnami/kafka:3.7 (KRaft mode, single broker)

Flink: 1.19 session cluster (jobmanager, taskmanager, flink-submit)

Python: 3.13

Producer libs: mysql-connector-python, confluent-kafka, python-dotenv

Consumer libs: psycopg[binary], confluent-kafka, python-dotenv

Flink connectors: flink-sql-connector-kafka and flink-json (already baked into the image)

Repository structure
schiss-training-pipeline/
├─ docker-compose.yml
├─ .env                    
├─ secrets/                 
│  ├─ mysql_password.txt
│  └─ pg_password.txt
├─ flink/
│  └─ flink_job.py          # PyFlink SQL job (raw→curated)
├─ producer/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  ├─ mysql_to_kafka.py     # reads MySQL, writes JSON to training_raw
│  └─ query.sql             # your provided SELECT
└─ consumer/
   ├─ Dockerfile
   ├─ requirements.txt
   └─ kafka_to_postgres.py  # reads training_curated, upserts Postgres

Prerequisites

Docker Desktop (Windows)


Configuration
1) .env (safe to commit)

Create .env in the project root:

# ─── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER=kafka:9092
RAW_TOPIC=training_raw
CURATED_TOPIC=training_curated
CONSUMER_GROUP=schiss_training_consumer

# ─── MySQL (NO PASSWORD HERE – use secret file) ────────────────────────────────
MYSQL_HOST=host.docker.internal
MYSQL_PORT=3306
MYSQL_DB=schiss_training
MYSQL_USER=

# Producer behavior
POLL_INTERVAL_SEC=900
BATCH_SIZE=1000
PRODUCER_ONE_SHOT=0

# ─── Postgres (NO PASSWORD HERE – use secret file) ─────────────────────────────
PG_HOST=host.docker.internal
PG_PORT=5432
PG_DB=schiss_training
PG_USER=postgres

# Consumer behavior
UPSERT_BATCH_SIZE=1000
INIT_SCHEMA=1

2) Secrets

Create once from the project root

New-Item -ItemType Directory -Force -Path .\secrets | Out-Null
Set-Content -NoNewline -Path .\secrets\mysql_password.txt -Value "your_mysql_password_here"
Set-Content -NoNewline -Path .\secrets\pg_password.txt    -Value "your_postgres_password_here"
Add-Content .gitignore "`r`nsecrets/"


The compose file mounts these as Docker secrets and your apps read them via MYSQL_PASSWORD_FILE / PG_PASSWORD_FILE.

Run the stack

Always run from the project root (the folder with docker-compose.yml).

docker compose up -d --build


Compose will:

Start Kafka

Start Flink (jobmanager, taskmanager)

Start producer (reads MySQL → Kafka training_raw)

Start consumer (reads training_curated → Postgres)

Start flink-submit (auto-submits the Flink job)

Submit/check the Flink job

The job is auto-submitted by the flink-submit service. You can also trigger manually:

# Manual submit (if needed)
docker compose exec jobmanager /opt/flink/bin/flink run -py /opt/jobs/flink_job.py

# Check job status in the Flink UI:
# http://localhost:8081
# Or via API:
curl http://localhost:8081/jobs/overview


If jobs: [] keep showing, check docker compose logs -f flink-submit.

Verify end-to-end
1) Raw topic has data
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 `
  --topic training_raw --from-beginning --max-messages 3

2) Curated topic has data
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 `
  --topic training_curated --from-beginning --max-messages 3

3) Postgres receiving rows

Connect with your preferred client and run:

SELECT COUNT(*) FROM capacity_building;
SELECT staffid, fullname, financialyear, quarters, isocode
FROM capacity_building
LIMIT 10;


Or tail consumer logs:

docker compose logs -f consumer

Operating the pipeline
Rebuild only one service
# Rebuild + restart consumer only
docker compose build --no-cache consumer
docker compose up -d consumer

Update and re-submit the Flink job
# Copy a modified job into the running JM
docker cp .\flink\flink_job.py jobmanager:/opt/jobs/flink_job.py

# Submit
docker compose exec jobmanager /opt/flink/bin/flink run -py /opt/jobs/flink_job.py

Reset the consumer offsets (backfill)
# Stop consumer, then reset offsets for its group
docker compose stop consumer

# View groups
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Reset to earliest for the configured group
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 `
  --group schiss_training_consumer --topic training_curated --reset-offsets --to-earliest --execute

# Start consumer again
docker compose start consumer

Temporarily bypass Flink (for quick validation)

Set in .env:

CURATED_TOPIC=training_raw


Then:

docker compose up -d --build consumer


Confirm rows land in Postgres. Revert to training_curated afterwards.

Troubleshooting

Kafka healthy but consumer says “unknown topic training_curated”
Ensure the Flink job is running; it creates/produces to training_curated. The consumer now also auto-creates the topic if missing, but no data will arrive until Flink runs.

Flink SQL: Unsupported options json.ignore-parse-errors
Fixed by using value.json.ignore-parse-errors (already in flink_job.py).

No jobs showing in Flink UI
Check flink-submit logs:

docker compose logs -f flink-submit


Submit manually (see above).

No rows in Postgres

Verify training_curated has messages (see verify steps).

Check consumer logs for DB connection errors.

Confirm secrets are correct (secrets/*.txt) and Postgres is reachable at PG_HOST:PG_PORT.

Windows paths
Run compose commands from the project directory. If running elsewhere, pass -f <fullpath>\docker-compose.yml.

Security notes

Passwords live in ./secrets/*.txt (mounted as Docker secrets).

.env contains no secrets; safe to commit.

Rotate passwords by updating the secret files and recreating services:

Set-Content -NoNewline -Path .\secrets\pg_password.txt -Value "new_password"
docker compose up -d --build consumer

Performance & hardening tips

Use at-least-once delivery on the Flink Kafka sink (already reliable for single broker):

'sink.delivery-guarantee' = 'at-least-once'


Increase consumer UPSERT_BATCH_SIZE to 5000 if Postgres is comfortable.

Add indexes in Postgres:

CREATE INDEX IF NOT EXISTS idx_capacity_building_fy ON capacity_building(financialyear);
CREATE INDEX IF NOT EXISTS idx_capacity_building_isocode ON capacity_building(isocode);
CREATE INDEX IF NOT EXISTS idx_capacity_building_reg_date ON capacity_building(reg_date);

Clean up
# Stop and remove containers + network
docker compose down

# Also remove anonymous volumes (if any)
docker compose down -v

Credits / Notes

Pipeline name: schiss-training-pipeline

Destination table: capacity_building (schema created automatically by the consumer)

Flink job reads JSON from training_raw and republishes to training_curated as JSON.