# flink/Dockerfile
FROM flink:1.19.3-scala_2.12

USER root
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# PyFlink pinned to 1.19.x (inside the Flink image this is fine)
RUN pip install --no-cache-dir apache-flink==1.19.3

# --- Correct connector + format jars (match Flink 1.19.x) ---
ARG KAFKA_SQL_VER=3.3.0-1.19
ARG FLINK_VER=1.19.3

# Kafka SQL connector (3.x.y-1.19 lineage)
RUN curl -fSL "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_SQL_VER}/flink-sql-connector-kafka-${KAFKA_SQL_VER}.jar" \
    -o /opt/flink/lib/flink-sql-connector-kafka.jar

# JSON format (matches Flink core version)
RUN curl -fSL "https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VER}/flink-json-${FLINK_VER}.jar" \
    -o /opt/flink/lib/flink-json.jar

# Your PyFlink job
COPY flink_job.py /opt/jobs/flink_job.py

USER flink
