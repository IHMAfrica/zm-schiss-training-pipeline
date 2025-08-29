FROM flink:1.19.3-scala_2.12

# PyFlink deps
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip install --no-cache-dir apache-flink==1.19.3

# Kafka + JSON connectors compatible with Flink 1.19
RUN curl -fSL "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar" \
      -o /opt/flink/lib/flink-sql-connector-kafka.jar && \
    curl -fSL "https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.19.3/flink-json-1.19.3.jar" \
      -o /opt/flink/lib/flink-json.jar

# Job location
COPY flink_job.py /opt/jobs/flink_job.py
