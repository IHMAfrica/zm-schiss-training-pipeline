import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def truthy(v: str | None) -> bool:
    return str(v).lower() in {"1", "true", "t", "yes", "y", "on"}

RAW_TOPIC = os.getenv("RAW_TOPIC", "training_raw")
CURATED_TOPIC = os.getenv("CURATED_TOPIC", "training_curated")
BROKERS = os.getenv("KAFKA_BROKER", "kafka:9092")
AUTH_ENABLED = truthy(os.getenv("KAFKA_AUTH_ENABLED", "0"))

SEC  = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
MECH = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
USER = os.getenv("KAFKA_SASL_USERNAME")
PASS = os.getenv("KAFKA_SASL_PASSWORD")
JAAS = os.getenv("KAFKA_SASL_JAAS_CONFIG")  # optional

def extra_kafka_props_sql() -> str:
    if not AUTH_ENABLED:
        return ""
    lines = [f"'properties.security.protocol' = '{SEC}'",
             f"'properties.sasl.mechanism' = '{MECH}'"]
    jaas = JAAS
    if not jaas and USER and PASS:
        jaas = f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{USER}" password="{PASS}";'
    if jaas:
        lines.append(f"'properties.sasl.jaas.config' = '{jaas}'")
    return (",\n      " + ",\n      ".join(lines))

def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.name", "schiss-training-pipeline")
    t_env.get_config().set("parallelism.default", "1")

    extra = extra_kafka_props_sql()

    t_env.execute_sql(f"""
    CREATE TABLE `t_raw` (
        staffid BIGINT,
        fullname STRING,
        reg_date STRING,
        decision STRING,
        training_type STRING,
        staff_type STRING,
        nrc STRING,
        sex STRING,
        age STRING,
        job_role STRING,
        education STRING,
        used_smartcare STRING,
        currently_using STRING,
        stakeholder_name STRING,
        facility_name STRING,
        province_name STRING,
        district_name STRING,
        financialyear STRING,
        quarters STRING,
        isocode STRING,
        pretestdate STRING,
        pretestscore INT,
        posttestdate STRING,
        posttestscore INT,
        mgr_pretestdate STRING,
        mgr_pretestscore INT,
        mgr_posttestdate STRING,
        mgr_posttestscore INT
    ) WITH (
      'connector' = 'kafka',
      'topic' = '{RAW_TOPIC}',
      'properties.bootstrap.servers' = '{BROKERS}',
      'properties.group.id' = 'flink-raw-reader',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'json',
      'value.json.ignore-parse-errors' = 'true',
      'value.json.fail-on-missing-field' = 'false'
      {extra}
    )
    """)

    t_env.execute_sql(f"""
    CREATE TABLE `t_curated` (
        staffid BIGINT,
        fullname STRING,
        reg_date STRING,
        decision STRING,
        training_type STRING,
        staff_type STRING,
        nrc STRING,
        sex STRING,
        age STRING,
        job_role STRING,
        education STRING,
        used_smartcare STRING,
        currently_using STRING,
        stakeholder_name STRING,
        facility_name STRING,
        province_name STRING,
        district_name STRING,
        financialyear STRING,
        quarters STRING,
        isocode STRING,
        pretestdate STRING,
        pretestscore INT,
        posttestdate STRING,
        posttestscore INT,
        mgr_pretestdate STRING,
        mgr_pretestscore INT,
        mgr_posttestdate STRING,
        mgr_posttestscore INT
    ) WITH (
      'connector' = 'kafka',
      'topic' = '{CURATED_TOPIC}',
      'properties.bootstrap.servers' = '{BROKERS}',
      'value.format' = 'json',
      'value.json.timestamp-format.standard' = 'ISO-8601'
      {extra}
    )
    """)

    res = t_env.execute_sql("INSERT INTO `t_curated` SELECT * FROM `t_raw`")
    try:
        jc = res.get_job_client()
        if jc is not None:
            print("Submitted job:", jc.get_job_id())
    except Exception:
        pass

if __name__ == "__main__":
    main()
