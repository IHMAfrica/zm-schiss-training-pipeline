import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    raw_topic = os.getenv("RAW_TOPIC", "zabbix_raw")
    curated_topic = os.getenv("CURATED_TOPIC", "zabbix_curated")
    broker = os.getenv("KAFKA_BROKER", "zbx-kafka:9092")

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Source: Kafka JSON (raw)
    t_env.execute_sql(f"""
        CREATE TABLE `t_raw` (
            -- schema relaxed: pass-through JSON
            `value` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{raw_topic}',
            'properties.bootstrap.servers' = '{broker}',
            'properties.group.id' = 'flink-zbx-raw-reader',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'json',
            'value.json.ignore-parse-errors' = 'true'
        )
    """)

    # Sink: Kafka JSON (curated)
    t_env.execute_sql(f"""
        CREATE TABLE `t_curated` (
            `value` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{curated_topic}',
            'properties.bootstrap.servers' = '{broker}',
            'value.format' = 'json',
            'sink.delivery-guarantee' = 'at-least-once'
        )
    """)

    # Simple pass-through (you can add transforms later)
    t_env.execute_sql("INSERT INTO `t_curated` SELECT `value` FROM `t_raw`").wait()

if __name__ == "__main__":
    main()
