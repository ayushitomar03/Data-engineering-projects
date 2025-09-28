import json
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
from datetime import datetime, timezone

KAFKA_BROKER = "kafka:29092"
TOPIC = "wikimedia-recentchange"

DB_CONFIG = {
    "dbname": "sensor_db",
    "user": "user",
    "password": "password",
    "host": "postgres",
    "port": "5432"
}

def create_parent_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wikimedia_events (
            id BIGINT,
            type TEXT,
            title TEXT,
            "user" TEXT,
            bot BOOLEAN,
            minor BOOLEAN,
            timestamp TIMESTAMPTZ,
            server_name TEXT,
            comment TEXT,
            PRIMARY KEY (id, timestamp)
        ) PARTITION BY RANGE (timestamp);
    """)
    conn.commit()
    cur.close()
    conn.close()

def ensure_month_partition(ts: datetime):
    """Create a monthly partition if it doesn't exist."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    partition_name = f"wikimedia_events_{ts.year}_{ts.month:02d}"
    start_date = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if ts.month == 12:
        end_date = start_date.replace(year=ts.year + 1, month=1)
    else:
        end_date = start_date.replace(month=ts.month + 1)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF wikimedia_events
        FOR VALUES FROM (%s) TO (%s);
    """, (start_date, end_date))

    # Optional index on timestamp inside this partition
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{partition_name}_timestamp
        ON {partition_name} (timestamp);
    """)

    conn.commit()
    cur.close()
    conn.close()

def insert_data(item):

    ts_val = item.get("timestamp")
    event_id = item.get("id")

    if ts_val is None:
        print(f"Skipping event with no timestamp: {item}", flush=True)
        return

    if event_id is None:
        print(f"Skipping event with no ID: {item}", flush=True)
        return
    try:
        # Convert UNIX epoch to timezone-aware datetime
        ts = datetime.fromtimestamp(float(ts_val), tz=timezone.utc)
    except Exception as e:
        print(f"Failed to parse timestamp {ts_val}: {e}", flush=True)
        return

    # Ensure the correct monthly partition exists
    ensure_month_partition(ts)

    # Insert into Postgres
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO wikimedia_events (id, type, title, "user", bot, minor, timestamp, server_name, comment)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id, timestamp) DO NOTHING;
    """, (
        item.get("id"),
        item.get("type"),
        item.get("title"),
        item.get("user"),
        item.get("bot"),
        item.get("minor"),
        ts,
        item.get("server_name"),
        item.get("comment")
    ))
    conn.commit()
    cur.close()
    conn.close()

def connect_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="wikimedia-group"
            )
            print("Connected to Kafka!", flush=True)
            return consumer
        except NoBrokersAvailable:
            print("Waiting for Kafka to be ready...", flush=True)
            time.sleep(5)

def main():
    create_parent_table()
    consumer = connect_consumer()

    print("Consumer started, waiting for messages...", flush=True)
    for message in consumer:
        item = message.value
        insert_data(item)
        print(f"Inserted: {item}", flush=True)

if __name__ == "__main__":
    # Retry until Postgres is ready
    for _ in range(10):
        try:
            create_parent_table()
            break
        except Exception as e:
            print("Waiting for Postgres...", e, flush=True)
            time.sleep(5)
    main()
