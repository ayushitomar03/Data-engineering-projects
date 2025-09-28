import json
import time
import requests
import sseclient
from kafka import KafkaProducer
from datetime import datetime, timezone
from requests.exceptions import ChunkedEncodingError

KAFKA_BROKER = "kafka:29092"  # internal Docker hostname
TOPIC = "wikimedia-recentchange"
URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS = {
    "User-Agent": "KafkaProducerDemo/1.0 (https://yourdomain.com/; contact@example.com)"
}

FIELDS_TO_KEEP = [
    "id", "type", "title", "user", "bot", "minor",
    "timestamp", "server_name", "comment"
]

def connect_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print("Waiting for Kafka...", e)
            time.sleep(5)

def stream_events(producer):
    while True:
        print("Connecting to Wikimedia EventStreams...")
        try:
            with requests.get(URL, headers=HEADERS, stream=True, timeout=30) as resp:
                if resp.status_code != 200:
                    print(f"Stream connect failed: {resp.status_code}, retrying in 10s...")
                    time.sleep(10)
                    continue

                client = sseclient.SSEClient(resp)
                for event in client.events():
                    try:
                        data = json.loads(event.data)
                        producer.send(TOPIC, value=data)
                        print(f"Sent: {data}", flush=True)
                    except Exception as e:
                        print("Error processing event:", e)

        except ChunkedEncodingError:
            print("Connection dropped, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print("Stream error, retrying in 10s...", e)
            time.sleep(10)

def main():
    producer = connect_producer()
    stream_events(producer)

if __name__ == "__main__":
    main()
