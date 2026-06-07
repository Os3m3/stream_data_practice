from confluent_kafka import Producer
import requests
from logger import log
import time

producer_config = {
    'bootstrap.servers':'localhost:9092'
    }

producer = Producer(producer_config)

topic_name = "witsml-drilling-data"

api_url =  "http://localhost:8000/witsml/logs"

last_sequence = 0

log.info("Producer started")
log.info(f"Kafka bootstrap server: {producer_config['bootstrap.servers']}")
log.info(f"Kafka topic: {topic_name}")
log.info(f"API URL: {api_url}")

while True:
    try:
        res = requests.get(api_url, params={"after_sequence": last_sequence}, timeout=5)

        data = res.json()

        rows = data["rows"]
        for row in rows:
            print(row)
            last_sequence = row["sequence_number"]

    except requests.exceptions.Timeout:
        log.error ("API timeout. Will try again...")

    except requests.exceptions.ConnectionError:
        log.error("API connection failed. Is the API running?")

    time.sleep(1)