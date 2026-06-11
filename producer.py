from confluent_kafka import Producer
import requests
from logger import log
import time
import json

producer_config = {
    'bootstrap.servers':'localhost:9092'
    }

producer = Producer(producer_config)


api_url =  "http://localhost:8000/witsml/logs"

last_sequence = 0

log.info("Producer started")
log.info(f"Kafka bootstrap server: {producer_config['bootstrap.servers']}")
log.info(f"API URL: {api_url}")


# -- Deliver report:
def deliery_report(err, msg):
    if err:
        log.error(f"❌ Delivery Faild: {err}")
    else:
        log.info(f"✅ Delivered: {msg}")
        

while True:
    try:
        res = requests.get(api_url, params={"after_sequence": last_sequence}, timeout=5)

        data = res.json()

        rows = data["rows"]
        for row in rows:
            print(row)
            value = json.dumps(row).encode("utf-8")
            producer.produce(
                topic="raw-drill-data",
                value=value,
                callback=deliery_report
            )
            producer.flush()
            last_sequence = row["sequence_number"]

    except requests.exceptions.Timeout:
        log.error ("API timeout. Will try again...")

    except requests.exceptions.ConnectionError:
        log.error("API connection failed. Is the API running?")

    time.sleep(1)