from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from schema import DRILLING_LOG_SCHEMA
import requests
from logger import log
import time

producer_config = {
    'bootstrap.servers':'localhost:9092'
    }

# Connecting to Schema Registry docker image
schema_registry_client = SchemaRegistryClient ({
    "url": "http://localhost:8081"
})


# Before sending the data to the Kafka, first convert the python data into JSON byte,validate it using the schema, and register/use that schema in Schema Registry.
json_serializer = JSONSerializer(
    schema_str=DRILLING_LOG_SCHEMA,
    schema_registry_client=schema_registry_client
)

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
            producer.produce(
                topic="raw-drill-data",
                value=json_serializer(
                    row,
                    SerializationContext("raw-drill-data", MessageField.VALUE)
                ),
                callback=deliery_report
            )
            producer.flush()
            last_sequence = row["sequence_number"]

    except requests.exceptions.Timeout:
        log.error ("API timeout. Will try again...")

    except requests.exceptions.ConnectionError:
        log.error("API connection failed. Is the API running?")

    time.sleep(1)