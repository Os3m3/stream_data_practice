from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from schema import DRILLING_LOG_SCHEMA

RAW_TOPIC = "raw-drill-data"
CLEAN_TOPIC = "clean-drilling-data"
BAD_TOPIC = "bad-drilling-data"

consumer = Consumer({
    "bootstrap.servers" : "localhost:9092",
    "group.id" : "validation_group",
    "auto.offset.reset" : "earliest"
})

consumer.subscribe([RAW_TOPIC])

producer = Producer({
    "bootstrap.servers" : "localhost:9092"
})

schema_registry_client = SchemaRegistryClient({
    "url": "http://localhost:8081"
})


# deserialize the event to match the schema that sent from producer.py and validated by Schema Registry.
json_deserializer = JSONDeserializer(
    schema_str=DRILLING_LOG_SCHEMA,
    schema_registry_client=schema_registry_client
)


while True:
    try:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        raw_data = json_deserializer(
            msg.value(),
            SerializationContext(RAW_TOPIC, MessageField.VALUE)
        )

        print("Deserialized data:", raw_data)

    except Exception as e:
        print("Error:", e)