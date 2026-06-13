from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from schema import DRILLING_LOG_SCHEMA
import json
from logger import log

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

        mnemonics = raw_data["mnemonic_list"]
        values = raw_data["data"]

        # Len Validation:
                # Len Validation:
        if len(mnemonics) == len(values):

            md = values[1]
            rop = values[2]
            wob = values[3]
            rpm = values[4]
            torque = values[5]
            spp = values[6]
            flow = values[7]
            hkld = values[8]

            is_good = (
                md >= 0
                and rop >= 0
                and wob >= 0
                and 0 <= rpm <= 300
                and torque >= 0
                and spp > 0
                and flow >= 0
                and hkld >= 0
            )

            if is_good:
                producer.produce(
                    topic=CLEAN_TOPIC,
                    value=json.dumps(raw_data).encode("utf-8")
                )
                producer.flush()
                log.info(f"Sent to clean topic: {raw_data['sequence_number']}")
                print("Sent to clean topic:", raw_data["sequence_number"])

            else:
                producer.produce(
                    topic=BAD_TOPIC,
                    value=json.dumps(raw_data).encode("utf-8")
                )
                producer.flush()
                log.info(f"Sent to unclean topic: {raw_data['sequence_number']}")
                print("Sent to unclean topic:", raw_data["sequence_number"])

        else:
            producer.produce(
                topic=BAD_TOPIC,
                value=json.dumps(raw_data).encode("utf-8")
            )
            producer.flush()
            log.info(f"Sent to unclean topic: {raw_data['sequence_number']}")
            print("Sent to unclean topic:", raw_data["sequence_number"])

    except Exception as e:
        print("Error:", e)