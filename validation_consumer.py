from confluent_kafka import Consumer, Producer
import json

consumer = Consumer({
    "bootstrap.servers" : "localhost:9092",
    "group.id" : "validation_group",
    "auto.offset.reset" : "earliest"
})

consumer.subscribe(["raw-drill-data"])

producer = Producer({
    "bootstrap.servers" : "localhost:9092"
})

while True:
    try:
        msg = consumer.poll(1.0)
        print(msg.value().decode("utf-8"))
    except:
        pass