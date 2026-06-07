from confluent_kafka import Producer
import requests


producer_config = {
    'bootstrap.servers':'localhost:9092'
    }

producer = Producer(producer_config)

topic_name = "witsml-drilling-data"

api_url = "http://localhost:8000/well/current"

res = requests.get(api_url)
data = res.json()
print(data)