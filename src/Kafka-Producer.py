import json
import requests
from kafka import KafkaProducer
from sseclient import SSEClient as EventSource

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

url = 'https://stream.wikimedia.org/v2/stream/recentchange'

for event in EventSource(url):
    if event.event == 'message':
        try:
            change = json.loads(event.data)
            producer.send('wikipedia_changes', change)
            print(f"Sent: {change['title']}")
        except Exception as e:
            print(f"Error: {e}")

producer.flush()