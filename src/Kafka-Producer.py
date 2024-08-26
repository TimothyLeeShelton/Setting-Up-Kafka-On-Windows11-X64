import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')],
    value_serializer=json_serializer
)

url = 'https://stream.wikimedia.org/v2/stream/recentchange'

def event_stream():
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            yield line.decode('utf-8')

if __name__ == "__main__":
    for event in event_stream():
        if event.startswith('data: '):
            try:
                change = json.loads(event[6:])  # Skip the 'data: ' prefix
                producer.send('wikipedia_edits', change)
                print(f"Sent: {change['title']}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON: {event}")
            except Exception as e:
                print(f"Error: {e}")

    producer.flush()