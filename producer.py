import csv
import json
import time
from kafka import KafkaProducer

# Config matching your tutorial's variables
KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC = "ev_topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def run_producer():
    # Path as specified in your training PDF
    file_path = 'input/Electric_Vehicle_Population_Data.csv'
    print(f"Streaming data to {KAFKA_TOPIC}...")

    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            producer.send(KAFKA_TOPIC, value=row)
            time.sleep(0.01) # Simulate real-time streaming

if __name__ == "__main__":
    run_producer()
    producer.flush()
