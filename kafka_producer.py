import csv
import json
import time
from kafka import KafkaProducer

KAFKA_TOPIC = "network-traffic"
KAFKA_BOOTSTRAP_SERVERS = "xxx.xxx.xxx.xxx:9092"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

CSV_FILE_PATH = r"C:\Users\PC\Desktop\CTI\network_traffic.csv"

def produce_data():
    with open(CSV_FILE_PATH, mode="r") as file:
        reader = csv.DictReader(file)

        for row in reader:
            # Remove the label column with leading space if it exists
            label_key = None
            for key in row.keys():
                if key.strip().lower() == "label":
                    label_key = key
                    break
            if label_key:
                del row[label_key]

            # Convert values to float (double precision) where possible
            clean_row = {k: try_cast_float(v) for k, v in row.items()}

            producer.send(KAFKA_TOPIC, clean_row)
            print(f"Produced to Kafka: {clean_row}")

            time.sleep(0.1)  # simulate streaming

    producer.flush()
    print("✅ All rows sent to Kafka.")

def try_cast_float(value):
    try:
        return float(value)
    except ValueError:
        return value

if __name__ == "__main__":
    produce_data()