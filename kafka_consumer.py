from kafka import KafkaConsumer
import json
import sys

KAFKA_TOPIC = "network-traffic"
KAFKA_BOOTSTRAP_SERVERS = "192.168.0.230:9092"
OUTPUT_FILE = r"C:\Users\PC\Desktop\CTI\data.json"

def consume_and_write(max_messages=100):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Consuming from topic '{KAFKA_TOPIC}', writing up to {max_messages} messages to '{OUTPUT_FILE}'...")

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            count = 0
            for message in consumer:
                data = message.value
                f.write(json.dumps(data) + "\n")
                f.flush()
                print(f"Written message {count+1}: {data}")
                count += 1
                if count >= max_messages:
                    print(f"Reached max_messages limit ({max_messages}). Stopping.")
                    break
    except Exception as e:
        print(f"Error writing to file: {e}", file=sys.stderr)

if __name__ == "__main__":
    consume_and_write()
