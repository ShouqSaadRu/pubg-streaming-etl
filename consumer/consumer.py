import json, os
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
load_dotenv()
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_COMBAT_TOPIC", "pubg.combat-events")

def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "pubg-kill-feed",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    print(f"Listening to {TOPIC}. Press Ctrl+C to stop.")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            event = json.loads(message.value().decode("utf-8"))
            print(f"[partition={message.partition()} offset={message.offset()}] {event['killer']} killed {event['victim']} with {event['weapon']} from {event['distance_meters']}m")
            consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
