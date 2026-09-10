"""
Order producer.

Generates randomized order messages and publishes them, Avro-encoded,
to the 'orders' topic. Run this alongside consumer.py to demo the
full pipeline.
"""
import random
import sys
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer

sys.path.append(str(Path(__file__).resolve().parent.parent))
from common.avro_utils import encode_order

BROKER = "localhost:9092"
TOPIC = "orders"
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def make_order() -> dict:
    return {
        "orderId": str(uuid.uuid4().int)[:6],
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=encode_order,
        # Ask the broker to acknowledge before we consider a send successful.
        acks="all",
    )

    print(f"Producing to '{TOPIC}' on {BROKER}. Ctrl+C to stop.")
    try:
        while True:
            order = make_order()
            producer.send(TOPIC, value=order)
            producer.flush()
            print(f"Sent: {order}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
