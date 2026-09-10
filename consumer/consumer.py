"""
Order consumer.

For each order message:
  1. Deserializes it from Avro.
  2. "Processes" it (here: updates a running average of prices). Processing
     is randomly made to fail sometimes, to simulate a real transient
     failure (e.g. a flaky downstream DB call).
  3. On failure, retries with exponential backoff up to MAX_RETRIES times.
  4. If still failing after MAX_RETRIES, the original message is
     forwarded untouched to the DLQ topic so nothing is silently lost.

Run producer.py in another terminal at the same time to see messages
flow end-to-end.
"""
import random
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

sys.path.append(str(Path(__file__).resolve().parent.parent))
from common.avro_utils import decode_order

BROKER = "localhost:9092"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-processing-group"

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1
# Chance that "processing" a message fails transiently, to demonstrate
# the retry + DLQ paths without needing a real flaky dependency.
SIMULATED_FAILURE_RATE = 0.35


class RunningAverage:
    """Tracks a running average of prices, overall and per product."""

    def __init__(self):
        self.count = 0
        self.total = 0.0
        self.per_product = {}  # product -> [count, total]

    def update(self, order: dict):
        self.count += 1
        self.total += order["price"]

        stats = self.per_product.setdefault(order["product"], [0, 0.0])
        stats[0] += 1
        stats[1] += order["price"]

    def overall_avg(self) -> float:
        return self.total / self.count if self.count else 0.0

    def product_avg(self, product: str) -> float:
        stats = self.per_product.get(product)
        return stats[1] / stats[0] if stats else 0.0


def process_order(order: dict, aggregator: RunningAverage):
    """Simulate business-logic processing that can transiently fail."""
    if random.random() < SIMULATED_FAILURE_RATE:
        raise RuntimeError(f"Transient processing error for order {order['orderId']}")
    aggregator.update(order)


def handle_message(raw_bytes: bytes, aggregator: RunningAverage, dlq_producer: KafkaProducer):
    order = decode_order(raw_bytes)
    attempt = 0

    while attempt <= MAX_RETRIES:
        try:
            process_order(order, aggregator)
            print(
                f"Processed {order} | "
                f"running avg (overall): {aggregator.overall_avg():.2f} | "
                f"running avg ({order['product']}): {aggregator.product_avg(order['product']):.2f}"
            )
            return
        except RuntimeError as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                print(f"Giving up on order {order['orderId']} after {MAX_RETRIES} retries: {e}")
                # Forward the original Avro bytes untouched to the DLQ.
                dlq_producer.send(DLQ_TOPIC, value=raw_bytes)
                dlq_producer.flush()
                print(f"-> Sent order {order['orderId']} to DLQ topic '{DLQ_TOPIC}'")
                return
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"Retry {attempt}/{MAX_RETRIES} for order {order['orderId']} after {backoff}s ({e})")
            time.sleep(backoff)


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b,  # decode manually so we can forward raw bytes to DLQ
    )
    dlq_producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda b: b)
    aggregator = RunningAverage()

    print(f"Consuming from '{TOPIC}' on {BROKER}. Ctrl+C to stop.")
    try:
        for message in consumer:
            handle_message(message.value, aggregator, dlq_producer)
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()
        dlq_producer.close()


if __name__ == "__main__":
    main()
