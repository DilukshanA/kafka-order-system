"""
Optional helper: prints out any messages that landed in the DLQ topic,
decoded back from Avro. Handy for the live demo to prove failed
messages weren't lost.
"""
import sys
from pathlib import Path

from kafka import KafkaConsumer

sys.path.append(str(Path(__file__).resolve().parent.parent))
from common.avro_utils import decode_order

BROKER = "localhost:9092"
DLQ_TOPIC = "orders-dlq"


def main():
    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BROKER,
        group_id="dlq-inspector-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b,
    )
    print(f"Watching DLQ topic '{DLQ_TOPIC}'. Ctrl+C to stop.")
    try:
        for message in consumer:
            order = decode_order(message.value)
            print(f"[DLQ] {order}")
    except KeyboardInterrupt:
        print("\nStopping DLQ consumer.")


if __name__ == "__main__":
    main()
