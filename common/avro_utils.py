"""
Shared helpers for Avro-encoding and decoding order messages.

We use fastavro directly against the order.avsc schema file rather than
a Schema Registry, to keep the assignment self-contained (no extra
service to run). Each message on the wire is just the raw Avro binary
bytes for one record.
"""
import io
import os
import fastavro

_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order.avsc")
ORDER_SCHEMA = fastavro.schema.load_schema(_SCHEMA_PATH)


def encode_order(order: dict) -> bytes:
    """Serialize a dict matching the Order schema into Avro binary bytes."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, ORDER_SCHEMA, order)
    return buf.getvalue()


def decode_order(data: bytes) -> dict:
    """Deserialize Avro binary bytes back into a dict."""
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, ORDER_SCHEMA)
