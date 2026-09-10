# Kafka Order Processing System

A Kafka-based pipeline that produces and consumes Avro-encoded order
messages, with real-time running-average aggregation, retry logic for
transient failures, and a Dead Letter Queue (DLQ) for permanently
failed messages.

## Architecture

```
producer.py --(Avro-encoded Order)--> [orders topic] --> consumer.py
                                                            |  |
                                                   running avg  on repeated
                                                   (in-memory)  failure -->
                                                                [orders-dlq topic]
                                                                     |
                                                              dlq_consumer.py
```

- **Schema**: `schemas/order.avsc` defines the `Order` record (`orderId`,
  `product`, `price`).
- **Serialization**: `common/avro_utils.py` uses `fastavro` to encode/decode
  each message's raw bytes directly against the schema (no separate
  Schema Registry service, to keep this self-contained for the assignment).
- **Producer**: generates a random order every second and sends it.
- **Consumer**: decodes each order, "processes" it (a stand-in for real
  business logic) which is randomly made to fail ~35% of the time to
  demonstrate retries, and maintains a running average of prices overall
  and per product.
- **Retry logic**: on a processing failure, the consumer retries up to 3
  times with exponential backoff (1s, 2s, 4s).
- **DLQ**: if all retries are exhausted, the original message is forwarded
  untouched to the `orders-dlq` topic instead of being dropped.

## 1. Setup

```bash
# Start Kafka + Zookeeper
docker compose up -d

# Python deps
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2. Run the demo (3 terminals)

Terminal 1 — consumer:
```bash
source venv/bin/activate
python consumer/consumer.py
```

Terminal 2 — producer:
```bash
source venv/bin/activate
python producer/producer.py
```

Terminal 3 (optional) — watch the DLQ:
```bash
source venv/bin/activate
python dlq_consumer/dlq_consumer.py
```

You should see the producer sending orders, the consumer processing most
of them and printing the running average, occasional "Retry x/3..." lines,
and every so often a message forwarded to the DLQ after 3 failed retries.

## 3. Tear down

```bash
docker compose down
```

## Step-by-step: preparing this for submission

1. **Understand every file before the live demo.** You'll likely be asked
   to explain a design choice (why Avro, why exponential backoff, why the
   DLQ forwards raw bytes, etc.) — walk through `consumer/consumer.py` and
   `common/avro_utils.py` until you can explain each function in your own
   words.
2. **Test it yourself end-to-end** using the steps above. Let it run for
   a minute or two so you actually see retries and at least one DLQ
   message — if you never see one, temporarily raise
   `SIMULATED_FAILURE_RATE` in `consumer.py` to make failures more
   frequent for the demo, then set it back.
3. **Initialize a Git repository** (if you haven't already):
   ```bash
   cd kafka-order-system
   git init
   git add .
   git commit -m "Kafka order processing system: Avro, retries, DLQ"
   ```
4. **Create a remote repo** (e.g. on GitHub/GitLab) and push:
   ```bash
   git remote add origin <your-repo-url>
   git branch -M main
   git push -u origin main
   ```
5. **Add a `.gitignore`** for the venv so you don't commit it:
   ```
   venv/
   __pycache__/
   *.pyc
   ```
6. **Re-read the assignment brief** and confirm each bullet is covered:
   Avro serialization ✓, running average ✓, retry logic ✓, DLQ ✓,
   live demo readiness ✓, Git repo ✓.
7. **Submit the Git repository link** per your module's submission
   instructions (check if your course wants the link on the LMS, emailed,
   or a tagged release — this part is course-specific, so check your
   assignment portal).
8. **On demo day**: start Kafka, start the consumer, start the producer,
   narrate what's happening as messages flow, then point out a retry and
   a DLQ message live so the grader sees both failure paths working.

## Notes / possible extensions if you want to go further
- Swap `kafka-python` for `confluent-kafka` if you want a real Schema
  Registry integration instead of the local `.avsc` file.
- Persist the running average to a file or database so it survives a
  consumer restart (currently it's in-memory only).
- Add unit tests for `process_order` and the retry loop.
