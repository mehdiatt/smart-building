"""Owner: Person B — Day 3 (console version)
Reads building.telemetry and building.alerts and checks every message against the
interface contract. Day 4 will add writing to InfluxDB.

Prints:
- every alert,
- every message that does not follow the contract (missing fields / invalid JSON),
- a summary every SUMMARY_SECONDS.

Environment variables:
  KAFKA_BOOTSTRAP   default kafka:29092 (use localhost:9092 outside Docker)
  GROUP_ID          default storage
  SUMMARY_SECONDS   default 10
"""
import json
import os
import signal
import sys
import time
from collections import Counter
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
sys.path.insert(0, "/app/common")
from contract import (  # noqa: E402
    ALERT_REQUIRED, TELEMETRY_REQUIRED, TOPIC_ALERTS, TOPIC_TELEMETRY, missing_fields,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
GROUP_ID = os.getenv("GROUP_ID", "storage")
SUMMARY_SECONDS = float(os.getenv("SUMMARY_SECONDS", "10"))

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_TELEMETRY, TOPIC_ALERTS])
    print(f"Storage consumer (console) <- {BOOTSTRAP} | group={GROUP_ID}", flush=True)

    counts = Counter()
    statuses = Counter()
    rooms = set()
    last_summary = time.time()
    last_sample = None

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[KAFKA ERROR] {msg.error()}", flush=True)
                else:
                    topic = msg.topic()
                    counts[topic] += 1
                    try:
                        data = json.loads(msg.value())
                    except (TypeError, ValueError):
                        counts["invalid_json"] += 1
                        print(f"[NOT JSON] {topic}: {msg.value()[:120]!r}", flush=True)
                        data = None

                    if data is not None:
                        required = TELEMETRY_REQUIRED if topic == TOPIC_TELEMETRY else ALERT_REQUIRED
                        missing = missing_fields(data, required)
                        key = msg.key().decode() if msg.key() else None
                        if missing or key is None:
                            counts["contract_violations"] += 1
                            if counts["contract_violations"] <= 5 or counts["contract_violations"] % 100 == 0:
                                print(f"[CONTRACT] {topic} key={key} missing={missing} "
                                      f"(#{counts['contract_violations']})", flush=True)
                        if topic == TOPIC_TELEMETRY:
                            statuses[data.get("status", "?")] += 1
                            rooms.add(data.get("room"))
                            last_sample = data
                        else:
                            print(f"[ALERT] {data.get('severity')} {data.get('room')} "
                                  f"{data.get('sensorType')}={data.get('value')} - {data.get('message')}",
                                  flush=True)

            if time.time() - last_summary >= SUMMARY_SECONDS:
                print(f"--- last {SUMMARY_SECONDS:.0f}s: telemetry={counts[TOPIC_TELEMETRY]} "
                      f"alerts={counts[TOPIC_ALERTS]} rooms={len(rooms)} status={dict(statuses)} "
                      f"contract_violations={counts['contract_violations']}", flush=True)
                if last_sample:
                    print(f"    sample: {json.dumps(last_sample)}", flush=True)
                counts.clear()
                statuses.clear()
                rooms.clear()
                last_summary = time.time()
    finally:
        consumer.close()
        print("Consumer stopped.", flush=True)


if __name__ == "__main__":
    main()
