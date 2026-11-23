"""Owner: Person B — Day 4
Storage consumer: reads Kafka and writes to InfluxDB (interface contract, section 4).

  building.telemetry  -> measurement "telemetry"   tags: building, floor, room, zone, sensorType, status
                                                    field: value
  building.alerts     -> measurement "alerts"      tags: floor, room, sensorType, severity
                                                    fields: value, threshold, message
  building.aggregates -> measurement "aggregates"  tags: floor, room, sensorType
                                                    fields: avg, min, max, count

No data loss: Kafka offsets are committed only AFTER InfluxDB confirmed the write.
If InfluxDB is down, the batch is retried until it succeeds.

Messages that break the contract are logged ([CONTRACT]) and skipped.

Environment variables:
  KAFKA_BOOTSTRAP   default kafka:29092 (use localhost:9092 outside Docker)
  GROUP_ID          default storage
  INFLUX_URL        default http://influxdb:8086
  INFLUX_TOKEN      required
  INFLUX_ORG        default smartbuilding
  INFLUX_BUCKET     default building
  BATCH_SIZE        default 500
  SUMMARY_SECONDS   default 10
"""
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
sys.path.insert(0, "/app/common")
from contract import (  # noqa: E402
    ALERT_REQUIRED, TELEMETRY_REQUIRED, TOPIC_AGGREGATES, TOPIC_ALERTS, TOPIC_TELEMETRY,
    missing_fields,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
GROUP_ID = os.getenv("GROUP_ID", "storage")
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "smartbuilding")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "building")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
SUMMARY_SECONDS = float(os.getenv("SUMMARY_SECONDS", "10"))

AGGREGATE_REQUIRED = ["floor", "room", "sensorType", "windowStart", "avg", "min", "max", "count"]

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def parse_time(value: str) -> datetime:
    """ISO 8601 with a Z suffix -> aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_point(topic: str, data: dict) -> Point:
    """Convert a contract message into an InfluxDB point.

    Numbers are always written as float so a field never changes type
    (InfluxDB rejects an int after a float in the same field)."""
    if topic == TOPIC_TELEMETRY:
        return (Point("telemetry")
                .tag("building", data["buildingId"])
                .tag("floor", str(data["floor"]))
                .tag("room", data["room"])
                .tag("zone", data["zone"])
                .tag("sensorType", data["sensorType"])
                .tag("status", data["status"])
                .field("value", float(data["value"]))
                .time(parse_time(data["timestamp"]), WritePrecision.MS))
    if topic == TOPIC_ALERTS:
        point = (Point("alerts")
                 .tag("floor", str(data["floor"]))
                 .tag("room", data["room"])
                 .tag("sensorType", data["sensorType"])
                 .tag("severity", data["severity"])
                 .field("value", float(data["value"]))
                 .field("message", str(data["message"]))
                 .time(parse_time(data["timestamp"]), WritePrecision.MS))
        if data.get("threshold") is not None:
            point.field("threshold", float(data["threshold"]))
        return point
    # aggregates
    return (Point("aggregates")
            .tag("floor", str(data["floor"]))
            .tag("room", data["room"])
            .tag("sensorType", data["sensorType"])
            .field("avg", float(data["avg"]))
            .field("min", float(data["min"]))
            .field("max", float(data["max"]))
            .field("count", float(data["count"]))
            .time(parse_time(data["windowStart"]), WritePrecision.MS))


REQUIRED = {TOPIC_TELEMETRY: TELEMETRY_REQUIRED, TOPIC_ALERTS: ALERT_REQUIRED,
            TOPIC_AGGREGATES: AGGREGATE_REQUIRED}


def write_with_retry(write_api, points: list[Point]) -> None:
    """Write a batch to InfluxDB, retrying until it works (or the service is stopped)."""
    delay = 1
    while True:
        try:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            return
        except Exception as exc:  # noqa: BLE001 — log any InfluxDB/network error and retry
            print(f"[INFLUX ERROR] {exc.__class__.__name__}: {str(exc)[:200]} "
                  f"- retry in {delay}s", flush=True)
            if not running:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 30)


def main():
    if not INFLUX_TOKEN:
        sys.exit("INFLUX_TOKEN is not set")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,          # we commit ourselves, after InfluxDB confirmed
    })
    consumer.subscribe([TOPIC_TELEMETRY, TOPIC_ALERTS, TOPIC_AGGREGATES])

    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = influx.write_api(write_options=SYNCHRONOUS)
    print(f"Storage consumer <- {BOOTSTRAP} (group={GROUP_ID}) -> {INFLUX_URL} "
          f"bucket={INFLUX_BUCKET}", flush=True)

    counts = Counter()
    last_summary = time.time()
    written_ms = []

    try:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            points = []
            for msg in messages:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[KAFKA ERROR] {msg.error()}", flush=True)
                    continue
                topic = msg.topic()
                try:
                    data = json.loads(msg.value())
                except (TypeError, ValueError):
                    counts["skipped"] += 1
                    print(f"[NOT JSON] {topic}: {msg.value()[:120]!r}", flush=True)
                    continue

                missing = missing_fields(data, REQUIRED[topic])
                if missing:
                    counts["skipped"] += 1
                    if counts["skipped"] <= 5 or counts["skipped"] % 100 == 0:
                        print(f"[CONTRACT] {topic} missing={missing} - skipped "
                              f"(#{counts['skipped']})", flush=True)
                    continue

                try:
                    points.append(to_point(topic, data))
                except (ValueError, TypeError, KeyError) as exc:
                    counts["skipped"] += 1
                    print(f"[BAD VALUE] {topic}: {exc} - skipped", flush=True)
                    continue

                counts[topic] += 1
                if topic == TOPIC_ALERTS:
                    print(f"[ALERT] {data['severity']} {data['room']} {data['sensorType']}="
                          f"{data['value']} - {data['message']}", flush=True)

            if points:
                started = time.time()
                write_with_retry(write_api, points)
                written_ms.append((time.time() - started) * 1000)
                counts["written"] += len(points)
            if messages:
                consumer.commit(asynchronous=False)

            if time.time() - last_summary >= SUMMARY_SECONDS:
                avg_ms = sum(written_ms) / len(written_ms) if written_ms else 0
                print(f"--- last {SUMMARY_SECONDS:.0f}s: written={counts['written']} "
                      f"(telemetry={counts[TOPIC_TELEMETRY]} alerts={counts[TOPIC_ALERTS]} "
                      f"aggregates={counts[TOPIC_AGGREGATES]}) skipped={counts['skipped']} "
                      f"avg_write={avg_ms:.0f}ms", flush=True)
                counts.clear()
                written_ms.clear()
                last_summary = time.time()
    finally:
        consumer.close()
        influx.close()
        print("Storage consumer stopped.", flush=True)


if __name__ == "__main__":
    main()
