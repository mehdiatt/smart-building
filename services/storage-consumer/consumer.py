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

Pipeline latency (day 9): for every telemetry message written, three delays are measured
  ingest   = processedAt - timestamp      (sensor -> MQTT -> Node-RED)
  pipeline = written    - processedAt     (Node-RED -> Kafka -> consumer -> InfluxDB)
  total    = written    - timestamp       (end to end)
Every SUMMARY_SECONDS their avg / p95 / max and the throughput are written to the
InfluxDB measurement "pipeline_latency" (shown in Grafana) and printed in the logs.

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
from datetime import datetime, timezone
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


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile of a non-empty list."""
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round(pct / 100 * len(ordered)) - 1))
    return ordered[index]


def latency_point(lat: dict, window_seconds: float) -> Point | None:
    """Summary of the latencies measured during one window, or None if nothing was written."""
    if not lat["total"]:
        return None
    point = Point("pipeline_latency").field("messages", float(len(lat["total"]))) \
        .field("throughput", len(lat["total"]) / window_seconds)
    for name, values in lat.items():
        if values:
            point.field(f"avg_{name}_ms", sum(values) / len(values))
            point.field(f"p95_{name}_ms", float(percentile(values, 95)))
            point.field(f"max_{name}_ms", float(max(values)))
    return point.time(datetime.now(timezone.utc), WritePrecision.MS)


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
    latencies = {"ingest": [], "pipeline": [], "total": []}

    try:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            points = []
            stamps = []          # (reading time, processedAt) of each telemetry message, epoch seconds
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
                if topic == TOPIC_TELEMETRY:
                    try:
                        stamps.append((parse_time(data["timestamp"]).timestamp(),
                                       parse_time(data["processedAt"]).timestamp()))
                    except (ValueError, AttributeError):
                        pass
                if topic == TOPIC_ALERTS:
                    print(f"[ALERT] {data['severity']} {data['room']} {data['sensorType']}="
                          f"{data['value']} - {data['message']}", flush=True)

            if points:
                started = time.time()
                write_with_retry(write_api, points)
                written_at = time.time()
                written_ms.append((written_at - started) * 1000)
                counts["written"] += len(points)
                for reading_ts, processed_ts in stamps:
                    latencies["ingest"].append((processed_ts - reading_ts) * 1000)
                    latencies["pipeline"].append((written_at - processed_ts) * 1000)
                    latencies["total"].append((written_at - reading_ts) * 1000)
            if messages:
                consumer.commit(asynchronous=False)

            if time.time() - last_summary >= SUMMARY_SECONDS:
                window = time.time() - last_summary
                avg_ms = sum(written_ms) / len(written_ms) if written_ms else 0
                print(f"--- last {SUMMARY_SECONDS:.0f}s: written={counts['written']} "
                      f"(telemetry={counts[TOPIC_TELEMETRY]} alerts={counts[TOPIC_ALERTS]} "
                      f"aggregates={counts[TOPIC_AGGREGATES]}) skipped={counts['skipped']} "
                      f"avg_write={avg_ms:.0f}ms", flush=True)
                point = latency_point(latencies, window)
                if point is not None:
                    write_with_retry(write_api, [point])
                    total = latencies["total"]
                    print(f"    latency ms: total avg={sum(total) / len(total):.0f} "
                          f"p95={percentile(total, 95):.0f} max={max(total):.0f} | "
                          f"ingest avg={sum(latencies['ingest']) / len(total):.0f} | "
                          f"pipeline avg={sum(latencies['pipeline']) / len(total):.0f} | "
                          f"throughput={len(total) / window:.1f} msg/s", flush=True)
                    if min(total) < -100:
                        print("    [WARN] negative latency: the clocks of the PC and of Docker "
                              "are out of sync (wsl --shutdown then restart Docker Desktop)", flush=True)
                counts.clear()
                written_ms.clear()
                for values in latencies.values():
                    values.clear()
                last_summary = time.time()
    finally:
        consumer.close()
        influx.close()
        print("Storage consumer stopped.", flush=True)


if __name__ == "__main__":
    main()
