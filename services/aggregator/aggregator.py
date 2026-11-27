"""Owner: Person B — Day 6
Aggregator: reads building.telemetry, computes 1-minute tumbling windows per room + sensorType
and publishes them to building.aggregates (interface contract, section 2.3).

    {"schemaVersion": 1, "floor": 2, "room": "R203", "sensorType": "co2",
     "windowStart": "2026-11-23T13:04:00Z", "windowEnd": "2026-11-23T13:05:00Z",
     "avg": 912.4, "min": 845.2, "max": 1620.0, "count": 30}

How windows are closed (event time):
- Each message is placed in the window of its own `timestamp` (time of the reading), not the
  time it arrives. Replaying old data therefore gives the same result.
- Watermark = the oldest "latest timestamp" among the Kafka partitions that are active
  (received data in the last IDLE_SECONDS). A window is closed and published once
  windowEnd + ALLOWED_LATENESS_SECONDS <= watermark.
- If no data arrives at all for IDLE_SECONDS, the wall clock is used, so the last windows
  are still published when the simulator stops.
- A reading that arrives after its window was closed is counted as "late" and ignored.

No data loss / no duplicates after a restart:
- Offsets are committed manually. For each partition we commit the oldest offset that still
  belongs to an open window. After a crash, Kafka replays from there and the open windows are
  rebuilt; windows already published are older than the watermark and are not published twice.

Environment variables:
  KAFKA_BOOTSTRAP            default kafka:29092 (use localhost:9092 outside Docker)
  GROUP_ID                   default aggregator
  WINDOW_SECONDS             default 60
  ALLOWED_LATENESS_SECONDS   default 5
  IDLE_SECONDS               default 10
  SUMMARY_SECONDS            default 30
"""
import json
import os
import signal
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
sys.path.insert(0, "/app/common")
from contract import SCHEMA_VERSION, TOPIC_AGGREGATES, TOPIC_TELEMETRY  # noqa: E402

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
GROUP_ID = os.getenv("GROUP_ID", "aggregator")
WINDOW = int(os.getenv("WINDOW_SECONDS", "60"))
LATENESS = float(os.getenv("ALLOWED_LATENESS_SECONDS", "5"))
IDLE = float(os.getenv("IDLE_SECONDS", "10"))
SUMMARY_SECONDS = float(os.getenv("SUMMARY_SECONDS", "30"))

REQUIRED = ["floor", "room", "sensorType", "value", "timestamp"]

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def parse_ts(value: str) -> float:
    """ISO 8601 (Z or +00:00) -> epoch seconds."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Window:
    start: float
    floor: int
    room: str
    sensor_type: str
    total: float = 0.0
    minimum: float = float("inf")
    maximum: float = float("-inf")
    count: int = 0
    first_offsets: dict = field(default_factory=dict)  # partition -> lowest offset in this window

    def add(self, value: float, partition: int, offset: int) -> None:
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        self.count += 1
        if partition not in self.first_offsets or offset < self.first_offsets[partition]:
            self.first_offsets[partition] = offset

    def to_message(self) -> dict:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "floor": self.floor,
            "room": self.room,
            "sensorType": self.sensor_type,
            "windowStart": iso(self.start),
            "windowEnd": iso(self.start + WINDOW),
            "avg": round(self.total / self.count, 2),
            "min": round(self.minimum, 2),
            "max": round(self.maximum, 2),
            "count": self.count,
        }


class Aggregator:
    def __init__(self):
        self.windows: dict[tuple, Window] = {}
        self.max_ts: dict[int, float] = {}         # partition -> latest reading timestamp seen
        self.last_seen: dict[int, float] = {}      # partition -> wall-clock time of last message
        self.next_offset: dict[int, int] = {}      # partition -> offset after the last consumed one
        self.assigned_at: dict[int, float] = {}   # partition -> wall-clock time it was assigned
        self.closed_until = float("-inf")          # every window starting before this is closed
        self.stats = Counter()

    # ------------------------------------------------------------------ input
    def add(self, data: dict, partition: int, offset: int) -> None:
        self.next_offset[partition] = offset + 1
        self.last_seen[partition] = time.time()

        if any(f not in data for f in REQUIRED):
            self.stats["skipped"] += 1
            return
        try:
            ts = parse_ts(data["timestamp"])
            value = float(data["value"])
        except (TypeError, ValueError):
            self.stats["skipped"] += 1
            return

        self.max_ts[partition] = max(self.max_ts.get(partition, ts), ts)
        start = ts - (ts % WINDOW)
        if start < self.closed_until:
            self.stats["late"] += 1
            return

        key = (start, data["room"], data["sensorType"])
        window = self.windows.get(key)
        if window is None:
            window = self.windows[key] = Window(start, int(data["floor"]), data["room"], data["sensorType"])
        window.add(value, partition, offset)
        self.stats["consumed"] += 1

    # ------------------------------------------------------------------ closing
    def watermark(self, now: float) -> float:
        # A partition just assigned that has not delivered anything yet may still hold old
        # readings (e.g. replay after a restart): wait for it, up to IDLE seconds.
        for p, assigned in self.assigned_at.items():
            if p not in self.max_ts and now - assigned < IDLE:
                return float("-inf")
        active = [self.max_ts[p] for p, seen in self.last_seen.items()
                  if now - seen < IDLE and p in self.max_ts]
        return min(active) if active else now

    def close_ready(self, now: float) -> list[Window]:
        """Windows whose end + allowed lateness is behind the watermark, oldest first."""
        wm = self.watermark(now)
        ready = [w for w in self.windows.values() if w.start + WINDOW + LATENESS <= wm]
        for w in ready:
            del self.windows[(w.start, w.room, w.sensor_type)]
            self.closed_until = max(self.closed_until, w.start + WINDOW)
        return sorted(ready, key=lambda w: (w.start, w.room, w.sensor_type))

    def commit_positions(self) -> dict[int, int]:
        """Per partition: oldest offset still needed by an open window, else next offset."""
        positions = dict(self.next_offset)
        for w in self.windows.values():
            for p, off in w.first_offsets.items():
                positions[p] = min(positions.get(p, off), off)
        return positions


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] delivery failed for {msg.topic()}: {err}", flush=True)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "aggregator",
                         "acks": "all", "linger.ms": 50})
    agg = Aggregator()

    def on_assign(_consumer, partitions):
        now = time.time()
        for tp in partitions:
            agg.assigned_at[tp.partition] = now
        print(f"Assigned partitions: {[tp.partition for tp in partitions]}", flush=True)

    consumer.subscribe([TOPIC_TELEMETRY], on_assign=on_assign)
    print(f"Aggregator <- {TOPIC_TELEMETRY} -> {TOPIC_AGGREGATES} | window={WINDOW}s "
          f"lateness={LATENESS}s | group={GROUP_ID}", flush=True)

    last_summary = time.time()
    last_committed: dict[int, int] = {}
    published = 0

    try:
        while running:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[KAFKA ERROR] {msg.error()}", flush=True)
                    continue
                try:
                    data = json.loads(msg.value())
                except (TypeError, ValueError):
                    agg.stats["skipped"] += 1
                    continue
                agg.add(data, msg.partition(), msg.offset())

            closed = agg.close_ready(time.time())
            for w in closed:
                producer.produce(TOPIC_AGGREGATES, key=w.room, value=json.dumps(w.to_message()),
                                 callback=delivery_report)
            if closed:
                producer.flush(10)          # aggregates are safely in Kafka before we commit
                published += len(closed)
                starts = sorted({iso(w.start)[11:16] for w in closed})
                print(f"[PUBLISHED] {len(closed)} aggregates for window(s) {', '.join(starts)} UTC",
                      flush=True)

            positions = agg.commit_positions()
            to_commit = [TopicPartition(TOPIC_TELEMETRY, p, off) for p, off in positions.items()
                         if last_committed.get(p) != off]
            if to_commit:
                consumer.commit(offsets=to_commit, asynchronous=False)
                last_committed.update({tp.partition: tp.offset for tp in to_commit})

            if time.time() - last_summary >= SUMMARY_SECONDS:
                print(f"--- last {SUMMARY_SECONDS:.0f}s: consumed={agg.stats['consumed']} "
                      f"published={published} open_windows={len(agg.windows)} "
                      f"late={agg.stats['late']} skipped={agg.stats['skipped']}", flush=True)
                agg.stats.clear()
                published = 0
                last_summary = time.time()
    finally:
        producer.flush(10)
        consumer.close()
        print("Aggregator stopped.", flush=True)


if __name__ == "__main__":
    main()
