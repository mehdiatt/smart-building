"""Owner: Person B — Day 3
Fake producer: writes contract-compliant messages to Kafka so Person B can build and test
the consumers, InfluxDB and Grafana without waiting for the real MQTT -> Node-RED pipeline.

- building.telemetry : one enriched message per sensor every INTERVAL_SECONDS (section 2.1)
- building.alerts    : an alert when a reading crosses a threshold, max one per sensor and
                       severity every 60 s (section 2.2)

Every ANOMALY_EVERY_SECONDS, one random room gets a CO2 spike or overheating for
ANOMALY_DURATION_SECONDS, so alerts can be tested.

Environment variables:
  KAFKA_BOOTSTRAP           default kafka:29092 (use localhost:9092 outside Docker)
  BUILDING_CONFIG           default /app/building.json
  INTERVAL_SECONDS          default 2
  ANOMALY_EVERY_SECONDS     default 60   (0 = no anomalies)
  ANOMALY_DURATION_SECONDS  default 20
"""
import json
import math
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
sys.path.insert(0, "/app/common")
from contract import (  # noqa: E402
    SCHEMA_VERSION, TOPIC_ALERTS, TOPIC_TELEMETRY, evaluate, now_iso,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
CONFIG_PATH = Path(os.getenv("BUILDING_CONFIG", "/app/building.json"))
INTERVAL = float(os.getenv("INTERVAL_SECONDS", "2"))
ANOMALY_EVERY = float(os.getenv("ANOMALY_EVERY_SECONDS", "60"))
ANOMALY_DURATION = float(os.getenv("ANOMALY_DURATION_SECONDS", "20"))
ALERT_COOLDOWN = 60  # seconds, contract section 2.2

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def load_sensors(config: dict) -> list[dict]:
    """Expand building.json into one entry per sensor."""
    building = config["buildingId"]
    sensors = []
    for floor in config["floors"]:
        for room in floor["rooms"]:
            for sensor_type, spec in config["sensors"].items():
                sensors.append({
                    "sensorId": f"{building}-{floor['floor']}-{room}-{sensor_type}",
                    "buildingId": building,
                    "floor": floor["floor"],
                    "room": room,
                    "zone": floor["zone"],
                    "sensorType": sensor_type,
                    "unit": spec["unit"],
                    "spec": spec,
                })
    return sensors


def occupancy_level(hour: float) -> float:
    """0 at night, close to 1 during office hours (8h-18h local)."""
    if hour < 7 or hour >= 20:
        return 0.0
    return max(0.0, math.sin(math.pi * (hour - 7) / 13))


def simulate(sensor: dict, hour: float, anomaly: dict | None) -> float:
    """A plausible value for a sensor at a given local hour."""
    level = occupancy_level(hour)
    stype = sensor["sensorType"]
    spec = sensor["spec"]

    if stype == "occupancy":
        value = round(level * random.uniform(4, 14))
    elif stype == "co2":
        value = 450 + level * 450 + random.gauss(0, 25)
    elif stype == "temperature":
        value = 20 + level * 3 + random.gauss(0, 0.3)
    elif stype == "humidity":
        value = 42 + level * 8 + random.gauss(0, 1.5)
    elif stype == "energy":
        value = 0.4 + level * 2.0 + random.gauss(0, 0.15)
    else:
        value = spec["normal"]

    if anomaly and anomaly["room"] == sensor["room"] and anomaly["sensorType"] == stype:
        value = anomaly["value"] + random.gauss(0, anomaly["value"] * 0.02)

    value = max(spec["min"], min(spec["max"], value)) if stype != "co2" else max(spec["min"], value)
    return int(value) if stype == "occupancy" else round(value, 2)


def pick_anomaly(sensors: list[dict]) -> dict:
    room = random.choice(sorted({s["room"] for s in sensors}))
    kind = random.choice([("co2", 1700), ("co2", 1200), ("temperature", 29.5)])
    return {"room": room, "sensorType": kind[0], "value": kind[1], "until": time.time() + ANOMALY_DURATION}


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] delivery failed for {msg.topic()}: {err}", flush=True)


def main():
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    sensors = load_sensors(config)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "fake-producer",
                         "linger.ms": 50, "acks": "all"})
    print(f"Fake producer -> {BOOTSTRAP} | {len(sensors)} sensors every {INTERVAL}s "
          f"| anomaly every {ANOMALY_EVERY}s", flush=True)

    last_alert: dict[tuple, float] = {}
    anomaly = None
    next_anomaly = time.time() + ANOMALY_EVERY if ANOMALY_EVERY > 0 else math.inf
    cycle = 0

    while running:
        started = time.time()
        local = datetime.now()
        hour = local.hour + local.minute / 60

        if anomaly and started > anomaly["until"]:
            print(f"[ANOMALY END] {anomaly['room']} {anomaly['sensorType']}", flush=True)
            anomaly = None
        if anomaly is None and started >= next_anomaly:
            anomaly = pick_anomaly(sensors)
            next_anomaly = started + ANOMALY_EVERY
            print(f"[ANOMALY START] {anomaly['room']} {anomaly['sensorType']} ~{anomaly['value']} "
                  f"for {ANOMALY_DURATION:.0f}s", flush=True)

        alerts_sent = 0
        for s in sensors:
            value = simulate(s, hour, anomaly)
            status, threshold = evaluate(s["sensorType"], value)
            ts = now_iso()
            message = {
                "schemaVersion": SCHEMA_VERSION,
                "messageId": str(uuid.uuid4()),
                "sensorId": s["sensorId"],
                "buildingId": s["buildingId"],
                "floor": s["floor"],
                "room": s["room"],
                "zone": s["zone"],
                "sensorType": s["sensorType"],
                "value": value,
                "unit": s["unit"],
                "status": status,
                "timestamp": ts,
                "processedAt": now_iso(),
            }
            producer.produce(TOPIC_TELEMETRY, key=s["room"], value=json.dumps(message),
                             callback=delivery_report)

            if status != "ok":
                key = (s["sensorId"], status)
                if started - last_alert.get(key, 0) >= ALERT_COOLDOWN:
                    last_alert[key] = started
                    alert = {
                        "schemaVersion": SCHEMA_VERSION,
                        "alertId": str(uuid.uuid4()),
                        "sensorId": s["sensorId"],
                        "floor": s["floor"],
                        "room": s["room"],
                        "zone": s["zone"],
                        "sensorType": s["sensorType"],
                        "value": value,
                        "threshold": threshold,
                        "severity": status,
                        "message": f"{status.capitalize()} {s['sensorType']} level in {s['room']} "
                                   f"({value} {s['unit']}, threshold {threshold})",
                        "timestamp": ts,
                    }
                    producer.produce(TOPIC_ALERTS, key=s["room"], value=json.dumps(alert),
                                     callback=delivery_report)
                    alerts_sent += 1
                    print(f"[ALERT] {alert['message']}", flush=True)

        producer.poll(0)
        cycle += 1
        if cycle % 5 == 0 or alerts_sent:
            print(f"cycle {cycle}: {len(sensors)} telemetry messages, {alerts_sent} alerts", flush=True)

        time.sleep(max(0.0, INTERVAL - (time.time() - started)))

    print("Stopping, flushing pending messages...", flush=True)
    producer.flush(10)


if __name__ == "__main__":
    main()
