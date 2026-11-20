
"""
Generate and publish simulated sensor readings using MQTT.
"""

import json
import random
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt


CONFIG_PATH = Path(__file__).parent / "building.json"

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_QOS = 1
PUBLISH_INTERVAL = 2


def load_config():
    """Load the building configuration."""
    with CONFIG_PATH.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)


def generate_sensor_reading(sensor, sensor_type, sensor_spec, timestamp):
    """Generate one realistic reading for a sensor."""
    minimum = sensor_spec["min"]
    maximum = sensor_spec["max"]
    normal = sensor_spec["normal"]

    if sensor_type == "occupancy":
        value = round(random.gauss(normal, (maximum - minimum) / 6))
        value = max(minimum, min(maximum, value))
    else:
        standard_deviation = (maximum - minimum) / 6
        value = random.gauss(normal, standard_deviation)
        value = max(minimum, min(maximum, value))
        value = round(value, 2)

    return {
        "sensorId": sensor["sensorId"],
        "sensorType": sensor_type,
        "topic": sensor["topic"],
        "value": value,
        "unit": sensor_spec["unit"],
        "timestamp": timestamp
    }


def generate_readings(config):
    """Generate readings for every sensor in the building."""
    building_id = config["buildingId"]
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    readings = []

    for floor in config["floors"]:
        for room in floor["rooms"]:
            for sensor_type, sensor_spec in config["sensors"].items():
                sensor = {
                    "sensorId": f"{building_id}-{floor['floor']}-{room}-{sensor_type}",
                    "topic": f"building/{building_id}/{floor['floor']}/{room}/{sensor_type}"
                }

                reading = generate_sensor_reading(
                    sensor,
                    sensor_type,
                    sensor_spec,
                    timestamp
                )

                readings.append(reading)

    return readings


def on_connect(client, userdata, flags, reason_code, properties):
    """Handle MQTT connection result."""
    if reason_code.is_failure:
        print(f"MQTT connection failed: {reason_code}")
    else:
        print(f"Connected to MQTT broker: {MQTT_BROKER}:{MQTT_PORT}")


def publish_readings(client, readings):
    """Publish all sensor readings to MQTT."""
    for reading in readings:
        topic = reading["topic"]
        payload = json.dumps(reading, ensure_ascii=False)

        result = client.publish(
            topic,
            payload,
            qos=MQTT_QOS
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"Failed to publish to {topic}: {result.rc}")
            continue

        result.wait_for_publish(timeout=5)

        if result.is_published():
            print(f"Published: {topic} -> {reading['value']} {reading['unit']}")
        else:
            print(f"Publish timeout: {topic}")


def main():
    """Connect to MQTT and publish sensor readings continuously."""
    config = load_config()

    connection_ready = threading.Event()

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="smart-building-simulator"
    )

    def handle_connect(client, userdata, flags, reason_code, properties):
        on_connect(client, userdata, flags, reason_code, properties)
        if not reason_code.is_failure:
            connection_ready.set()

    client.on_connect = handle_connect

    try:
        print(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT}...")

        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_start()

        if not connection_ready.wait(timeout=10):
            raise RuntimeError("Could not establish MQTT connection.")

        print("Starting sensor simulation...")

        while True:
            readings = generate_readings(config)

            publish_readings(client, readings)

            print(f"Published cycle: {len(readings)} sensor readings.")
            print(f"Next cycle in {PUBLISH_INTERVAL} seconds.\n")

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping sensor simulator...")

    finally:
        if client.is_connected():
            client.disconnect()

        client.loop_stop()
        print("MQTT simulator stopped.")


if __name__ == "__main__":
    main()
