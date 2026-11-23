
"""
Smart Building MQTT sensor simulator.
Supports smooth readings, day/night profiles and demo anomalies.
"""

import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
import threading

import paho.mqtt.client as mqtt


CONFIG_PATH = Path(__file__).parent / "building.json"

MQTT_BROKER = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS = 1
PUBLISH_INTERVAL = 2

STATUS_TOPIC = "status/simulator"

# Anomaly configuration
ANOMALY_MODE = os.getenv("ANOMALY_MODE", "none")
ANOMALY_ROOM = os.getenv("ANOMALY_ROOM", "")

# Occupancy configuration
DAY_OCCUPANCY = int(os.getenv("DAY_OCCUPANCY", "8"))
FORCE_DAYTIME = os.getenv("FORCE_DAYTIME", "0") == "1"

# Ventilation configuration
COMMAND_TOPIC = "cmd/#"
VENTILATION_TARGET = 450
VENTILATION_STEP = 40

# Room ventilation states
ventilation_on = {}

# Office schedule: Monday-Friday, 08:00-18:00
OFFICE_START = 8
OFFICE_END = 18

# Previous values, indexed by sensor ID
previous_values = {}

# MQTT connection event
connection_ready = threading.Event()


def timestamp_utc():
    """Return UTC timestamp ending with Z."""
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def load_config():
    """Load the building configuration."""
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def is_office_hours():
    """Check office hours or force daytime for tests."""
    if FORCE_DAYTIME:
        return True

    now = datetime.now()

    return (
        now.weekday() < 5
        and OFFICE_START <= now.hour < OFFICE_END
    )


def clamp(value, minimum, maximum):
    """Keep a value within its allowed range."""
    return max(minimum, min(maximum, value))


def smooth_value(sensor_id, target, minimum, maximum, step):
    """Move gradually from the previous value towards the target."""
    previous = previous_values.get(sensor_id, target)

    difference = target - previous
    movement = clamp(difference, -step, step)

    value = (
        previous
        + movement
        + random.uniform(-step * 0.2, step * 0.2)
    )

    value = clamp(value, minimum, maximum)

    previous_values[sensor_id] = value

    return value


def generate_sensor_reading(
    sensor,
    sensor_type,
    sensor_spec,
    timestamp,
    occupancy=0
):
    """Generate one sensor reading."""

    minimum = sensor_spec["min"]
    maximum = sensor_spec["max"]
    normal = sensor_spec["normal"]

    sensor_id = sensor["sensorId"]
    daytime = is_office_hours()

    # -----------------------------------------
    # OCCUPANCY SENSOR
    # -----------------------------------------
    if sensor_type == "occupancy":

        target = DAY_OCCUPANCY if daytime else 0

        value = smooth_value(
            sensor_id,
            target,
            0,
            maximum,
            max(1, DAY_OCCUPANCY / 5)
        )

        value = round(value)

        # Guarantee zero occupancy at night
        if not daytime:
            value = 0
            previous_values[sensor_id] = 0

    # -----------------------------------------
    # CO2 SENSOR
    # -----------------------------------------
    elif sensor_type == "co2":

        # CO2 increases with the number of occupants
        target = normal + occupancy * 15
        step = 12

        # Apply anomalies only to the selected room
        anomaly_here = (
            ANOMALY_MODE != "none"
            and ANOMALY_ROOM in ("", sensor["room"])
        )

        if anomaly_here and ANOMALY_MODE == "co2_warning":
            target = max(target, 1100)
            step = 80

        elif anomaly_here and ANOMALY_MODE == "co2_critical":
            target = max(target, 1700)
            step = 80

        # Ventilation overrides anomalies
        if ventilation_on.get(sensor["room"], False):
            target = VENTILATION_TARGET
            step = VENTILATION_STEP

        value = smooth_value(
            sensor_id,
            target,
            minimum,
            maximum,
            step
        )

        value = round(value, 2)

    # -----------------------------------------
    # OTHER SENSORS
    # -----------------------------------------
    else:

        target = normal

        value = smooth_value(
            sensor_id,
            target,
            minimum,
            maximum,
            max((maximum - minimum) * 0.005, 0.1)
        )

        value = round(value, 2)

    return {
        "sensorId": sensor["sensorId"],
        "buildingId": sensor["buildingId"],
        "floor": sensor["floor"],
        "room": sensor["room"],
        "sensorType": sensor_type,
        "topic": sensor["topic"],
        "value": value,
        "unit": sensor_spec["unit"],
        "timestamp": timestamp
    }


def generate_readings(config):
    """Generate readings for every configured sensor."""

    building_id = config["buildingId"]
    timestamp = timestamp_utc()

    readings = []

    for floor in config["floors"]:

        for room in floor["rooms"]:

            room_sensors = {}

            for sensor_type, sensor_spec in config["sensors"].items():

                sensor_id = (
                    f"{building_id}-{floor['floor']}-"
                    f"{room}-{sensor_type}"
                )

                sensor = {
                    "sensorId": sensor_id,
                    "buildingId": building_id,
                    "floor": floor["floor"],
                    "room": room,
                    "topic": (
                        f"building/{building_id}/"
                        f"{floor['floor']}/{room}/{sensor_type}"
                    )
                }

                room_sensors[sensor_type] = (
                    sensor,
                    sensor_spec
                )

            occupancy = 0

            # Generate occupancy before other sensors
            if "occupancy" in room_sensors:

                sensor, spec = room_sensors["occupancy"]

                occupancy_reading = generate_sensor_reading(
                    sensor,
                    "occupancy",
                    spec,
                    timestamp
                )

                occupancy = occupancy_reading["value"]

                readings.append(occupancy_reading)

            # Generate all remaining sensors
            for sensor_type, (sensor, spec) in room_sensors.items():

                if sensor_type == "occupancy":
                    continue

                reading = generate_sensor_reading(
                    sensor,
                    sensor_type,
                    spec,
                    timestamp,
                    occupancy
                )

                readings.append(reading)

    return readings


def publish_status(client, status):
    """Publish simulator connection status."""

    payload = json.dumps({
        "status": status,
        "timestamp": timestamp_utc()
    })

    client.publish(
        STATUS_TOPIC,
        payload,
        qos=1,
        retain=True
    )


def on_connect(client, userdata, flags, reason_code, properties):
    """Handle MQTT connection events."""

    if reason_code.is_failure:
        print(f"MQTT connection failed: {reason_code}")
        return

    publish_status(client, "online")

    client.subscribe(COMMAND_TOPIC, qos=1)

    connection_ready.set()

    print("Simulator connected")


def on_message(client, userdata, message):
    """Apply ventilation commands."""

    # Expected topic:
    # cmd/{building}/{floor}/{room}/ventilation

    parts = message.topic.split("/")

    if (
        len(parts) != 5
        or parts[0] != "cmd"
        or parts[4] != "ventilation"
    ):
        print(f"[CMD] Ignored topic: {message.topic}")
        return

    try:
        command = json.loads(message.payload)

        action = command["action"]

    except (ValueError, KeyError, TypeError):
        print(
            f"[CMD] Invalid payload on {message.topic}: "
            f"{message.payload!r}"
        )
        return

    if action not in ("on", "off"):
        print(
            f"[CMD] Unknown action {action!r} "
            f"on {message.topic}"
        )
        return

    room = parts[3]

    ventilation_on[room] = (action == "on")

    print(
        f"[CMD] {room} ventilation {action.upper()} "
        f"({command.get('reason', '-')})"
    )


def publish_readings(client, readings):
    """Publish all sensor readings to MQTT."""

    for reading in readings:

        result = client.publish(
            reading["topic"],
            json.dumps(reading),
            qos=MQTT_QOS
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:

            print(
                f"Publish failed: {reading['topic']} "
                f"| rc={result.rc} "
                f"| connected={client.is_connected()}"
            )


def main():
    """Start the MQTT sensor simulator."""

    config = load_config()

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="smart-building-simulator"
    )

    client.on_connect = on_connect
    client.on_message = on_message

    client.will_set(
        STATUS_TOPIC,
        json.dumps({
            "status": "offline",
            "timestamp": timestamp_utc()
        }),
        qos=1,
        retain=True
    )

    try:

        client.connect(
            MQTT_BROKER,
            MQTT_PORT,
            keepalive=60
        )

        client.loop_start()

        if not connection_ready.wait(timeout=10):
            raise RuntimeError("MQTT connection timeout")

        print(
            f"Simulator started | "
            f"anomaly={ANOMALY_MODE} | "
            f"room={ANOMALY_ROOM or 'all'} | "
            f"daytime={FORCE_DAYTIME}"
        )

        while True:

            readings = generate_readings(config)

            publish_readings(client, readings)

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:

        print("Stopping simulator...")

    finally:

        if client.is_connected():

            publish_status(client, "offline")

            client.disconnect()

        client.loop_stop()

        print("Simulator stopped")


if __name__ == "__main__":
    main()
