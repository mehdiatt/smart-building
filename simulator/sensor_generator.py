
"""
Smart Building MQTT sensor simulator.
Supports realistic sensor correlations, dynamic occupancy,
energy consumption, ventilation and demo anomalies.
"""

import json
import os
import random
import time
import threading

from datetime import datetime, timezone
from pathlib import Path

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

# Dynamic sensor states
co2_states = {}
energy_states = {}

# Anomaly lifecycle
ANOMALY_DURATION = int(os.getenv("ANOMALY_DURATION", "180"))
ANOMALY_RECOVERY = int(os.getenv("ANOMALY_RECOVERY", "60"))
ANOMALY_STARTED_AT = time.monotonic()

# Sensor simulation timing
last_update_times = {}

# MQTT connection event
connection_ready = threading.Event()


# =====================================================
# TIME AND CONFIGURATION
# =====================================================

def timestamp_utc():
    """Return UTC timestamp ending with Z."""

    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
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


def anomaly_intensity():
    """
    Return anomaly intensity:
    1.0 = fully active
    0.0 = normal
    Gradual recovery after configured duration.
    """

    if ANOMALY_MODE == "none":
        return 0.0

    elapsed = time.monotonic() - ANOMALY_STARTED_AT

    if elapsed < ANOMALY_DURATION:
        return 1.0

    recovery_elapsed = elapsed - ANOMALY_DURATION

    if recovery_elapsed >= ANOMALY_RECOVERY:
        return 0.0

    return 1.0 - (
        recovery_elapsed / max(ANOMALY_RECOVERY, 1)
    )


# =====================================================
# ROOM OCCUPANCY PROFILE
# =====================================================

def occupancy_profile(room):
    """Generate different occupancy profiles for each room."""

    now = datetime.now()
    hour = now.hour

    if not is_office_hours():

        if not FORCE_DAYTIME:
            return 0.0

        profile = 0.65

    elif 8 <= hour < 9:
        profile = 0.35

    elif 9 <= hour < 12:
        profile = 0.90

    elif 12 <= hour < 14:
        profile = 0.45

    elif 14 <= hour < 17:
        profile = 0.95

    else:
        profile = 0.40

    # Stable variation between rooms
    room_factor = (
        0.75
        + (sum(ord(char) for char in room) % 6) * 0.07
    )

    return profile * room_factor


# =====================================================
# SMOOTH SENSOR VALUES
# =====================================================

def smooth_value(sensor_id, target, minimum, maximum, step):
    """Move gradually from previous value towards target."""

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


# =====================================================
# SENSOR GENERATION
# =====================================================

def generate_sensor_reading(
    sensor,
    sensor_type,
    sensor_spec,
    timestamp,
    occupancy=0
):
    """Generate realistic and correlated sensor readings."""

    minimum = sensor_spec["min"]
    maximum = sensor_spec["max"]
    normal = sensor_spec["normal"]

    sensor_id = sensor["sensorId"]
    room = sensor["room"]

    daytime = is_office_hours()

    anomaly_here = (
        ANOMALY_MODE != "none"
        and ANOMALY_ROOM in ("", room)
        and anomaly_intensity() > 0
    )

    ventilation_active = ventilation_on.get(room, False)

    ventilation_failure = (
        anomaly_here
        and ANOMALY_MODE == "ventilation_failure"
    )

    # -------------------------------------------------
    # 1. OCCUPANCY
    # -------------------------------------------------

    if sensor_type == "occupancy":

        target = DAY_OCCUPANCY * occupancy_profile(room)

        previous_values.setdefault(sensor_id, 0)

        value = smooth_value(
            sensor_id,
            target,
            minimum,
            maximum,
            max(DAY_OCCUPANCY * 0.08, 1)
        )

        value = round(value)

        # Guarantee zero occupancy outside working hours
        if not daytime and not FORCE_DAYTIME:
            value = 0
            previous_values[sensor_id] = 0

    # -------------------------------------------------
    # 2. CO2
    # -------------------------------------------------

    elif sensor_type == "co2":

        now = time.monotonic()

        previous = co2_states.get(sensor_id, normal)

        last_time = last_update_times.get(sensor_id, now)

        dt = clamp(now - last_time, 0.1, 10)

        last_update_times[sensor_id] = now

        # Occupancy contribution
        occupancy_ratio = clamp(
            occupancy / max(DAY_OCCUPANCY, 1),
            0,
            1
        )

        # CO2 production: maximum 100 ppm/min
        production = occupancy_ratio * 30

        # Natural air renewal
        removal_rate = 0.08

        # Ventilation increases CO2 removal
        if ventilation_active and not ventilation_failure:
            removal_rate = 0.8
        # CO2 anomaly intensity
        intensity = anomaly_intensity()

        anomaly_target = 0

        if anomaly_here and ANOMALY_MODE == "co2_warning":
            anomaly_target = 1100

        elif anomaly_here and ANOMALY_MODE == "co2_critical":
            anomaly_target = 1700

        elif anomaly_here and ANOMALY_MODE == "ventilation_failure":
            anomaly_target = 1700

        # Anomaly gradually pushes CO2 towards its target
        anomaly_effect = 0

        # A working ventilation system stops the critical CO2 source
        if anomaly_target > 0 and not (
            ventilation_active and not ventilation_failure
        ):
            anomaly_effect = (
                max(0, anomaly_target - previous)
                * 3.0
                * intensity
            )

        # Physical accumulation and removal
        removal = max(0, previous - 420) * removal_rate

        value = previous + (
            (production + anomaly_effect - removal)
            * dt / 60
        )


        # Small environmental variation
        value += random.uniform(-1.0, 1.0)

        value = clamp(value, minimum, maximum)

        co2_states[sensor_id] = value

        value = round(value, 2)

    # -------------------------------------------------
    # 3. TEMPERATURE
    # -------------------------------------------------

    elif sensor_type == "temperature":

        target = normal

        # Occupants generate heat
        target += occupancy * 0.045

        # Daytime thermal contribution
        if daytime:
            target += 0.35

        # Ventilation cooling effect
        if ventilation_active and not ventilation_failure:
            target -= 0.6

        # High temperature anomaly
        if anomaly_here and ANOMALY_MODE == "temperature_high":
            target = max(target, normal + 4)

        value = smooth_value(
            sensor_id,
            target,
            minimum,
            maximum,
            max((maximum - minimum) * 0.008, 0.08)
        )

        value = round(value, 2)

    # -------------------------------------------------
    # 4. HUMIDITY
    # -------------------------------------------------

    elif sensor_type == "humidity":

        target = normal + occupancy * 0.12

        # Ventilation changes indoor humidity
        if ventilation_active and not ventilation_failure:
            target -= 1.5

        # High humidity anomaly
        if anomaly_here and ANOMALY_MODE == "humidity_high":
            target = max(target, normal + 15)

        value = smooth_value(
            sensor_id,
            target,
            minimum,
            maximum,
            max((maximum - minimum) * 0.003, 0.1)
        )

        value = round(value, 2)

    # -------------------------------------------------
    # 5. ENERGY CONSUMPTION
    # -------------------------------------------------

    elif sensor_type == "energy":

        # Occupancy ratio
        occupancy_ratio = clamp(
            occupancy / max(DAY_OCCUPANCY, 1),
            0,
            1
        )

        # Base standby consumption
        target = minimum + 0.35

        # Normal daytime operation
        if daytime:
            target += 0.70

        # Equipment load according to occupancy
        target += occupancy_ratio * 1.2

        # Ventilation electricity consumption
        if ventilation_active:
            target += 0.25

        # Equipment load variation
        hour = datetime.now().hour

        if 9 <= hour < 12:
            load_factor = 1.0

        elif 12 <= hour < 14:
            load_factor = 0.75

        elif 14 <= hour < 17:
            load_factor = 1.05

        else:
            load_factor = 0.65

        target *= load_factor

        # Persistent energy anomaly
        intensity = anomaly_intensity()

        if anomaly_here and ANOMALY_MODE in (
            "energy_spike",
            "energy_overconsumption"
        ):
            target += intensity * (maximum * 0.85 - target)

        target = clamp(target, minimum, maximum)

        # Gradual transition with small fluctuations
        value = smooth_value(
            sensor_id,
            target,
            minimum,
            maximum,
            max((maximum - minimum) * 0.025, 0.05)
        )

        value = round(value, 2)

    # -------------------------------------------------
    # 6. OTHER SENSOR TYPES
    # -------------------------------------------------

    else:

        value = smooth_value(
            sensor_id,
            normal,
            minimum,
            maximum,
            max((maximum - minimum) * 0.005, 0.1)
        )

        value = round(value, 2)

    # -------------------------------------------------
    # STANDARD JSON MESSAGE
    # -------------------------------------------------

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


# =====================================================
# GENERATE ALL BUILDING READINGS
# =====================================================

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

            # Generate occupancy first
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


# =====================================================
# MQTT STATUS
# =====================================================

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


# =====================================================
# MQTT CONNECTION CALLBACK
# =====================================================

def on_connect(client, userdata, flags, reason_code, properties):
    """Handle MQTT connection events."""

    if reason_code.is_failure:
        print(f"MQTT connection failed: {reason_code}")
        return

    publish_status(client, "online")

    client.subscribe(COMMAND_TOPIC, qos=1)

    connection_ready.set()

    print("Simulator connected")


# =====================================================
# MQTT VENTILATION COMMANDS
# =====================================================

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


# =====================================================
# MQTT SENSOR PUBLICATION
# =====================================================

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


# =====================================================
# MAIN SIMULATOR
# =====================================================

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
