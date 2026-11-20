"""Owner: Person A
Day 1: checks the building configuration and prints the 60 sensors
(ID + MQTT topic), as defined in the interface contract.
The actual simulator (MQTT publishing) will be built on day 3 from this file.

Usage: python simulator/list_sensors.py
"""
import json
from pathlib import Path

config = json.loads((Path(__file__).parent / "building.json").read_text(encoding="utf-8"))
b = config["buildingId"]

sensors = []
for f in config["floors"]:
    for room in f["rooms"]:
        for stype, spec in config["sensors"].items():
            sensors.append({
                "sensorId": f"{b}-{f['floor']}-{room}-{stype}",
                "topic": f"building/{b}/{f['floor']}/{room}/{stype}",
                "zone": f["zone"],
                "unit": spec["unit"],
            })

for s in sensors:
    print(f"{s['sensorId']:<24} {s['topic']:<34} {s['zone']:<10} {s['unit']}")

rate = len(sensors) / config["publishIntervalSeconds"]
print(f"\n{len(sensors)} sensors, about {rate:.0f} messages/s")
