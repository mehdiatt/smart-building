# Interface Contract — Smart Building Monitoring

**Schema version: 1** — approved on: 20 / 11 / 2026

| Sign-off | Person A (ingestion & processing) | Person B (streaming & analytics) |
|---|---|---|
| Name | | |

> This document is the **only boundary** between A and B. Any change is made **by both**:
> 1. notify the other person, 2. update this file, 3. bump `schemaVersion`.
> Rule: fields may be **added**, never **removed**, during the project.

---

## 0. Scenario

- Building **B1**, **3 floors**, **4 rooms per floor** → 12 rooms.
- **5 sensors per room** → **60 sensors**, one reading every **2 s** (≈ 30 msg/s).

| Floor | Rooms | Zone |
|---|---|---|
| 1 | R101, R102, R103, R104 | reception |
| 2 | R201, R202, R203, R204 | offices |
| 3 | R301, R302, R303, R304 | meeting |

Sensor ID: `{buildingId}-{floor}-{room}-{sensorType}` → e.g. `B1-2-R203-co2`.

---

## 1. MQTT (Person A's scope)

| Topic | Direction | Example |
|---|---|---|
| `building/{building}/{floor}/{room}/{sensorType}` | Simulator → Node-RED | `building/B1/2/R203/co2` |
| `cmd/{building}/{floor}/{room}/{device}` | Node-RED → Simulator | `cmd/B1/2/R203/ventilation` |
| `status/simulator` | Last Will (retained) | `online` / `offline` |

- QoS **1** for readings and commands.
- Commands live under `cmd/` so they are not caught by the `building/#` subscription.

### 1.1 Raw reading (MQTT)

```json
{
  "sensorId":   "B1-2-R203-co2",
  "buildingId": "B1",
  "floor":      2,
  "room":       "R203",
  "sensorType": "co2",
  "value":      845.2,
  "unit":       "ppm",
  "timestamp":  "2026-11-20T15:02:11.123Z"
}
```

### 1.2 Command (MQTT)

```json
{ "action": "on", "reason": "co2_critical", "timestamp": "2026-11-20T15:04:02.050Z" }
```
`action` ∈ `on` | `off`.

---

## 2. Kafka (A ↔ B boundary)

Brokers: `kafka:29092` from inside a container, `localhost:9092` from the host machine.

| Topic | Partitions | Key | Producer | Consumers (group.id) |
|---|---|---|---|---|
| `building.telemetry` | 3 | `room` | A (Node-RED) | `storage`, `aggregator` |
| `building.alerts` | 1 | `room` | A (Node-RED) | `notifier`, `storage` |
| `building.aggregates` | 3 | `room` | B (aggregator) | `storage` |
| `building.invalid` | 1 | none | A (Node-RED) | manual inspection (Kafka UI) |

Retention: 24 h. Values are **UTF-8 JSON**, keys are plain text.
All timestamps are **ISO 8601 UTC** (`Z` suffix).

### 2.1 `building.telemetry` — enriched message

```json
{
  "schemaVersion": 1,
  "messageId":   "6f1c2a9e-0b7d-4c55-9a1e-2f0c7b1d9e33",
  "sensorId":    "B1-2-R203-co2",
  "buildingId":  "B1",
  "floor":       2,
  "room":        "R203",
  "zone":        "offices",
  "sensorType":  "co2",
  "value":       845.2,
  "unit":        "ppm",
  "status":      "ok",
  "timestamp":   "2026-11-20T15:02:11.123Z",
  "processedAt": "2026-11-20T15:02:11.140Z"
}
```
- `status` ∈ `ok` | `warning` | `critical` (computed from the thresholds in §3).
- `timestamp` = time of the reading; `processedAt` = time it went through Node-RED.

### 2.2 `building.alerts`

```json
{
  "schemaVersion": 1,
  "alertId":    "a7d0c1e2-...",
  "sensorId":   "B1-2-R203-co2",
  "floor": 2, "room": "R203", "zone": "offices",
  "sensorType": "co2",
  "value":      1620,
  "threshold":  1500,
  "severity":   "critical",
  "message":    "Critical CO2 level in R203 (1620 ppm > 1500)",
  "timestamp":  "2026-11-20T15:04:02.010Z"
}
```
Anti-spam: at most **one alert per sensor and per severity every 60 s**.

### 2.3 `building.aggregates`

```json
{
  "schemaVersion": 1,
  "floor": 2, "room": "R203", "sensorType": "co2",
  "windowStart": "2026-11-20T15:04:00Z",
  "windowEnd":   "2026-11-20T15:05:00Z",
  "avg": 912.4, "min": 845.2, "max": 1620.0, "count": 30
}
```
Fixed (tumbling) **1-minute** windows, per `room` + `sensorType`, 5 s allowed lateness.

### 2.4 `building.invalid`

The original message, unchanged, with extra fields:
```json
{ "...": "original message", "error": "value is not a number", "receivedAt": "2026-11-20T15:02:11.140Z" }
```
Rejected when: a required field is missing (`sensorId`, `room`, `sensorType`, `value`, `timestamp`),
`value` is not numeric or physically impossible, `sensorType` is unknown, or the JSON cannot be parsed.

---

## 3. Sensors and thresholds

| sensorType | Unit | Simulated range | Warning | Critical |
|---|---|---|---|---|
| `temperature` | °C | 16 – 30 | > 26 or < 18 | > 28 or < 16 |
| `humidity` | % | 20 – 80 | > 65 or < 25 | > 75 or < 20 |
| `co2` | ppm | 400 – 2000 | > 1000 | > 1500 |
| `occupancy` | people | 0 – 20 | — | > 0 between 10 pm and 6 am |
| `energy` | kW | 0 – 6 | > 3 | > 5 |

Physical limits (outside → `building.invalid`): temperature −40…80, humidity 0…100,
CO2 0…10000, occupancy 0…200 (integer), energy 0…100.

---

## 4. InfluxDB (Person B's scope)

Org `smartbuilding`, bucket `building`.

| Measurement | Tags | Fields |
|---|---|---|
| `telemetry` | building, floor, room, zone, sensorType, status | value |
| `aggregates` | floor, room, sensorType | avg, min, max, count |
| `alerts` | floor, room, sensorType, severity | value, threshold, message |

---

## Change log

| Version | Date | Who | Change |
|---|---|---|---|
| 1 | | A + B | Initial version |
