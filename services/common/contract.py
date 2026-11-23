"""Owner: Person B
Shared helpers that encode docs/interface-contract.md (schema version 1).
Used by the fake producer and the consumers so every service applies the same rules.
"""
from datetime import datetime, timezone

SCHEMA_VERSION = 1

TOPIC_TELEMETRY = "building.telemetry"
TOPIC_ALERTS = "building.alerts"
TOPIC_AGGREGATES = "building.aggregates"
TOPIC_INVALID = "building.invalid"

# Section 3 — thresholds. Each entry: (warning_low, warning_high, critical_low, critical_high)
# None means "no limit on that side".
THRESHOLDS = {
    "temperature": (18, 26, 16, 28),
    "humidity": (25, 65, 20, 75),
    "co2": (None, 1000, None, 1500),
    "energy": (None, 3, None, 5),
}

TELEMETRY_REQUIRED = [
    "schemaVersion", "messageId", "sensorId", "buildingId", "floor", "room", "zone",
    "sensorType", "value", "unit", "status", "timestamp", "processedAt",
]
ALERT_REQUIRED = [
    "schemaVersion", "alertId", "sensorId", "floor", "room", "zone", "sensorType",
    "value", "threshold", "severity", "message", "timestamp",
]


def now_iso() -> str:
    """Current UTC time, ISO 8601 with milliseconds and a Z suffix."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def evaluate(sensor_type: str, value: float, hour_utc: int | None = None):
    """Return (status, threshold) for a reading, following section 3 of the contract.

    status is "ok", "warning" or "critical"; threshold is the limit that was crossed (or None).
    """
    if sensor_type == "occupancy":
        hour = datetime.now(timezone.utc).hour if hour_utc is None else hour_utc
        night = hour >= 22 or hour < 6
        if night and value > 0:
            return "critical", 0
        return "ok", None

    limits = THRESHOLDS.get(sensor_type)
    if limits is None:
        return "ok", None
    warn_low, warn_high, crit_low, crit_high = limits

    if crit_high is not None and value > crit_high:
        return "critical", crit_high
    if crit_low is not None and value < crit_low:
        return "critical", crit_low
    if warn_high is not None and value > warn_high:
        return "warning", warn_high
    if warn_low is not None and value < warn_low:
        return "warning", warn_low
    return "ok", None


def missing_fields(message: dict, required: list[str]) -> list[str]:
    """Fields required by the contract that are absent from a message."""
    return [field for field in required if field not in message]
