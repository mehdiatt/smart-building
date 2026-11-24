"""Owner: Person B — Day 8
Notifier: reads building.alerts and sends each alert to a Telegram chat.

Without Telegram settings it runs in DRY-RUN mode and only prints the messages,
so the stack still starts on a PC where nobody configured a bot.

Behaviour
- Only recent alerts are sent (MAX_AGE_SECONDS): after a restart or a long stop we do not
  flood the phone with old alerts. On the very first start the group begins at the end of the
  topic (auto.offset.reset = latest).
- When several alerts arrive at once (more than GROUP_THRESHOLD), they are grouped into one
  message, to stay under Telegram's rate limits.
- At-least-once: the Kafka offset is committed only after Telegram accepted the message.
  If Telegram or the network is down, the message is retried (1 s, 2 s, 4 s ... 60 s).

Environment variables
  KAFKA_BOOTSTRAP      default kafka:29092
  GROUP_ID             default notifier
  TELEGRAM_BOT_TOKEN   token given by @BotFather (empty = DRY-RUN)
  TELEGRAM_CHAT_ID     chat that receives the alerts (empty = DRY-RUN)
  MAX_AGE_SECONDS      default 300
  GROUP_THRESHOLD      default 3
  TZ                   default Europe/Paris (time zone used in the messages)
  TELEGRAM_FORCE_IPV4  default 1: reach Telegram over IPv4 only (some networks have a broken
                       IPv6 route: the connection opens but no answer ever comes back)

Helper to find your chat id (send any message to your bot first):
  docker compose run --rm notifier python notifier.py --find-chat-id
"""
import html
import json
import os
import signal
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
sys.path.insert(0, "/app/common")
from contract import ALERT_REQUIRED, TOPIC_ALERTS, missing_fields  # noqa: E402

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
GROUP_ID = os.getenv("GROUP_ID", "notifier")
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
MAX_AGE = float(os.getenv("MAX_AGE_SECONDS", "300"))
GROUP_THRESHOLD = int(os.getenv("GROUP_THRESHOLD", "3"))
API = "https://api.telegram.org/bot{token}/{method}"
LOCAL_TZ = ZoneInfo(os.getenv("TZ", "Europe/Paris"))   # time shown in the messages

DRY_RUN = not (TOKEN and CHAT_ID)

if os.getenv("TELEGRAM_FORCE_IPV4", "1") == "1":
    # Only affects Python's own connections (Telegram); Kafka uses librdkafka and is untouched.
    _getaddrinfo = socket.getaddrinfo

    def _ipv4_only(host, port, family=0, *args, **kwargs):
        return _getaddrinfo(host, port, socket.AF_INET, *args, **kwargs)

    socket.getaddrinfo = _ipv4_only

ICONS = {"critical": "🔴", "warning": "🟠"}
LABELS = {"temperature": "Temperature", "humidity": "Humidity", "co2": "CO₂",
          "occupancy": "Occupancy", "energy": "Energy"}
UNITS = {"temperature": "°C", "humidity": "%", "co2": "ppm", "occupancy": "people", "energy": "kW"}

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


# ---------------------------------------------------------------------------- Telegram
def telegram(method: str, params: dict) -> dict:
    """Call the Telegram Bot API and return the decoded JSON answer."""
    url = API.format(token=TOKEN, method=method)
    data = urllib.parse.urlencode(params).encode()
    with urllib.request.urlopen(url, data=data, timeout=10) as resp:
        return json.loads(resp.read())


def send(text: str) -> None:
    """Send a message, retrying until it works (or the service is stopped)."""
    if DRY_RUN:
        print("[DRY-RUN] " + text.replace("\n", " | "), flush=True)
        return
    delay = 1
    while True:
        try:
            answer = telegram("sendMessage", {"chat_id": CHAT_ID, "text": text,
                                              "parse_mode": "HTML",
                                              "disable_web_page_preview": "true"})
            if answer.get("ok"):
                return
            raise RuntimeError(answer.get("description", "unknown error"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")[:200]
            if exc.code == 429:                                  # rate limited: wait as asked
                retry = json.loads(body or "{}").get("parameters", {}).get("retry_after", delay)
                print(f"[TELEGRAM] rate limited, waiting {retry}s", flush=True)
                time.sleep(float(retry))
                continue
            if exc.code in (400, 401, 403, 404):                 # wrong token / chat id: retrying won't help
                print(f"[TELEGRAM ERROR] {exc.code} {body} - check TELEGRAM_BOT_TOKEN and "
                      f"TELEGRAM_CHAT_ID in .env. Message dropped.", flush=True)
                return
            print(f"[TELEGRAM ERROR] HTTP {exc.code} {body} - retry in {delay}s", flush=True)
        except Exception as exc:  # noqa: BLE001 — network errors: retry
            print(f"[TELEGRAM ERROR] {exc.__class__.__name__}: {exc} - retry in {delay}s", flush=True)
        if not running:
            return
        time.sleep(delay)
        delay = min(delay * 2, 60)


def find_chat_id() -> None:
    """Print the chat ids that recently wrote to the bot."""
    if not TOKEN:
        sys.exit("Set TELEGRAM_BOT_TOKEN in .env first.")
    answer = telegram("getUpdates", {})
    chats = {}
    for update in answer.get("result", []):
        message = update.get("message") or update.get("channel_post") or {}
        chat = message.get("chat")
        if chat:
            chats[chat["id"]] = chat.get("title") or chat.get("first_name") or chat.get("username")
    if not chats:
        print("No message found. Open your bot in Telegram, send it 'hello', then run this again.")
    for chat_id, name in chats.items():
        print(f"TELEGRAM_CHAT_ID={chat_id}    ({name})")


# ---------------------------------------------------------------------------- formatting
def local_time(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(LOCAL_TZ).strftime("%H:%M:%S")
    except ValueError:
        return iso


def fmt_value(sensor_type: str, value) -> str:
    unit = UNITS.get(sensor_type, "")
    if isinstance(value, float) and not value.is_integer():
        return f"{value:.1f} {unit}".strip()
    return f"{int(value)} {unit}".strip() if isinstance(value, (int, float)) else f"{value} {unit}"


def format_one(a: dict) -> str:
    icon = ICONS.get(a["severity"], "⚠️")
    label = LABELS.get(a["sensorType"], a["sensorType"])
    lines = [
        f"{icon} <b>{a['severity'].upper()}</b> — {html.escape(label)} in <b>{html.escape(a['room'])}</b>",
        f"Value: <b>{fmt_value(a['sensorType'], a['value'])}</b>"
        + (f" (threshold {fmt_value(a['sensorType'], a['threshold'])})" if a.get("threshold") is not None else ""),
        f"Floor {a['floor']} · {html.escape(str(a['zone']))} zone · {local_time(a['timestamp'])}",
    ]
    return "\n".join(lines)


def format_group(alerts: list[dict]) -> str:
    critical = sum(1 for a in alerts if a["severity"] == "critical")
    head = f"🚨 <b>{len(alerts)} alerts</b> ({critical} critical, {len(alerts) - critical} warning)"
    rows = [f"{ICONS.get(a['severity'], '⚠️')} {html.escape(a['room'])} · "
            f"{html.escape(LABELS.get(a['sensorType'], a['sensorType']))} "
            f"{fmt_value(a['sensorType'], a['value'])} · {local_time(a['timestamp'])}"
            for a in alerts[:15]]
    if len(alerts) > 15:
        rows.append(f"… and {len(alerts) - 15} more")
    return head + "\n" + "\n".join(rows)


def age_seconds(a: dict) -> float:
    try:
        ts = datetime.fromisoformat(a["timestamp"].replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------- main loop
def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",        # first start: do not send the whole history
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_ALERTS])
    mode = "DRY-RUN (no Telegram settings)" if DRY_RUN else f"Telegram chat {CHAT_ID}"
    print(f"Notifier <- {TOPIC_ALERTS} (group={GROUP_ID}) -> {mode} | max age {MAX_AGE:.0f}s", flush=True)

    if not DRY_RUN:
        send("✅ <b>Smart Building</b>: alert service started.")

    try:
        while running:
            messages = consumer.consume(num_messages=50, timeout=1.0)
            alerts = []
            for msg in messages:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[KAFKA ERROR] {msg.error()}", flush=True)
                    continue
                try:
                    data = json.loads(msg.value())
                except (TypeError, ValueError):
                    print(f"[NOT JSON] {msg.value()[:120]!r}", flush=True)
                    continue
                missing = missing_fields(data, ALERT_REQUIRED)
                if missing:
                    print(f"[CONTRACT] alert missing={missing} - skipped", flush=True)
                    continue
                age = age_seconds(data)
                if age > MAX_AGE:
                    print(f"[OLD] {data['room']} {data['sensorType']} {data['severity']} "
                          f"({age:.0f}s old) - not sent", flush=True)
                    continue
                alerts.append(data)

            if alerts:
                if len(alerts) > GROUP_THRESHOLD:
                    send(format_group(alerts))
                else:
                    for a in alerts:
                        send(format_one(a))
                        time.sleep(0.2)
                print(f"[SENT] {len(alerts)} alert(s): "
                      + ", ".join(f"{a['room']} {a['sensorType']} {a['severity']}" for a in alerts),
                      flush=True)
            if messages:
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        print("Notifier stopped.", flush=True)


if __name__ == "__main__":
    if "--find-chat-id" in sys.argv:
        find_chat_id()
    else:
        main()
