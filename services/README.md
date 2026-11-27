# Person B services

All services are Python 3.12 + `confluent-kafka`, built with `./services` as Docker build context
so they can share `common/contract.py` (the interface contract in code).

| Service | Day | Reads | Writes | Start |
|---|---|---|---|---|
| `fake-producer` | 3 | `simulator/building.json` | `building.telemetry`, `building.alerts` | `docker compose --profile fake up -d` |
| `storage-consumer` | 3–4 | `building.telemetry`, `building.alerts`, `building.aggregates` | InfluxDB | `docker compose up -d` |
| `aggregator` | 6 | `building.telemetry` | `building.aggregates` | `docker compose up -d` |
| `notifier` | 8 | `building.alerts` | Telegram (or console in DRY-RUN) | `docker compose up -d` |

## Fake producer

Sends 60 contract-compliant readings every 2 s (key = `room`), with a realistic day/night
pattern. Every 60 s one random room gets a CO2 spike or overheating for 20 s, which produces
alerts (max one per sensor and severity every 60 s). Tune it with the environment variables in
`docker-compose.yml` (`INTERVAL_SECONDS`, `ANOMALY_EVERY_SECONDS`, `ANOMALY_DURATION_SECONDS`).

Stop using it on day 5, once Node-RED produces the real data: start the stack without
`--profile fake`.

## Storage consumer

Consumer group `storage`. Writes `telemetry`, `alerts` and `aggregates` measurements to InfluxDB
(contract section 4). Numbers are always stored as floats so a field never changes type.

**No data loss:** auto-commit is off; Kafka offsets are committed only after InfluxDB confirmed
the write. If InfluxDB is down, the batch is retried (1 s, 2 s, 4 s … up to 30 s).
Messages that break the contract are logged as `[CONTRACT] ... missing=[...]` and skipped.

**Pipeline latency:** for each telemetry message written, the consumer measures
`ingest` (sensor → Node-RED, `processedAt - timestamp`), `pipeline` (Node-RED → InfluxDB) and
`total` (end to end). Every 10 s it writes avg / p95 / max and the throughput to the InfluxDB
measurement `pipeline_latency` and prints them in its logs (`latency ms: total avg=… p95=…`).

```bash
docker compose logs -f storage-consumer
```

## Aggregator

Consumer group `aggregator`. Computes **1-minute tumbling windows per room + sensorType**
(avg, min, max, count) and publishes them to `building.aggregates` (contract §2.3, key = room).
The storage consumer then writes them to the InfluxDB measurement `aggregates`.

- **Event time:** a reading goes into the window of its own `timestamp`, not its arrival time,
  so replaying old data gives the same result.
- **Closing:** a window is published once `windowEnd + 5 s` is behind the watermark (the oldest
  latest-timestamp among active partitions). With no data for 10 s, the wall clock is used, so
  the last windows are still published when the simulator stops.
- **Late readings** (after their window was published) are counted as `late` and ignored.
- **Restart safety:** offsets are committed manually, at the oldest message still belonging to an
  open window. After a crash, Kafka replays from there: open windows are rebuilt, nothing is lost
  and no window is published twice.

```bash
docker compose logs -f aggregator
```

## Notifier

Consumer group `notifier`. Sends every alert of `building.alerts` to a Telegram chat
(🔴 critical / 🟠 warning, room, value, threshold, floor, zone, local time).

- **DRY-RUN:** without `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`, messages are only
  printed in the logs, so the stack still works on any PC.
- **No flood:** first start begins at the end of the topic; alerts older than 5 min are not sent;
  more than 3 alerts at once are grouped into one message.
- **At-least-once:** the Kafka offset is committed after Telegram accepted the message; network
  errors are retried (1 s → 60 s). A wrong token / chat id is reported clearly and not retried.

Setup: create a bot with **@BotFather** in Telegram, put the token in `.env`, send "hello" to the
bot, then `docker compose run --rm notifier python notifier.py --find-chat-id` and copy the
chat id into `.env`. Restart with `docker compose up -d notifier`.

```bash
docker compose logs -f notifier
```

## Grafana

Dashboard **Smart Building — Overview** is provisioned from `grafana/provisioning/dashboards/`
(folder *Smart Building*): key figures (messages, rooms, warnings, critical alerts, latency p95,
throughput), CO2 / temperature / humidity / energy by room on fixed scales with threshold bands,
1-minute CO2 averages from the aggregator, pipeline latency, latest alerts, alert annotations
(red = critical, orange = warning, toggles at the top) and a *Floor* filter. Edits made in the Grafana UI are kept in
Grafana only: export the JSON (Share → Export) into `overview.json` to version them.
