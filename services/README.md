# Person B services

All services are Python 3.12 + `confluent-kafka`, built with `./services` as Docker build context
so they can share `common/contract.py` (the interface contract in code).

| Service | Day | Reads | Writes | Start |
|---|---|---|---|---|
| `fake-producer` | 3 | `simulator/building.json` | `building.telemetry`, `building.alerts` | `docker compose --profile fake up -d` |
| `storage-consumer` | 3–4 | `building.telemetry`, `building.alerts`, `building.aggregates` | InfluxDB | `docker compose up -d` |
| `aggregator` | 6 | `building.telemetry` | `building.aggregates` | `docker compose up -d` |
| `notifier` | 8 | `building.alerts` | Telegram / email | — |

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

## Grafana

Dashboard **Smart Building — Overview** is provisioned from `grafana/provisioning/dashboards/`
(folder *Smart Building*): key figures, CO2 / temperature / humidity / energy by room with
threshold bands, latest alerts, and a *Floor* filter. Edits made in the Grafana UI are kept in
Grafana only: export the JSON (Share → Export) into `overview.json` to version them.
