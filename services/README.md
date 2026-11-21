# Person B services

All services are Python 3.12 + `confluent-kafka`, built with `./services` as Docker build context
so they can share `common/contract.py` (the interface contract in code).

| Service | Day | Reads | Writes | Start |
|---|---|---|---|---|
| `fake-producer` | 3 | `simulator/building.json` | `building.telemetry`, `building.alerts` | `docker compose --profile fake up -d` |
| `storage-consumer` | 3–4 | `building.telemetry`, `building.alerts`, `building.aggregates` | InfluxDB | `docker compose up -d` |
| `aggregator` | 6 | `building.telemetry` | `building.aggregates` | — |
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

## Grafana

Dashboard **Smart Building — Overview** is provisioned from `grafana/provisioning/dashboards/`
(folder *Smart Building*): key figures, CO2 / temperature / humidity / energy by room with
threshold bands, latest alerts, and a *Floor* filter. Edits made in the Grafana UI are kept in
Grafana only: export the JSON (Share → Export) into `overview.json` to version them.
