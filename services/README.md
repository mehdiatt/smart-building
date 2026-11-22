# Person B services

All services are Python 3.12 + `confluent-kafka`, built with `./services` as Docker build context
so they can share `common/contract.py` (the interface contract in code).

| Service | Day | Reads | Writes | Start |
|---|---|---|---|---|
| `fake-producer` | 3 | `simulator/building.json` | `building.telemetry`, `building.alerts` | `docker compose --profile fake up -d` |
| `storage-consumer` | 3–4 | `building.telemetry`, `building.alerts` | console (day 3), InfluxDB (day 4) | `docker compose up -d` |
| `aggregator` | 6 | `building.telemetry` | `building.aggregates` | — |
| `notifier` | 8 | `building.alerts` | Telegram / email | — |

## Fake producer

Sends 60 contract-compliant readings every 2 s (key = `room`), with a realistic day/night
pattern. Every 60 s one random room gets a CO2 spike or overheating for 20 s, which produces
alerts (max one per sensor and severity every 60 s). Tune it with the environment variables in
`docker-compose.yml` (`INTERVAL_SECONDS`, `ANOMALY_EVERY_SECONDS`, `ANOMALY_DURATION_SECONDS`).

Stop using it on day 5, once Node-RED produces the real data: start the stack without
`--profile fake`.

## Storage consumer (console version)

Consumer group `storage`. Prints every alert, every message that breaks the contract
(`[CONTRACT] ... missing=[...]`), and a summary every 10 s.

```bash
docker compose logs -f storage-consumer
```
