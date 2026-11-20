# Smart Building Monitoring

Real-time monitoring of a smart building using **MQTT**, **Node-RED**, **Kafka**, **InfluxDB** and **Grafana**.

```
Simulator ──MQTT──► Mosquitto ──► Node-RED ──► Kafka ──► consumers ──► InfluxDB ──► Grafana
   (A)                 (A)          (A)         (B)         (B)           (B)          (B)
```

## Team

| Role | Person | Scope |
|---|---|---|
| A — Ingestion & processing | Nouha Madiouni | `simulator/`, `mosquitto/`, `nodered/` |
| B — Streaming & analytics | Mehdi Attafi | `kafka/`, `services/`, `grafana/` |
| Shared | A + B | `docker-compose.yml`, `docs/interface-contract.md`, `README.md`, `tests/` |

The message format is defined in **[docs/interface-contract.md](docs/interface-contract.md)**.

## Requirements

- Docker Desktop (Windows / macOS) or Docker Engine + Compose plugin (Linux)
- 4 GB of free RAM for Docker
- Git, Python 3.11+ (to run scripts outside Docker)

## Getting started

```bash
git clone <repository-url> smart-building
cd smart-building
cp .env.example .env            # then change the passwords
docker compose up -d
docker compose ps               # every service should be "running" (kafka-init: "exited (0)")
```

> **Linux only**: Node-RED runs as user 1000. If the container fails to start
> (permission error on /data): `sudo chown -R 1000:1000 nodered/data`

## Web interfaces

| Service | URL | Credentials |
|---|---|---|
| Node-RED | http://localhost:1880 | — |
| Kafka UI | http://localhost:8080 | — |
| InfluxDB | http://localhost:8086 | `INFLUX_USER` / `INFLUX_PASSWORD` from `.env` |
| Grafana | http://localhost:3000 | `GRAFANA_USER` / `GRAFANA_PASSWORD` from `.env` |

## Quick checks

```bash
# MQTT: listen to every sensor
docker exec -it mosquitto mosquitto_sub -t 'building/#' -v

# MQTT: publish a test message
docker exec mosquitto mosquitto_pub -t building/B1/2/R203/co2 -m '{"sensorId":"B1-2-R203-co2","buildingId":"B1","floor":2,"room":"R203","sensorType":"co2","value":845.2,"unit":"ppm","timestamp":"2026-11-20T15:02:11.123Z"}'

# Kafka: list topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Kafka: read a topic
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic building.telemetry --from-beginning
```

## Git workflow

- `main`: always working, no direct pushes.
- `develop`: integration branch (default branch).
- Feature branches: `A/<topic>` and `B/<topic>`, merged into `develop` through a Pull Request reviewed by the other person.
- Commit messages: `feat(nodered): ...`, `fix(storage): ...`, `docs: ...`

## Project structure

```
smart-building/
├── docker-compose.yml          shared
├── .env.example                shared
├── docs/interface-contract.md   shared
├── simulator/                  A
├── mosquitto/config/           A
├── nodered/data/               A
├── kafka/                      B
├── services/                   B  (fake-producer, storage-consumer, aggregator, notifier)
├── grafana/provisioning/       B
└── tests/                      A + B
```
