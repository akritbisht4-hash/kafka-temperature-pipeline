# Kafka Event Pipeline in Rust

Project 2 — Kafka-based event pipeline. No AWS needed, runs 100% locally with Docker.

## What I built
Mock sensor producer → Kafka topic (`sensor-events`) → Rust consumer → `output/events.jsonl`.

Theme: message-queue / event-driven thinking, message delivery + ordering.

```
producer (random SensorEvent JSON)
   |
   v
Kafka topic `sensor-events` (1 partition, ordered log)
   |
   v
consumer (validate -> alert rule -> append to file)
```

## Architecture
- `src/lib.rs`: `SensorEvent`, `validate()`, `is_alert()`, `process_event()`, `format_for_storage()` + 8 unit tests
- `src/bin/producer.rs`: generates mock temp/humidity events, produces with `rskafka` (pure Rust, no cmake/librdkafka needed)
- `src/bin/consumer.rs`: `fetch_records` loop from earliest offset, validates, marks `ALERT` if temp > 35°C, writes JSONL
- `docker-compose.yml`: `apache/kafka:3.7.0` KRaft single-node (512M heap for 8GB RAM machines) + producer + consumer
- `.github/workflows/ci.yml`: `cargo fmt --check`, `clippy`, `cargo test` — block merge if fail

## How to run

### 1. Prereqs (Omarchy / Arch)
```bash
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```

### 2. Run full pipeline
```bash
docker compose up --build
# in another terminal:
cat output/events.jsonl
# [OK] {"sensor_id":"sensor-1",...}
# [ALERT] {"sensor_id":"sensor-2","temperature_c":38.2,...}
```

Services:
- Kafka inside compose: `kafka:9092`
- Kafka from host: `localhost:29092`

### 3. Run locally (without Docker, needs Kafka up)
```bash
cargo test
KAFKA_BROKERS=localhost:29092 cargo run --bin consumer &
KAFKA_BROKERS=localhost:29092 EVENT_COUNT=50 cargo run --bin producer
```

### 4. Tests
```bash
cargo test
# 8 tests: serialization roundtrip, validation (temp/humidity/id), alert true/false, process+format, malformed JSON rejection
```

### 5. CI/CD
Push to `main` runs GitHub Action. Enable branch protection in GitHub:
Settings → Branches → Add rule for `main` → Require status checks → `test` → block merge if tests fail.

## Why Rust + rskafka?
- Pure-Rust client (`rskafka 0.6`) — no `cmake` / `librdkafka` system deps, tiny runtime (~5-10MB vs Python 50-80MB)
- Static binaries in `debian:bookworm-slim` image
- `tokio` async producer/consumer, retry loop for slow Kafka startup on low-RAM machines

## Reproduce
1. `docker compose up --build`
2. Wait 20s for Kafka healthy, producer logs `produced event offset=N`
3. Consumer logs `consumed offset=N [ALERT]/[OK]`
4. Check ordering: offsets increase monotonically in `output/events.jsonl`
