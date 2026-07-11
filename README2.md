# Car GPS → Kafka Streams → Postgres Demo

Multi-module Maven build:

```
Connected-Car-Analytics-Platform/        (parent pom, packaging=pom)
├── gps-simulator/            producer — pushes GPS pings to Kafka
└── gps-streams-processor/    Kafka Streams app — raw sink + windowed speed aggregation, writes to Postgres
```

## 1. Build everything from the parent

```bash
cd Connected-Car-Analytics-Platform
mvn clean install
```

This builds both modules in the correct order (Maven reactor resolves that
`gps-streams-processor` and `gps-simulator` both depend on the parent).

## 2. Start infra (Kafka in KRaft mode — no Zookeeper — + Postgres + kafka-ui)

```bash
docker compose up -d
```

- Kafka: `localhost:9092`
- Kafka UI: http://localhost:8085
- Postgres: `localhost:5432` (db `gpsdb`, user/pass `gps`/`gps`)

## 3. Create topics

```bash
./scripts/create-topics.sh
```

Creates `car-gps-data` (3 partitions) and `car-speed-stats` (3 partitions).

## 4. Run the Kafka Streams processor first

```bash
cd gps-streams-processor
mvn spring-boot:run
```

On startup it logs the topology (`topology.describe()`), creates
`gps_ping` and `car_speed_window_stats` tables via `schema.sql`, then:

- persists every raw ping to `gps_ping`
- maintains a 30s tumbling window (5s grace) per car, aggregating
  count / avg speed / max speed, upserting into `car_speed_window_stats`
  and republishing to the `car-speed-stats` topic

## 5. Run the simulator

```bash
cd gps-simulator
mvn spring-boot:run
```

Publishes pings for 5 simulated cars every 2s, keyed by `carId`.

## Check results

```bash
curl localhost:8082/api/pings/car-1
curl localhost:8082/api/speed-stats/car-1
```

Or watch messages flow through `car-gps-data` and `car-speed-stats` in Kafka UI.

## Design notes worth knowing

- **Topology is framework-agnostic**: `GpsStreamsTopology.build(...)` takes plain
  `Consumer<GpsData>` / `Consumer<SpeedStats>` sinks instead of the JDBC repositories
  directly, so `GpsStreamsTopologyTest` exercises it with `TopologyTestDriver` and
  no Spring context, no broker, no DB.
- **`EXACTLY_ONCE_V2`** is set on the Streams config — this guarantees the
  consume → update-state-store → produce-to-`car-speed-stats` cycle is atomic
  even across rebalances/retries. It does **not** extend to the JDBC writes
  inside `foreach`, since those are a side effect outside Kafka's transaction —
  if you need the DB write itself to be exactly-once, either make the upsert
  idempotent (as `car_speed_window_stats` already is, via `ON CONFLICT`) or
  move to a transactional outbox / Kafka Connect JDBC sink instead of `foreach`.
- **Raw ping writes are plain inserts**, not idempotent — a Streams restart can
  redeliver and duplicate rows in `gps_ping` unless you add a unique constraint
  (e.g. on `(car_id, event_timestamp)`) with `ON CONFLICT DO NOTHING`.
- `car_speed_window_stats` upserts are safe to replay since the primary key is
  `(car_id, window_start)` and Streams re-emits the same window repeatedly as
  more pings arrive — that's expected windowed-aggregation behavior, not a bug.
- Swap `JsonSerde` for Avro/Protobuf + Schema Registry once more than one team
  owns `car-gps-data` / `car-speed-stats`.
