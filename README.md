# kafka-outbox

Transactional outbox + Kafka in Java: publish domain events reliably without
losing them on crashes or double-sending them on retries. Zero dependencies
for the core; the Kafka adapter needs `kafka-clients` and a real broker.

## The problem

A service writes to its database, then publishes an event to Kafka. Two
things go wrong in production:

1. **Dual-write failure.** The DB commit succeeds, the process crashes before
   the Kafka send — the event is lost forever. No retry can recover it because
   nothing remembers it was supposed to be sent.
2. **Naive retry double-send.** The Kafka send succeeds, the ack is lost, the
   code retries — the consumer processes the event twice. For a
   "charge the card" event, that is a real incident.

The transactional outbox fixes both with one move: the business transaction
writes the domain row **and** the outbox row atomically. A relay then
publishes unpublished rows and marks them published only after the broker
acks. The consumer dedupes on a processed-message table. At-least-once
delivery plus an idempotent consumer gives effectively-once processing.

## Why outbox, not the alternatives

- **Dual-write (DB then Kafka):** broken by construction — no atomicity
  between two systems. This repo exists because of this failure mode.
- **Change-data-capture (Debezium):** excellent at scale, but it is
  infrastructure to operate (Kafka Connect cluster, schema registry,
  connector config). The outbox table here is the same idea with zero new
  infrastructure — the right call for most services.
- **Outbox:** one extra table, one relay loop, works with the database you
  already have. The tradeoff is polling latency (tunable, ~100ms is typical)
  and the relay being another thing to run.

## What's here

- **`OutboxEvent`** — immutable domain event: id, aggregate type/id,
  event type, JSON payload.
- **`OutboxStore`** — the outbox table contract: `append`, atomic
  `claimUnpublished(limit)`, `markPublished` (only after ack),
  `releaseClaim` (crash path). `InMemoryOutboxStore` mirrors
  `SELECT ... FOR UPDATE SKIP LOCKED`: rows claimed by one relay instance
  are skipped by the others, never double-claimed.
- **`OutboxRelay`** — the poll loop. Claims a batch, publishes each event,
  marks published only after the ack. On failure it releases the unprocessed
  remainder so this or another instance retries it. Safe with N instances
  polling concurrently.
- **`EventPublisher`** — broker contract. `RecordingEventPublisher` is the
  in-memory one with failure injection for the harness.
  `KafkaEventPublisher` is code-complete against the Kafka client API:
  sends to `outbox.<event-type>`, carries the event id in a header for
  dedupe, and blocks for the ack (`send().get()`).
- **`IdempotentConsumer`** + **`ProcessedMessageStore`** — the consumer
  side. `tryMark` atomically records the message id; redeliveries hit the
  table and are skipped, never re-applied. Same idea as an idempotency key.
- **`demo/OutboxDemo`** — the failure-injection harness (see Results).
- **`schema.sql`** — Postgres DDL for `outbox` and `processed_messages`,
  plus the relay's claim query (`FOR UPDATE SKIP LOCKED` with a stale-claim
  reaper window for crashed relays).
- **`docker-compose.yml`** — Postgres + Kafka (KRaft, no ZooKeeper) for the
  real run.

## Project structure

```
src/com/ajay/outbox/
  OutboxEvent.java  OutboxStore.java  InMemoryOutboxStore.java
  EventPublisher.java  RecordingEventPublisher.java  KafkaEventPublisher.java
  PublishException.java
  ProcessedMessageStore.java  InMemoryProcessedMessageStore.java
  IdempotentConsumer.java  OutboxRelay.java
  demo/OutboxDemo.java
schema.sql  docker-compose.yml
```

## Build & run (Java 17+, zero dependencies)

```bash
cd kafka-outbox
javac -encoding UTF-8 -d out $(find src -name '*.java' ! -name 'KafkaEventPublisher.java')
java -cp out com.ajay.outbox.demo.OutboxDemo
```

(`KafkaEventPublisher` is excluded from the default build because it needs
`kafka-clients` — compile it separately against the jar; it was verified
against the Kafka client API.)

## Running against a real broker

```bash
docker compose up -d
psql postgresql://outbox:outbox@localhost:5432/outbox -f schema.sql
```

Then wire `KafkaEventPublisher` (with `bootstrap.servers=localhost:29092`,
`acks=all`, `enable.idempotence=true`) behind a JDBC `OutboxStore`
implementation using the claim query in `schema.sql`. The relay and consumer
logic is unchanged — only the store and publisher implementations swap.

## Results

![OutboxDemo output](docs/output.png)

What this run proves, phase by phase:

- **Phase 1** — the baseline: 5 events appended, claimed, published, and
  marked; the consumer ledger shows 5 applied, 0 duplicates.
- **Phase 2** — the interesting one: the relay crashes *after* the broker
  ack but *before* `markPublished`. The unmarked rows are reclaimed on
  restart; one event is redelivered and the consumer absorbs it via the
  processed-message table. Final state: 9 applied, 1 duplicate skipped,
  **0 lost**.
- **Phase 3** — a duplicate delivery is skipped by the dedupe table; the
  ledger does not move.
- **Phase 4** — two relay instances poll concurrently over 20 events
  (11/9 claim split): the broker receives 20 distinct events, each exactly
  once, and the consumer applies 20 with 0 double-applied.

Net: 29 applied, 2 duplicates absorbed, 0 lost, 0 double-applied —
at-least-once delivery plus an idempotent consumer behaving as
effectively-once.

## Honest limitations

- The demo uses the in-memory store and publisher; the Postgres + Kafka path
  is provided (DDL, claim query, adapter, compose file) but the demo does not
  need a broker to prove the pattern's failure semantics.
- The relay polls rather than tails the WAL — simpler to operate, adds
  ~poll-interval latency. For sub-100ms delivery, Debezium-style CDC is the
  next step up.
- `KafkaEventPublisher` maps one topic per event type; real deployments often
  want a topic-naming strategy per bounded context, not per type.
