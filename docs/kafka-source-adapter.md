# Kafka Source Adapter

The first-party Kafka Source Adapter is the production Materialized-source
implementation of the Source Adapter SDK. Its public surfaces are deliberately
split by platform:

- `effect-view-server/kafka/contract` is browser-safe and owns codecs, Source
  Definitions, start positions, identifiers, Schemas, and public types.
- `effect-view-server/kafka/server` is the platform-neutral Kafka server seam.
- `effect-view-server/kafka/node` is the Node.js Platformatic Kafka
  implementation and Layer composition API.

Applications declare Kafka on the Topic's canonical `source` property. The
Topic row must contain the canonical `id: Schema.String`; the adapter derives
that ID from the source region and `localRowKey`.

## Define and run a source

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";
import { defineViewServerConfig } from "effect-view-server/config";
import { kafka } from "effect-view-server/kafka/contract";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";

const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: kafka.source({
        topic: "orders.v1",
        regions: ["primary", "recovery"],
        key: kafka.string(),
        value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          customerId: value.customerId,
          price: value.price,
          region,
        }),
        startFrom: "earliest",
      }),
    },
  },
});

const KafkaLive = kafkaNode.layer(viewServer, {
  consumerGroupPrefix: "orders-view-v1",
  regions: {
    primary: {
      bootstrapServers: "kafka-primary.internal:9092",
    },
    recovery: {
      bootstrapServers: ["kafka-recovery-a.internal:9092", "kafka-recovery-b.internal:9092"],
      tls: {
        rejectUnauthorized: true,
        serverName: "kafka-recovery.internal",
      },
    },
  },
});

runViewServerRuntime(viewServer).pipe(Effect.provide(KafkaLive), NodeRuntime.runMain);
```

The regions object is inferred from all Kafka Source Definitions in the View
Server config. Missing regions and extra region names are type errors, and
source tuples retain literal region names without requiring `as const`.
`kafkaNode.layerConfig(...)` accepts the same shape through Effect `Config`
values when credentials and endpoints should be resolved from application
configuration.

## Codecs and mapping

Every source supplies explicit key and value codecs:

- `kafka.bytes()` preserves the record bytes.
- `kafka.string()` decodes UTF-8.
- `kafka.json(() => Schema.toCodecJson(WireSchema))` uses an Effect Schema JSON
  codec and preserves its decoded type.
- `kafka.protobuf(MessageDescriptor)` uses a Buf generated message descriptor.
- `kafka.codec({ name, decode })` owns a custom Effect decoder and its typed
  error channel.

`localRowKey` receives the decoded key, source region, and exact Kafka metadata.
It never receives the value, so a compacted-topic tombstone can identify the row
without decoding a missing value. `map` receives decoded key and value,
metadata, and region. It returns every Topic field except `id`; returning a
missing, extra, or incorrectly typed field is rejected by the public type.
Runtime Core also decodes the mapped row through the Topic Schema before
applying it.

The canonical row ID is the exact text `region:localRowKey`. Region names cannot
contain a colon, so `kafka.decodeRowId(...)` splits only at the first colon and
preserves the local key verbatim, including any later colons or percent
characters. Two regions may therefore publish the same local key without
overwriting one another.

## Start positions and consumer groups

`startFrom` is mandatory on every Source Definition:

- `"earliest"` rebuilds from the beginning of each partition.
- `"latest"` starts after records already present when the source attempt
  begins.
- `{ mode: "committed", consumerGroupId, fallback }` uses another group's
  committed offsets and applies an explicit `"earliest"`, `"latest"`, or
  `"fail"` fallback for missing commits.
- `{ mode: "timestamp", atNanos, fallback }` resolves the first offset at or
  after an epoch-nanosecond timestamp.
- `{ mode: "durationAgo", duration, fallback }` captures the Effect Clock once
  for the Topic lifetime and resolves a timestamp relative to it.

Resolved offsets are frozen for retries within the same Topic lifetime. The
active adapter group still resumes from its own committed offsets after a
consumer restart. Its ID is derived from `consumerGroupPrefix` plus the View
Server Topic name—not the Kafka topic—so two Topic bindings cannot accidentally
share progress.

This runtime materializes rows in memory. A committed consumer offset is an
at-least-once delivery checkpoint, not a durable View Server snapshot. Choose an
authoritative replay position when a process restart must rebuild all rows.

## Delivery and failure behavior

The adapter acquires every configured region before publishing Ready health.
Regions are independent concurrent lanes, while records inside one region are
decoded, mapped, applied, and committed sequentially. This preserves partition
progress and prevents a slow region from blocking its siblings.

A non-null record produces an Upsert. A null value is a tombstone and produces
a Delete for the canonical row ID. Decode, key, mapping, and Topic-Schema
failures become exact item Rejections: the adapter publishes Degraded health,
settles and commits the rejected record, then continues the lane. Diagnostics
contain safe codec names and Kafka locations, never payload bytes or
credentials.

The offset is committed only after Runtime Core settles the corresponding
Delivery or Rejection successfully. A commit failure terminates that source
attempt so supervision can close its consumers and retry. All consumers,
iterators, and network resources are scoped; runtime shutdown and interrupted
consumers release them before the Layer closes.

## Health and metrics

`liveClient.subscribeSourceHealth("orders")` exposes the standard
Source Health stream. Kafka metrics include exact per-region assignment and
offset state plus counts for decode, mapping, rejection, commit, reconnect,
rebalance, and close outcomes. Metrics are sampled at the Source Adapter SDK's
bounded cadence and lifecycle or rejection transitions publish immediately.

The package's shared behavioral conformance suite places controllable Kafka
Region drivers behind the production server Layer and validates the resulting
Materialized lifecycle through Runtime Core. Separate package conformance
validates the built contract, server, and Node entry points. The browser-safe
contract has a 96 KiB gzipped budget and cannot import Platformatic Kafka,
Node.js built-ins, or server/runtime implementation modules.

The focused benchmark profile has two serial tasks. The transport-neutral task
compares equal-size accepted, poison-continuation, and four-region workloads
through the production Kafka Source Adapter server Layer, then measures
assignment, commit, lag, and metrics-snapshot bookkeeping across 64
partitions. The broker-backed task starts Apache Kafka, consumes through the
production Platformatic Kafka Node Adapter, waits for Runtime Core convergence,
and verifies the active consumer group's committed offset after every sample.
The accepted regression gate is:

```sh
vp run -w bench:baseline:kafka-source-adapter
```
