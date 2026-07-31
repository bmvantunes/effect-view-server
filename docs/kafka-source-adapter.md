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
Topic row must contain the canonical `id: ViewServerId`; the adapter derives
that ID according to the mandatory cleanup policy. Delete-only identity uses
Region, partition, and `localRowKey`; compaction-capable identity uses Region,
partition, and the exact serialized Kafka key bytes.

## Define and run a source

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { kafka } from "effect-view-server/kafka/contract";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";

const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

const Order = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "match-kafka-retention",
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
  retentionSweepInterval: "15 minutes",
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
- `kafka.json(() => Schema.toCodecJson(WireSchema))` requires well-formed UTF-8,
  uses an Effect Schema JSON codec, and preserves its decoded type.
- `kafka.protobuf(MessageDescriptor)` uses a Buf generated message descriptor.
- `kafka.codec({ name, decode })` owns a custom Effect decoder and its typed
  error channel.

Every source declares `cleanupPolicy` as `delete`, `compact`, or
`compact-and-delete`, and declares `retentionPolicy` as
`match-kafka-retention` or an Effect `Duration.Input`. Neither field has a
default.

Delete-only sources use the ordinary codecs above. `localRowKey` receives the
decoded key, decoded non-null value, and exact Region. Its `map` additionally
receives the derived `localRowKey` and detached metadata.

Compaction-capable sources use `kafka.compactionKey.bytes()`,
`.string()`, `.json(...)`, `.protobuf(...)`, or `.codec(...)` for the key and
must omit `localRowKey`. Those key decoders receive only exact non-null
serialized bytes—never metadata—so application behavior cannot influence
canonical compact identity. Their `map` receives decoded key/value, Region, and
metadata, but no local key or identity override.

Both mapping branches return every Topic field except `id`; returning a
missing, extra, or incorrectly typed field is rejected by the public type.
Runtime Core also decodes the complete adapter-injected row through the Topic
Schema before applying it.

Every executable codec and Mapping callback receives a detached, frozen
metadata envelope and header collection. Header byte arrays are copied once
into that snapshot. Custom codec names are quoted in safe rejection diagnostics;
decoder failures and payload bytes are never included.

Delete-only canonical row ID is
`region:partition:localRowKey`. The decoder preserves the local key verbatim,
including colons. Compaction-capable canonical row ID is
`region:partition:k<unpadded-base64url(serializedKeyBytes)>`. It reversibly
encodes the exact bytes; byte-equal keys address the same row only inside the
same Region and partition, while byte-distinct keys never collapse merely
because they decode to the same application value. Browser-safe
`deleteRowId`/`compactionRowId` and policy-specific decode helpers are available
on `kafka`.

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
share progress. The percent-encoded active group ID must fit Kafka's 32,767-byte
protocol-string ceiling and is rejected during pure Layer construction if it
does not.

This runtime materializes rows in memory. A committed consumer offset is an
at-least-once delivery checkpoint, not a durable View Server snapshot. Choose an
authoritative replay position when a process restart must rebuild all rows.

## Delivery and failure behavior

The adapter acquires every configured region before publishing Ready health.
Regions are independent concurrent lanes, while records inside one region are
decoded, mapped, applied, and committed sequentially. This preserves partition
progress and prevents a slow region from blocking its siblings.

The per-region `decoded` metric counts each fully decoded Kafka record once:
after key decoding for a tombstone, or after both key and value decoding for an
upsert. A key or value decode rejection does not increment it.

A non-null record produces an Upsert unless its finite deadline is already due,
in which case it takes the keyed ordinary Delete path without a transient
Upsert. A null value is a tombstone Delete only for `compact` and
`compact-and-delete`; under `delete`, it is a settled Source Item Rejection.
Decode, key, mapping, and Topic-Schema failures become exact item Rejections:
the adapter publishes Degraded health, settles and commits the rejected record,
then continues the lane. Diagnostics contain safe codec names and Kafka
locations, never payload bytes or credentials.

The offset is committed only after Runtime Core settles the corresponding
Delivery or Rejection successfully. A commit failure terminates that source
attempt so supervision can close its consumers and retry. All consumers,
iterators, and network resources are scoped; runtime shutdown and interrupted
consumers release them before the Layer closes.

## Broker contract and retention

The aggregate Node Layer opens one Platformatic Kafka Admin client per Region
and batches effective `cleanup.policy` and `retention.ms` discovery for all
unique source topics. Cleanup parsing is order-insensitive and trims whitespace,
so `compact,delete`, `compact, delete`, and `delete,compact` are equivalent.

Validation happens once during Layer acquisition. Missing `DESCRIBE_CONFIGS`
permission, Admin or response failure, missing/malformed fields, cleanup
mismatch, and invalid `retention.ms` are accumulated into one typed redacted
failure. The complete runtime fails before any consumer, retention sweep,
listener, or server port starts; validation failure is never reduced to health
degradation.

For `match-kafka-retention`, compact-only topics and broker
`retention.ms = -1` retain rows forever. Delete-capable topics use the
non-negative broker duration. Explicit positive finite durations or Effect
infinity override the broker value; zero and negative explicit values are
configuration errors. This projects configured time retention only—it does not
claim exact segment deletion or emulate `retention.bytes`.

Finite deadlines are `Kafka record timestamp + resolved duration`, all in
Unix-epoch nanoseconds. Eligibility compares them with epoch wall time from
Effect `Clock.currentTimeMillis`; monotonic nanoseconds are used only for
elapsed sweep duration and cadence. One logical Topic lifetime owns the
generation-protected index and one coarse sweep. `retentionSweepInterval` is a
positive finite Node Layer option and defaults to 15 minutes. Successful
Upserts replace deadlines with never-reused lifetime generation tokens;
tombstones, already-expired records, and expiration all use the same keyed
ordinary Delete path.

Expiration Delete failure never stops or pauses ingestion. The exact work
identity remains retryable, the Source/Topic/aggregate health trees degrade
immediately, and a later sweep retries it. Startup broker configuration is a
snapshot: coordinated stop, broker change, and restart is required to change
the contract safely.

## Health and metrics

`liveClient.subscribeSourceHealth({ topic: "orders" })` exposes the standard
Source Health stream. Kafka metrics include exact per-region assignment and
offset state plus counts for decode, mapping, rejection, commit, reconnect,
rebalance, and close outcomes. Retention metrics expose declared/observed
cleanup, configured/resolved retention, tracked and due rows, expired and
authoritative-expired Deletes, unique failed work, cumulative retry failures,
latest safe failure, last sweep epoch/duration, and interval. Metrics are
sampled at the Source Adapter SDK's bounded cadence; lifecycle, rejection, and
maintenance-ledger transitions publish immediately.

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
