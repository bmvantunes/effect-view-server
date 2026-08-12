# Effect View Server

Effect View Server turns validated source streams into typed snapshots and
deltas for React and framework-neutral clients.

## Guides

- [Public API](./docs/public-api.md)
- [Source Adapter SDK](./docs/source-adapter-sdk.md)
- [Kafka Source Adapter](./docs/kafka-source-adapter.md)
- [gRPC Source Adapter](./docs/grpc-source-adapter.md)
- [Runtime Config](./docs/runtime-config.md)
- [Health And Metrics](./docs/health-and-metrics.md)
- [Migration to the canonical Source API](./docs/migration-canonical-source.md)
- [Examples](./examples/README.md)

## Install

```sh
npm install effect-view-server effect
```

React applications also install `react`, `react-dom`, and
`@effect/atom-react`.

## One Topic tree

Every Topic Schema has one exact required `id: ViewServerId`. A Topic has zero
or one nominal Source Definition at `source`; there is no configurable Topic
key and no transport-specific Topic property.

```ts
import { Schema } from "effect";
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { kafka } from "effect-view-server/kafka/contract";

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

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "match-kafka-retention",
        topic: "orders.v1",
        regions: ["primary"],
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
    manualRows: {
      schema: Schema.Struct({
        id: ViewServerId,
        label: Schema.String,
      }),
    },
  },
});
```

Delete-only Kafka sources derive the canonical ID as
`region:partition:localRowKey`. Compaction-capable sources derive it from the
Region, partition, and exact serialized Kafka key bytes. gRPC Mappings return
the complete row, including `id`.

## Runtime composition

The generic runtime knows about server concerns only. Kafka and gRPC are
ordinary Effect Layers supplied at the application edge.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

const KafkaLive = kafkaNode.layer(viewServer, {
  consumerGroupPrefix: "orders-view",
  regions: {
    primary: { bootstrapServers: "kafka.internal:9092" },
  },
});

runViewServerRuntime(viewServer, {
  host: "0.0.0.0",
  websocketPort: 8080,
  tcpPublishHost: "127.0.0.1",
  tcpPublishPort: 8081,
}).pipe(Effect.provide(KafkaLive), NodeRuntime.runMain);
```

Use `Effect.provide([KafkaLive, GrpcLive])` when an application uses both
first-party adapters.

## Kafka Schema Registry Protobuf

Schema Registry decoding is opt-in per key or value and requires a
Buf-generated message descriptor. The Registry connection itself belongs to
the Kafka Region, so every Source in one cluster/Region shares one scoped
client, cache, and drift monitor:

```ts
import { OrderKeySchema, OrderValueSchema } from "./gen/orders_pb";

const registrySource = kafka.source({
  cleanupPolicy: "compact",
  retentionPolicy: "match-kafka-retention",
  topic: "orders.v1",
  regions: ["primary"],
  key: kafka.schemaRegistry.protobuf(OrderKeySchema),
  value: kafka.schemaRegistry.protobuf(OrderValueSchema),
  map: ({ key, value }) => ({
    orderId: key.orderId,
    total: value.total,
  }),
  startFrom: "earliest",
});
```

Attach `registrySource` to the Topic's `source` property, then configure that
View Server with the Region-scoped Registry connection:

```ts
const RegistryOrder = Schema.Struct({
  id: ViewServerId,
  orderId: Schema.String,
  total: Schema.Number,
});

const registryViewServer = defineViewServerConfig({
  topics: {
    orders: { schema: RegistryOrder, source: registrySource },
  },
});

const KafkaLive = kafkaNode.layer(registryViewServer, {
  consumerGroupPrefix: "orders-view",
  regions: {
    primary: {
      bootstrapServers: "kafka.internal:9092",
      schemaRegistry: {
        url: "https://schema-registry.internal",
        auth: { username: "view-server", password: "your_password_here" },
      },
    },
  },
});
```

The adapter is a read-only Registry consumer: it never registers schemas,
changes compatibility settings, or deletes versions. It supports Protobuf and
the default Topic Name Strategy only (`<topic>-key` and `<topic>-value`); there
is no dynamic-schema fallback, Avro, or JSON Schema path.

Before any Kafka consumer starts, the Layer requires effective
`FULL_TRANSITIVE` compatibility for every used subject and recursive reference,
loads the complete active history, and checks it against the configured
generated descriptor using Buf `WIRE` semantics. Compatibility is directional
and wire-level, not exact descriptor equality: adding a fresh-tag field is
accepted, and deleting a field is accepted only when its number is reserved.
Use a new topic/subject version for an incompatible change.

At runtime, an unknown, deleted, or incompatible schema ID is a fatal Source
failure before Mapping, settlement, or offset commit. A known compatible ID
whose payload is malformed is an ordinary item Rejection and may be settled and
committed according to the normal rejected-record policy. Key and value are
validated together against one Registry revision, including tombstones. Drift
is reported as a detailed `schema-registry` dependency issue while the existing
Source heartbeat lifecycle (`WaitingToRetry`, `Exhausted`, and so on) remains
authoritative. See the [Kafka Source Adapter guide](./docs/kafka-source-adapter.md#schema-registry-protobuf)
for the complete contract.

## React diagnostics

`createViewServerReact(viewServer)` exposes transport-neutral Live Query hooks,
aggregate health hooks, and exact pushed Source Diagnostics:

```tsx
const source = useSourceHealth({ topic: "orders" });

const leased = useSourceHealth({
  topic: "ordersByRegion",
  routeBy: { region: "eu" },
});
```

Materialized diagnostics reject `routeBy`. Leased diagnostics require the exact
Feed Route. Source-free Topics are rejected. Diagnostics are scoped streams:
matching consumers share one subscription, and unmount or client close releases
it. React does not poll and Source Health is not added to the Live Query event
hot path.

## Public seams

The package has no root export. Use explicit subpaths:

- `effect-view-server/config`, `/runtime`, `/client`, `/react`, `/in-memory`
- `effect-view-server/source-adapter`, `/server`, `/testing`
- `effect-view-server/kafka/contract`, `/server`, `/node`
- `effect-view-server/grpc/contract`, `/server`, `/node`
- `effect-view-server/value-semantics`

Contract imports are browser-safe. Server and Node implementations remain on
their explicit platform subpaths.
