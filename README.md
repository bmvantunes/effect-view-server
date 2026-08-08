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
