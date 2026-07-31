# Migrate to the canonical Source API

This release is a hard break. There are no aliases, compatibility decoders, or
wrappers. Migrate the authored Topic tree, runtime composition, diagnostics,
and imports together.

## Topic identity

Before, Topic configuration selected a key:

```ts
orders: {
  schema: Order,
  key: "id",
}
```

After, every Topic Schema declares exact `id: ViewServerId` and configuration
has no key property:

```ts
import { ViewServerId } from "effect-view-server/config";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

orders: {
  schema: Order,
}
```

Optional, transformed, refined, branded, or non-string IDs are invalid.

## Source ownership

Before, transport-specific properties and top-level transport registration
described source ownership.

After, every source-owned Topic has one nominal `source`:

```ts
orders: {
  schema: Order,
  source: kafka.source({
    cleanupPolicy: "delete",
    retentionPolicy: "match-kafka-retention",
    topic: "orders.v1",
    regions: ["eu"],
    key: kafka.string(),
    value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
    localRowKey: ({ key }) => key,
    map: ({ value, region }) => ({
      price: value.price,
      region,
    }),
    startFrom: "earliest",
  }),
}
```

Use `grpc.topicSources(descriptors).materialized(...)` or `.leased(...)` for
gRPC. gRPC Mapping returns the complete Topic Row including `id`. Delete-only
Kafka derives `id` as `region:partition:localRowKey`; compaction-capable Kafka
derives it from Region, partition, and exact serialized key bytes.

Kafka migration is a hard cut: every source must add `cleanupPolicy` and
`retentionPolicy`. Delete-only `localRowKey` now receives decoded key, decoded
non-null value, and Region. Compaction-capable sources replace their key codec
with the corresponding `kafka.compactionKey.*` codec and remove `localRowKey`.
Delete-only null records are rejected; only compaction-capable null records are
tombstone Deletes. No compatibility defaults or aliases are retained.

## Runtime composition

Before, generic runtime options carried Kafka/gRPC clients, callbacks,
consumer-group behavior, and reconnect settings.

After, generic options contain server concerns and ordinary Layers provide
adapters:

```ts
const KafkaLive = kafkaNode.layer(viewServer, {
  consumerGroupPrefix: "orders-view",
  regions: {
    eu: { bootstrapServers: "kafka.internal:9092" },
  },
});

const GrpcLive = grpcNode.layer(viewServer, {
  orders: { baseUrl: "https://orders.internal" },
});

runViewServerRuntime(viewServer, {
  host: "0.0.0.0",
  websocketPort: 8080,
  tcpPublishHost: "127.0.0.1",
  tcpPublishPort: 8081,
}).pipe(Effect.provide([KafkaLive, GrpcLive]));
```

Use `layerConfig(...)` for Effect Config values. Layer acquisition failures and
unsatisfied service requirements remain typed.

## Diagnostics and health

Transport-specific optional health trees are removed. `GET /health` includes
topic-keyed canonical Source Health, while `GET /metrics` projects fixed,
low-cardinality SDK source-runtime metrics. Use explicit pushed diagnostics for
route-scoped adapter state:

```tsx
const materialized = useSourceHealth({ topic: "orders" });
const leased = useSourceHealth({
  topic: "ordersByRegion",
  routeBy: { region: "eu" },
});
```

Do not poll or add a one-shot helper. Materialized diagnostics reject
`routeBy`; Leased diagnostics require the exact route; source-free Topics are
not valid diagnostic inputs.

## Package imports

Replace config transport subpaths with adapter seams:

| Removed                       | Canonical                                  |
| ----------------------------- | ------------------------------------------ |
| config Kafka helpers          | `effect-view-server/kafka/contract`        |
| config gRPC helpers           | `effect-view-server/grpc/contract`         |
| runtime Kafka implementation  | `effect-view-server/kafka/node` Layer      |
| runtime gRPC implementation   | `effect-view-server/grpc/node` Layer       |
| shared adapter implementation | `effect-view-server/source-adapter/server` |

Browser code imports `/contract` only. Server code imports `/server` or
`/node`. Private workspace, `src`, `dist`, and unapproved nested paths are not
supported.

## Mutation ownership

Source-free Topics retain Runtime Client and TCP publishing. Source-owned Topics
reject direct publish, patch, delete, and reset. Tests that need source-owned
delivery should drive the Source Adapter seam; do not bypass ownership with an
in-memory mutation client.
