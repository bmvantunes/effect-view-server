# ADR 0008: One shared View Server Config with Layer-provided source runtimes

## Status

Accepted design. Implementation is pending, and every package import described as planned below remains unavailable until its package surface is implemented and verified.

## Context

The Remote Browser Client needs Topic Row Schemas plus exact source failure and metrics Schemas at runtime to decode the Wire Protocol. Source execution may depend on Kafka, gRPC, message queues, custom streaming transports, credentials, sockets, or platform-specific libraries that must not enter the browser dependency graph.

A separate browser contract and mirrored server runtime configuration would create a hard dependency boundary, but would also force users to describe the same topic topology twice. Referencing contract fragments from a second tree still adds ceremony and creates a drift surface. The existing one-config View Server API is the ergonomic baseline.

Effect v4 distinguishes immutable descriptions from capabilities supplied through Context and Layer. View Server uses that same boundary without duplicating application configuration.

## Decision

Applications author exactly one frozen View Server Config through `defineViewServerConfig(...)`. It owns Topic names, Topic Schemas with canonical `id: Schema.String`, query metadata, and each Topic's zero-or-one canonical Source Definition. React, the Remote Browser Client, In-Memory View Server, and the real runtime all receive that same value. There is no `defineViewServerContract(...)`, `defineViewServerRuntime(...)`, mirrored server topic tree, per-topic implementation list, or generated compatibility alias.

This simplicity has an explicit v1 bundle tradeoff. Runtime values captured by browser-safe Source Definitions—Mapping functions, generated service descriptors, platform-neutral codecs, failure Schemas, rejection-location Schemas, and metrics Schemas—may enter the browser bundle even though React never executes ingestion. TypeScript types cannot replace the runtime Schemas needed to decode exact Source Diagnostics, and ordinary tree shaking cannot reliably remove nested object members. V1 accepts that cost rather than adding mirrored authoring, code generation, a custom build transform, or automatic contract projection. Every adapter's conformance suite builds a real browser fixture and enforces a documented bundle-size budget. Concrete transports, clients, credentials, Node modules, sockets, and platform Layers remain forbidden from the contract graph regardless of tree shaking.

A Source Adapter's shared contract surface creates complete topic-owned Source Definitions. These constructors accept the adapter's strongly typed browser-safe per-source options, including external source names, logical region or endpoint names, Schemas and codecs without platform imports, Mapping and Local Row Key functions, Source Start Position, Route Fields, and an optional explicit Source Retry Policy override. The definition also carries exact failure, lifecycle metrics, and lifecycle rejection-location Schemas plus its nominal Source Adapter identity. It contains no concrete transport client, client service token, credential value, socket, platform API, Node import, Layer, ManagedRuntime, or imperative mutation client.

The server-only `/server` surface implements the adapter's nominal runtime service. Platform surfaces such as `/node` provide concrete client Layers and must expose the standard aggregate `layer(...)` and `layerConfig(...)` pair that derives its exact logical-client map from the View Server Config and provides the adapter runtime service. Runtime resources remain scoped, failures remain typed, and no reusable adapter module calls `Effect.run*`.

The first-party Kafka API illustrates the complete composition. The one shared configuration declares each source once using the same concise region strings as the current Kafka API. In the pseudocode below, `kafka` comes from the planned `effect-view-server/kafka/contract` export:

```ts
import { defineViewServerConfig } from "effect-view-server/config";

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: kafka.source({
        topic: "source-orders-v3",
        regions: ["usa", "london"],
        value: kafka.protobuf(OrdersValueSchema),
        key: kafka.protobuf(OrdersKeySchema),
        localRowKey: ({ key }) => key.orderId,
        map: ({ key, value, region }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          strategyId: key.strategyId,
          updatedAt: value.updatedAt,
        }),
        startFrom: {
          mode: "durationAgo",
          duration: "5 minutes",
          fallback: "earliest",
        },
      }),
    },
  },
});
```

The first-party gRPC contract preserves exact generated request and response inference without exposing transport lifecycle callbacks:

```ts
const grpcSources = grpc.topicSources({
  orders: ordersService,
  strategies: strategiesService,
});

export const viewServer = defineViewServerConfig({
  topics: {
    strategies: {
      schema: Strategy,
      source: grpcSources.materialized({
        client: "strategies",
        method: "streamStrategies",
        request: () => ({ universe: "global" }),
        map: ({ value }) => ({
          id: `${value.strategyId}:${value.region}`,
          strategyId: value.strategyId,
          region: value.region,
          status: value.status,
          notional: value.notional,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});
```

`client` autocompletes only descriptor-record keys, `method` autocompletes only that client's server-streaming methods, `request` is recursively exact against the selected generated request-init type, and Mapping receives the exact generated response message. The gRPC adapter owns method invocation, AsyncIterable-to-Stream conversion, cancellation, and finalization. Its public Source Definition accepts no `acquire` or `release` callback.

The Node entrypoint provides one aggregate Kafka Layer and preserves the current generic runtime options. In the pseudocode below, `kafkaNode` comes from the planned `effect-view-server/kafka/node` export:

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { runViewServerRuntime } from "effect-view-server/runtime";

const KafkaLive = kafkaNode.layer(viewServer, {
  consumerGroupPrefix: "orders-view:replica-0",
  regions: {
    usa: usaOptions,
    london: londonOptions,
  },
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    websocketPort: 8080,
    tcpPublishPort: 8081,
  }).pipe(Effect.provide(KafkaLive)),
);
```

When deployment values come from the active Effect Config Provider, the matching Config-backed constructor preserves the same exact inferred region record and resolves it during Layer construction:

```ts
import { Config } from "effect";

const KafkaLive = kafkaNode.layerConfig(viewServer, {
  consumerGroupPrefix: Config.string("KAFKA_CONSUMER_GROUP_PREFIX"),
  regions: {
    usa: {
      bootstrapServers: Config.string("KAFKA_USA_BOOTSTRAP"),
    },
    london: {
      bootstrapServers: Config.string("KAFKA_LONDON_BOOTSTRAP"),
    },
  },
});
```

Kafka region strings are logical identities, not bootstrap addresses or credential values. The same literal names select regions in Topic-owned Source Definitions and key the concrete `regions` map supplied once to `kafkaNode.layer(viewServer, ...)` or the Config-wrapped map supplied to `kafkaNode.layerConfig(viewServer, ...)`. Both constructors derive the exact union of required regions from the View Server Config and reject missing or extra region entries. `layer(...)` snapshots already-resolved options; `layerConfig(...)` recursively resolves its exact option tree once through Effect `Config.unwrap(...)` during Layer construction. Both acquire clients as scoped resources and provide the Kafka Source Adapter runtime service. A custom application may instead compose lower-level `/server` and `/node` Layers explicitly.

`runViewServerRuntime(viewServer, options)` exposes the exact union of still-unsatisfied Source Adapter runtime, retry Schedule, and application service requirements. Effect v4 `Effect.provide(...)` removes the outputs of `KafkaLive` from that requirement union while preserving Layer construction failures. If several Source Adapters are used, the application supplies their aggregate Layers as the non-empty Layer tuple supported directly by `Effect.provide([KafkaLive, OtherLive])`.

View Server runtime options remain generic server concerns such as host, WebSocket port, TCP publish port, admission, authentication, health, and query-engine settings. They contain no hardcoded `kafka`, `grpc`, or future transport-specific bags. Adapter-wide settings belong to the adapter Layer; topic-specific settings belong to the one Source Definition.

Pure View Server Config and Source Definition constructors throw named configuration errors for deterministic declaration mistakes and return frozen snapshots. First-party platform adapters follow Effect's paired `layer(...)` and `layerConfig(...)` convention: the latter accepts an exact `Config.Wrap` option tree and preserves `Config.ConfigError` in its Layer error channel. Source acquisition and execution failures likewise remain typed. The first-party gRPC adapter converts the selected ConnectRPC AsyncIterable through Effect's scoped `Stream.fromAsyncIterable(...)`, whose finalizer invokes the iterator's `return()` when its Scope closes early; the adapter may additionally own an AbortController when required by the concrete transport. Runtime startup defensively revalidates the common Source Definition envelope before invoking the nominal adapter runtime service.

## Consequences

Users retain the current one-config API and pay one explicit aggregate Layer per Source Adapter rather than a second topic tree. They do not repeat client tokens or region-to-client mappings across Topics. React keeps exact topic, query, Feed Route, failure, status, and metrics inference without importing transport implementations. Published adapters must keep their contract entrypoint and accepted Source Definition options browser-safe, while server and platform code remains independently testable and replaceable through Effect Layers. Public type tests prove valid inference and reject missing, extra, mismatched, or structurally forged definitions and aggregate Layer entries without requiring `as const`.
