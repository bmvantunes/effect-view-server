# Effect View Server

## Guides

- [Public API](./docs/public-api.md)
- [Runtime Config](./docs/runtime-config.md)
- [Kafka Mapping](./docs/kafka-mapping.md)
- [In-Memory Browser Testing](./docs/in-memory-browser-testing.md)
- [Health And Metrics](./docs/health-and-metrics.md)
- [Query Semantics](./docs/query-semantics.md)
- [Benchmarks And Capacity](./docs/benchmarks-and-capacity.md)
- [Deployment](./docs/deployment.md)
- [Operations](./docs/operations.md)
- [Examples](./examples/README.md)

## Install

Core/server-only consumers need only the main package:

```sh
npm install effect-view-server
```

React consumers should also install the React subpath peers:

```sh
npm install effect-view-server react react-dom @effect/atom-react
```

## Source-Owned Config

Topics declare their source of truth directly. Kafka regions and gRPC clients are
configured once, then each topic chooses `kafkaSource`, `grpcSource`, or no
source for TCP/manual publishing:

```ts
import { Config, Schema, Stream } from "effect";
import { defineViewServerConfig, grpc, kafka } from "effect-view-server/config";
import { ordersService, strategiesService } from "./generated/grpc";
import { OrdersKeySchema, OrdersValueSchema } from "./generated/orders";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

const Strategy = Schema.Struct({
  id: Schema.String,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const grpcClients = {
  orders: grpc.connectClient({
    service: ordersService,
    baseUrl: "https://orders-grpc.example.com",
  }),
  strategies: grpc.connectClient({
    service: strategiesService,
    baseUrl: "https://strategies-grpc.example.com",
  }),
};

const grpcTopics = grpc.topicSources(grpcClients);

export const viewServer = defineViewServerConfig({
  kafka: {
    usa: Config.string("KAFKA_USA_BOOTSTRAP"),
    london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
  },
  grpc: {
    clients: grpcClients,
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceOrdersUsa",
        regions: ["usa"],
        value: kafka.protobuf(OrdersValueSchema),
        key: kafka.protobuf(OrdersKeySchema),
        rowKey: ({ key }) => key.orderId,
        map: ({ key, value, region }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          strategyId: key.strategyId,
          updatedAt: value.updatedAt,
        }),
      }),
    },
    strategies: grpcTopics.materialized({
      schema: Strategy,
      key: "id",
      client: "strategies",
      method: "streamStrategies",
      request: () => ({ universe: "global" }),
      acquire: ({ client, request }) =>
        Stream.fromAsyncIterable(client.streamStrategies(request), (cause) => cause),
      map: ({ value }) => ({
        id: `${value.strategyId}:${value.region}`,
        strategyId: value.strategyId,
        region: value.region,
        status: value.status,
        notional: value.notional,
        updatedAt: value.updatedAt,
      }),
    }),
    ordersByStrategy: grpcTopics.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["strategyId", "region"],
      request: ({ strategyId, region }) => ({ strategyId, region }),
      acquire: ({ client, request }) =>
        Stream.fromAsyncIterable(client.streamOrders(request), (cause) => cause),
      map: ({ value, route }) => ({
        id: `${route.strategyId}:${route.region}:${value.orderId}`,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        strategyId: route.strategyId,
        updatedAt: value.updatedAt,
      }),
    }),
  },
});
```

The region tuple `["usa"]`, gRPC client names, source topics, route fields, and
mapping inputs/outputs are all type checked. A topic can have only one owner:
Kafka, gRPC, or external/manual publishing.

`grpc.topicSources(grpcClients)` binds concrete gRPC service methods directly to
topic-owned `grpcSource` declarations. Top-level `grpc.clients` stays
infrastructure; runtime options keep operational knobs only.

## Remote React provider

Server code starts a runtime through Effect RPC WebSocket plus same-server
`GET /health` and `GET /metrics` endpoints:

Node entrypoints should use `@effect/platform-node`'s `NodeRuntime.runMain` so
`SIGINT` and `SIGTERM` interrupt the main fiber and run Effect finalizers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server-config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      consumerGroupId: "orders-view-server",
      startFrom: "latest",
    },
  }),
);
```

The runtime derives gRPC feeds from topic-owned source bindings. A materialized
gRPC topic starts at runtime startup. A leased gRPC topic opens one shared
upstream stream per route key when the first matching subscription arrives.

The same-server `GET /health` endpoint performs a fresh runtime health read for
deployment readiness checks. Overlapping concurrent reads are coalesced so they
share one runtime read. Internal `bigint` health fields, such as Kafka lag, are
encoded as decimal strings in the JSON response.

When `tcpPublishPort` is configured, the runtime also opens a non-browser TCP
NDJSON publish endpoint and exposes its `tcpPublishUrl`. That endpoint supports
`publish`, `publishMany`, `patch`, and `delete` commands and routes every
mutation through the same Runtime Core path as Kafka, gRPC, and in-memory tests.
TCP publish is for externally-published topics only; Kafka/gRPC-owned topics are
rejected so one View Server topic has one source of truth. TCP publish has its
own `tcpPublishHost` and defaults to `127.0.0.1`; it does not inherit the public
WebSocket/HTTP host. The TCP endpoint is bounded by connection, line-size, and
queued-command limits.

The same-server `GET /metrics` endpoint performs the same fresh, coalesced
runtime health read and renders Prometheus text exposition from that result. It
exposes scrape-safe runtime, transport, engine, Kafka, and gRPC gauges/counters.
It is not a full mirror of health: high-cardinality values such as raw error
messages and route-specific leased feed keys remain in `GET /health`. Scrape
failures that cannot decode health return `200` with the
`view_server_metrics_error` metric set to `1` so the scrape itself remains
observable. Pushed React health remains cadence-controlled and reads cached
client health; the HTTP routes are not UI polling interfaces.

Browser React code keeps using the normal provider and hooks:

```tsx
import { createViewServerReact } from "effect-view-server/react";
import { viewServer } from "./view-server-config";

const react = createViewServerReact(viewServer);

export function App() {
  return (
    <react.ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <Orders />
    </react.ViewServerProvider>
  );
}

function Orders() {
  const orders = react.useLiveQuery("orders", {
    select: ["id", "price"],
    where: [
      { field: "status", type: "equals", filter: "open" },
      {
        type: "OR",
        conditions: [
          { field: "customerId", type: "startsWith", filter: "customer-" },
          { field: "price", type: "greaterThanOrEqual", filter: 100 },
        ],
      },
    ],
    orderBy: [{ field: "price", direction: "asc" }],
    limit: 20,
  });

  return <pre>{JSON.stringify(orders.rows, null, 2)}</pre>;
}
```

`where` is always an implicit-`AND` array of typed Field Conditions or nested
`AND`, `OR`, and `NOT` expressions. An omitted `where`, `where: []`, and empty
generated groups all mean no filter. Field-keyed objects and shorthand operators
such as `eq`, `gte`, or `contains: "value"` are rejected.

Leased topics additionally require an exact `routeBy` object. Routing is
independent from local filtering, and route values are passed to the source
Adapter without case, accent, or text normalization:

```tsx
const orders = react.useLiveQuery("ordersByStrategy", {
  routeBy: {
    strategyId: "strategy-1",
    region: "ÁbCDEfgh",
  },
  where: [{ field: "status", type: "equals", filter: "open" }],
  select: ["id", "status", "price"],
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 20,
});
```
