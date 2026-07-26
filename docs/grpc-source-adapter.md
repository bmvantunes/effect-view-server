# gRPC Source Adapter

The first-party gRPC integration is an ordinary Source Adapter with
Materialized and Leased lifecycles. The browser-safe contract consumes generated
service descriptors; the Node Layer owns transports, endpoints, interceptors,
credentials, and concrete ConnectRPC clients.

## Define Sources

Use the generated service descriptors from your protobuf output. Do not create
ConnectRPC clients in the shared config module.

```ts
import { Schema } from "effect";
import { defineViewServerConfig } from "effect-view-server/config";
import { grpc } from "effect-view-server/grpc/contract";
import { OrdersService } from "./generated/orders_pb";

const grpcSources = grpc.topicSources({
  orders: OrdersService,
});

const Order = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  total: Schema.Number,
});
type OrderRow = typeof Order.Type;

export const viewServer = defineViewServerConfig({
  topics: {
    allOrders: {
      schema: Order,
      source: grpcSources.materialized({
        client: "orders",
        method: "streamOrders",
        request: () => ({ region: "all" }),
        map: ({ value }) => ({
          id: value.orderId,
          region: value.region,
          total: value.total,
        }),
      }),
    },
    regionalOrders: {
      schema: Order,
      source: grpcSources.leased({
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: (route) => ({ region: route.region }),
        map: ({ value, route }): OrderRow => ({
          id: value.orderId,
          region: route.region,
          total: value.total,
        }),
      }),
    },
  },
});
```

The descriptor record, `client`, server-streaming `method`, request object,
Mapping input, Mapping result, Topic Row, and Feed Route remain exact without
consumer casts or `as const`. Unary, client-streaming, and bidirectional methods
are not valid Source methods. Request factories and Mappings are synchronous.

Materialized sources start with Runtime Core. A Leased source starts when the
first Live Query acquires an exact Feed Route. Queries using the same route
share one frozen request and upstream stream even when their local selection,
filtering, sorting, grouping, pagination, or window differs. The final owner
release cancels the invocation and removes that route's retained rows.

## Compose The Node Layer

The Node options contain all and only logical clients referenced by the gRPC
Source Definitions:

```ts
import { Config } from "effect";
import { grpcNode } from "effect-view-server/grpc/node";
import { viewServer } from "./view-server";

export const GrpcLive = grpcNode.layerConfig(viewServer, {
  orders: {
    baseUrl: Config.string("ORDERS_GRPC_BASE_URL"),
  },
});
```

Use `grpcNode.layer(...)` for already-resolved values and ConnectRPC
interceptors:

```ts
const GrpcLive = grpcNode.layer(viewServer, {
  orders: {
    baseUrl: "https://orders.internal.example",
    interceptors: [upstreamCredentials],
  },
});
```

The Layer constructs each transport/client once and stores it in an O(1)
logical-client map. Every invocation, iterator, cancellation controller, and
subscription belongs to a fresh scoped Source Attempt.

When `transport.sessionManager` is omitted, the Node Layer creates the HTTP/2
session manager and closes it with the Layer. A supplied
`transport.sessionManager` remains caller-owned and the Layer never closes or
aborts it.

Browser request headers, cookies, authorization values, and session identity
are never forwarded upstream. Configure upstream TLS, authentication, metadata,
and credential refresh through the Node Layer and its interceptors. If caller
identity changes the upstream dataset, represent the authorized distinction as
an explicit Feed Route field.

## Failure, Rejection, And Metrics

Invocation or stream failure ends the current Source Attempt and follows the
selected retry Schedule. Request factories run once per logical source/feed
lifetime, and retries reuse the same frozen request object.

A Mapping throw, invalid complete Topic Row, invalid canonical ID, or Leased
route mismatch becomes a safe Source Item Rejection. Runtime Core records
sticky Degraded health before the adapter pulls the next decoded response, so a
later valid response continues through the same stream. Transport/framing
failure is an attempt failure rather than an item rejection.

`GrpcAdapterFailure`, `GrpcRejectionLocation`, `GrpcMaterializedMetrics`, and
`GrpcLeasedMetrics` are browser-safe Schema-backed contracts exported from
`effect-view-server/grpc/contract`. Metrics reads are local and never perform an
upstream health request.

The generated-descriptor browser contract has a 64 KiB gzip budget. The
contract graph excludes the Node transport, concrete clients, endpoints,
credentials, server implementation, and Node APIs.
