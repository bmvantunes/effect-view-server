# Public API

## Topic configuration

`defineViewServerConfig(...)` is the one authored tree shared by React, remote
clients, in-memory composition, and the server runtime.

Every Topic Schema must expose the exact required field `id: ViewServerId`.
Missing, optional, transformed, refined, branded, or non-string IDs are invalid.
A Topic declares zero or one nominal Source Definition through `source`.

```ts
import { Schema } from "effect";
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { grpc } from "effect-view-server/grpc/contract";
import { kafka } from "effect-view-server/kafka/contract";
import { OrdersService } from "./generated/orders_pb";

const Order = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  price: Schema.Number,
  region: Schema.String,
});
const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

const grpcSources = grpc.topicSources({
  orders: OrdersService,
});

export const viewServer = defineViewServerConfig({
  topics: {
    kafkaOrders: {
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
          customerId: value.customerId,
          price: value.price,
          region,
        }),
        startFrom: "earliest",
      }),
    },
    allOrders: {
      schema: Order,
      source: grpcSources.materialized({
        client: "orders",
        method: "streamOrders",
        request: () => ({ region: "all" }),
        map: ({ value }) => ({
          id: value.orderId,
          customerId: value.customerId,
          price: value.price,
          region: value.region,
        }),
      }),
    },
    regionalOrders: {
      schema: Order,
      source: grpcSources.leased({
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ region }),
        map: ({ value, route }) => ({
          id: value.orderId,
          customerId: value.customerId,
          price: value.price,
          region: route.region,
        }),
      }),
    },
    manualOrders: {
      schema: Order,
    },
  },
});
```

The config is snapshotted. Later mutation of the input cannot change runtime
behavior. Structural Source lookalikes are invalid; use an adapter constructor.

Source-free Topics accept Runtime Client and TCP mutations. Source-owned Topics
reject direct publish, patch, delete, and reset according to the Source
Ownership Policy.

## Remote client

Remote live-query, health-summary, and health-detail subscriptions automatically
restart after a WebSocket transport interruption. Source-health subscriptions use
a separate RPC and are not automatically retried. The logical subscription remains
open while the client retries every 500 milliseconds without an attempt or duration
limit. The first failed attempt in an outage emits a non-terminal `TransportError`
status so consumers report the connection as disconnected while retrying. The restarted
RPC stream begins with a fresh authoritative snapshot, which restores ready status;
consumers maintaining their own accumulated view must replace it with that snapshot
before applying later deltas. Query, validation, and other non-transport errors remain
terminal. Calling the subscription's `close` operation or closing the client cancels any
pending retry and releases the server subscription.

`subscriptionBufferSize` bounds the local queue for each remote subscription.
It defaults to `1024`; a non-positive or non-integer value is normalized to `1`.
If the consumer cannot keep up, the subscription emits its typed backpressure
status and closes instead of growing without bound.

## Schema value admission

Use ordinary Effect Schema constructors for JSON-faithful values. The
`viewSchema` factories admit Effect values whose equality or wire codec needs a
View Server witness:

- `viewSchema.BigDecimal`
- `viewSchema.Option(value)`
- `viewSchema.Chunk(value)`
- `viewSchema.HashMap(key, value)`
- `viewSchema.HashSet(value)`

Admit a concrete `Schema.Class` with `viewSchema.admitClass(Profile)`. Class
methods remain domain behavior and cannot be used as Topic columns.

## React

```ts
import { createViewServerReact } from "effect-view-server/react";

export const {
  ViewServerProvider,
  useLiveQuery,
  useLiveQueryViewport,
  useSourceHealth,
  useViewServerHealth,
  useViewServerHealthSummary,
} = createViewServerReact(viewServer);
```

The production provider owns its remote client:

```tsx
export function AppRoot() {
  return (
    <ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <App />
    </ViewServerProvider>
  );
}
```

### Live Queries

Raw queries require an explicit `select`. Grouped queries require
`aggregates`. Query fields, operators, aggregate aliases, order fields, and
Feed Routes are inferred from the selected Topic without consumer casts.

```tsx
const orders = useLiveQuery("manualOrders", {
  select: ["id", "customerId", "price"],
  where: [{ field: "price", type: "greaterThanOrEqual", filter: 10 }],
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 20,
});
```

Leased queries also require exact `routeBy`:

```tsx
const regional = useLiveQuery("regionalOrders", {
  select: ["id", "price"],
  routeBy: { region: "eu" },
});
```

`useLiveQueryViewport(topic)` is the transport-neutral virtual-grid seam. It
pushes sparse rows and their authoritative keys into a caller-owned sink while
React retains only chrome such as status, version, and total rows.

```tsx
const generation = useLiveQueryViewport("manualOrders").viewport.replace({
  window: { firstRow: 100, lastRow: 149 },
  query: {
    select: ["id", "price"],
    where: [],
    orderBy: [{ field: "price", direction: "desc" }],
  },
  sink: {
    setRowCount: (count, keepRenderedRows) => grid.setRowCount(count, keepRenderedRows),
    setRowData: (rowsByIndex, rowKeysByIndex) => grid.setRows(rowsByIndex, rowKeysByIndex),
  },
});
```

Every `setRowData` call contains rows and keys at exactly the same absolute
indexes. Raw queries receive the authoritative public row key; grouped queries
receive the complete canonical group key. Both maps describe the same
`ClientStateChange` and are delivered atomically, so keys remain stable when
rows move or aggregates change. Keys are never derived from viewport position.
Existing sinks that only accept the first `rowsByIndex` argument remain valid.

Structural adapters that need the complete configured Topic Row import the
declaration-only extractor from the pure subpath:

```ts
import type { LiveQueryViewportBaseRow } from "effect-view-server/react/viewport-base-row";

type BaseRow = LiveQueryViewportBaseRow<typeof viewport>;
```

The subpath owns its helper declaration directly, imports neither React nor
Effect, and has an empty runtime module. `effect-view-server/react` re-exports
the same helper for compatibility; it does not maintain a second extractor.

### Source Diagnostics

`useSourceHealth(...)` consumes the same scoped framework-neutral diagnostics
stream as `liveClient.subscribeSourceHealth(...)`.

```tsx
const materialized = useSourceHealth({ topic: "allOrders" });
const leased = useSourceHealth({
  topic: "regionalOrders",
  routeBy: { region: "eu" },
});
```

- Source-free Topics are rejected.
- Materialized Topics accept only `topic`.
- Leased Topics require their exact Feed Route.
- Materialized results contain active Source Health.
- Leased results are exact `Inactive` or `Active`.
- Inactive diagnostics do not start a Leased Feed.
- Matching consumers share one subscription; unmount and client close release it.

Source Health contains exact adapter identity, Source Target, Source Status,
runtime metrics, adapter metrics, typed failure/rejection details, and sampled
nanoseconds. It is pushed; there is no one-shot or polling interface.

## Package seams and browser budget

Only `effect-view-server` is published. Internal workspace packages are private.
Use these adapter seams:

- `effect-view-server/source-adapter`
- `effect-view-server/source-adapter/server`
- `effect-view-server/source-adapter/testing`
- `effect-view-server/kafka/contract`
- `effect-view-server/kafka/server`
- `effect-view-server/kafka/node`
- `effect-view-server/grpc/contract`
- `effect-view-server/grpc/server`
- `effect-view-server/grpc/node`

Contract roots are browser-safe; `/server` and `/node` are not browser entry
points. The real bundle fixture combining config, Kafka contract, gRPC
contract, and generated protobuf descriptors is capped at 128 KiB gzip. The
portable Source Adapter fixture is capped at 32 KiB, and the generated gRPC
contract fixture at 64 KiB.

Deep `src`, `dist`, internal-workspace, and unapproved nested imports are
rejected by package seam checks. Emitted JavaScript and declarations may not
contain private `@effect-view-server/*` specifiers.
