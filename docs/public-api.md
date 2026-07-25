# Public API

## Define Topics

Application code starts with `defineViewServerConfig`. Each topic has an Effect
Schema and a string row key field. The topic schema is the source of truth for
query typing, runtime validation, protocol encoding, and in-memory tests.

```ts
import { defineViewServerConfig, grpc, kafka } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { Config, Schema } from "effect";
import { ordersService } from "./generated/grpc";
import { OrdersValueSchema } from "./generated/orders";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
});

export const viewServer = defineViewServerConfig({
  kafka: {
    usa: Config.string("KAFKA_USA_BOOTSTRAP"),
    london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
  },
  grpc: {
    clients: {
      orders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://orders-grpc.example.com",
      }),
    },
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceOrdersUsa",
        regions: ["usa"],
        value: kafka.protobuf(OrdersValueSchema),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
    trades: {
      schema: Trade,
      key: "id",
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useLiveGrid, useViewServerHealthSummary } =
  viewServerReact;
```

### Live query vs live grid

- Use **`useLiveQuery(topic, query)`** when React owns a fixed Live Query and renders `rows` (client-side grids, simple lists).
- Use **`useLiveGrid(topic)`** for dumb virtualized / viewport tables. The hook returns a **datasource** plus Live Query chrome (`totalRows`, `version`, `status`, …) **without** a public `rows` list. The grid calls:

```ts
const { datasource, totalRows, status, version } = useLiveGrid("orders");

datasource.init({
  setRowCount(count) {
    /* drive scroll height / chrome */
  },
  setRowData(rowData) {
    /* absolute index → row map, same shape as AG Grid Viewport */
  },
});

// Full-state replace: every field is required (no partial patches).
// Clear filters with where: []; clear sort with orderBy: [].
// After filter/sort/column changes, include the new window (usually top of grid).
datasource.onChange({
  mode: "raw",
  firstRow: 0,
  lastRow: 49,
  select: ["id", "price", "status"], // visible columns only
  where: [], // required; [] matches all
  orderBy: [{ field: "price", direction: "asc" }], // required; [] allowed
});

// Scroll / viewport only — keeps the active query plan (select/where/orderBy/groupBy).
// Prefer this over onChange for high-frequency window moves; it is the path reserved for
// server page-cache / seek optimizations (warm pages above/below the cursor).
datasource.onScroll(50, 99);
```

`onChange` is a full-state replace (`raw` | `grouped`) including the window.  
`onScroll(firstRow, lastRow)` re-windows the **active** query only (requires a prior successful `onChange`). Do not call both for a filter reset: one `onChange` with the new query and top window is enough. Live updates use the same WebSocket subscription path as `useLiveQuery` and push into the sink.

### Schema value admission

Topic values must have one canonical identity across the in-memory engine, gRPC,
and the NDJSON Wire Protocol. Use ordinary Effect Schema constructors for
JSON-faithful primitives and structural composition. Use `viewSchema` for the
supported Effect values whose runtime equality or codec requires an explicit
View Server identity witness:

- `viewSchema.BigDecimal`
- `viewSchema.Option(value)`
- `viewSchema.Chunk(value)`
- `viewSchema.HashMap(key, value)`
- `viewSchema.HashSet(value)`

Define a concrete `Schema.Class` normally, then admit that exact class before
using it in a Topic schema:

```ts
import { defineViewServerConfig, viewSchema } from "effect-view-server/config";
import { Schema } from "effect";

class Profile extends Schema.Class<Profile>("Profile")(
  {
    id: Schema.String,
    score: Schema.NumberFromString,
    backup: viewSchema.Option(Schema.String),
  },
  { title: "Profile" },
) {
  label(): string {
    return `${this.id}:${this.score}`;
  }
}

viewSchema.admitClass(Profile);

export const profiles = defineViewServerConfig({
  topics: {
    profiles: {
      schema: Profile,
      key: "id",
    },
  },
});
```

Admission is attached to the exact schema declaration and is idempotent, so the
same concrete class may be admitted more than once and independent classes are
admitted separately. Supply class annotations in the `Schema.Class` definition
before admission. Derived codecs produced by operations such as
`Profile.annotate(...)` are distinct schemas and do not inherit admission or the
root Class field Interface.

The Topic Row type is derived from `Profile.fields`. Class methods remain
available on decoded `Profile` instances, but they are not Topic columns and
cannot appear in keys, source mappings, `select`, `where`, `orderBy`, `groupBy`,
aggregates, or patches.

Topics without `kafkaSource` or `grpcSource` are externally/manual published
topics, for example through TCP publish or an in-memory test client. A topic can
only have one source owner.

Runtime reset is rejected while any source-owned topic exists. Resetting only
manual topics while Kafka or gRPC topics continue running would make the public
contract ambiguous, so source-backed rebuilds should be driven by source replay
or by restarting a runtime with the intended Kafka start position.

Kafka source topics must define `rowKey`. The runtime uses that value as the
topic row key and forces the configured key field on the mapped row to match it,
so source-owned rows cannot drift away from their Kafka identity.

gRPC source topics bind their runtime stream beside the topic schema:

- `grpc.topicSources(grpcClients).materialized({ schema, key, client, method, ... })`
  creates a topic definition backed by a startup-materialized gRPC stream.
- `grpc.topicSources(grpcClients).leased({ schema, key, routeBy, client, method, ... })`
  creates a topic definition backed by a lazy shared gRPC stream per route key.
- `client` is a key from `grpc.clients`, and `method` is a server-streaming
  method on that generated client.

For leased topics, the `routeBy` tuple defines the required fields of an exact
query `routeBy` object. That object selects one Leased Feed, and the source
`request` callback receives those values unchanged to build the upstream gRPC
request. `where` remains independent local filtering within that feed.

## React Provider

Production React code passes a runtime URL to the provider. The provider owns
the remote Effect RPC WebSocket client. Application components use hooks from
the same `createViewServerReact(viewServer)` binding object.

```tsx
export function AppRoot() {
  return (
    <ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <App />
    </ViewServerProvider>
  );
}
```

## Live Queries

Raw queries must declare `select`. This prevents accidentally returning every
column from wide topics.

```tsx
function Orders() {
  const orders = useLiveQuery("orders", {
    select: ["id", "price", "status"],
    where: [
      { field: "status", type: "equals", filter: "open" },
      { field: "customerId", type: "startsWith", filter: "customer-" },
      { field: "price", type: "greaterThanOrEqual", filter: 10 },
    ],
    orderBy: [{ field: "price", direction: "desc" }],
    limit: 20,
  });

  return <pre>{JSON.stringify(orders.rows, null, 2)}</pre>;
}
```

The root `where` array is an implicit `AND`. Its entries are typed Field
Conditions or recursive `AND`, `OR`, and `NOT` expressions. An omitted `where`,
`where: []`, and empty generated groups mean no filter. Field-keyed objects and
shorthand operator properties are rejected. See [Query
Semantics](./query-semantics.md) for the complete condition language.

Grouped queries use `groupBy` plus an aggregate object keyed by output alias.
Aggregate aliases become fields on the returned row type. Their `where`, when
present, uses the same canonical expressions and filters source Topic Rows
before grouping.

```tsx
const totals = useLiveQuery("orders", {
  where: [{ field: "price", type: "greaterThanOrEqual", filter: 10 }],
  groupBy: ["status"],
  aggregates: {
    rowCount: { aggFunc: "count" },
    totalPrice: { aggFunc: "sum", field: "price" },
    maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
  },
  orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
  limit: 10,
});
```

The public API is designed so consumers do not need `as const` to keep type
safety for normal `select`, `where`, `orderBy`, `groupBy`, and aggregate
queries.
