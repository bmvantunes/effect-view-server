# effect-view-server

Typed Effect View Server for live query snapshots, deltas, React bindings, and runtime ingress adapters.

This package intentionally has no root export. Import from explicit subpaths so applications only load the surface they use:

```ts
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { SourceAdapter } from "effect-view-server/source-adapter";
import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
import { kafka } from "effect-view-server/kafka/contract";
import { kafkaNode } from "effect-view-server/kafka/node";
import { grpc } from "effect-view-server/grpc/contract";
import { grpcNode } from "effect-view-server/grpc/node";
```

Adapter tests and reusable conformance suites are exported from
`effect-view-server/source-adapter/testing`. That optional testing surface
requires the exact matching `@effect/vitest` peer. Its package-conformance
checks use the required TypeScript and Vite peers. The portable
`effect-view-server/source-adapter` surface is browser-safe and is verified
against a 32 KiB gzipped fixture budget.

Every Topic Schema has one exact required `id: ViewServerId`. Topics declare
zero or one nominal Source Definition at `source`; there is no configurable
Topic key or transport-specific Topic property. Kafka and gRPC platform
implementations are ordinary Layers provided to the transport-neutral runtime.
The combined real-browser fixture for config plus Kafka/gRPC contracts is
capped at 128 KiB gzip; `/server` and `/node` code is forbidden from that graph.

Topic schemas that use Effect `Option`, `Chunk`, `HashMap`, or `HashSet` values
use the corresponding `viewSchema` factory from `effect-view-server/config`.
`viewSchema.BigDecimal` is the admitted `Schema.BigDecimal` declaration, so
either spelling is valid. Admit each concrete schema class explicitly after
definition:

```ts
import { ViewServerId, viewSchema } from "effect-view-server/config";
import { Schema } from "effect";

class Profile extends Schema.Class<Profile>("Profile")({
  id: ViewServerId,
  displayName: viewSchema.Option(Schema.String),
}) {}
viewSchema.admitClass(Profile);
```

Class methods remain domain behavior and are not exposed as Topic Row columns.

Live-query filters use one canonical recursive format. The root `where` value is
an implicit-`AND` array; cross-field Boolean logic uses explicit nested groups:

```ts
const orders = react.useLiveQuery("orders", {
  select: ["id", "status", "price"],
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
});
```

An omitted `where`, `where: []`, and empty generated groups mean no filter.
Field-keyed `where` objects and shorthand operators are invalid. Leased topics
also require their exact, independently typed `routeBy` object.

Virtualized grids use `react.useLiveQueryViewport(topic)`. Its `replace`
operation binds a typed query and sparse row sink to an inclusive absolute
window, while the returned generation's `setWindow` operation handles
scroll-only changes. Each replacement is switch-latest: obsolete snapshots,
deltas, statuses, failures, and sink writes are ignored. See the Public API
guide for the complete adapter contract.

React applications should install the package and compatible peer dependencies:

```sh
npm install effect-view-server effect react react-dom @effect/atom-react
```

See the repository README, Public API guide, and Query Semantics guide for
schema admission, canonical filters, Kafka, gRPC, TCP publishing, in-memory
testing, and React usage.
