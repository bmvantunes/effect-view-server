# effect-view-server

Typed Effect View Server for live query snapshots, deltas, React bindings, and runtime ingress adapters.

This package intentionally has no root export. Import from explicit subpaths so applications only load the surface they use:

```ts
import { defineViewServerConfig } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { SourceAdapter } from "effect-view-server/source-adapter";
import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
```

Adapter tests and reusable conformance suites are exported from
`effect-view-server/source-adapter/testing`. That optional testing surface
requires the exact matching `@effect/vitest` peer. The portable
`effect-view-server/source-adapter` surface is browser-safe and is verified
against a 32 KiB gzipped fixture budget.

Topic schemas that use Effect `Option`, `Chunk`, `HashMap`, or `HashSet` values
use the corresponding `viewSchema` factory from `effect-view-server/config`.
`viewSchema.BigDecimal` is the admitted `Schema.BigDecimal` declaration, so
either spelling is valid. Admit each concrete schema class explicitly after
definition:

```ts
import { viewSchema } from "effect-view-server/config";
import { Schema } from "effect";

class Profile extends Schema.Class<Profile>("Profile")({ id: Schema.String }) {}
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

React applications should install the package and compatible peer dependencies:

```sh
npm install effect-view-server effect react react-dom @effect/atom-react
```

See the repository README, Public API guide, and Query Semantics guide for
schema admission, canonical filters, Kafka, gRPC, TCP publishing, in-memory
testing, and React usage.
