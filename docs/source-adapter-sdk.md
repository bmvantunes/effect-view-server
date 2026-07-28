# Source Adapter SDK

The Source Adapter SDK is the transport-neutral ingestion boundary for custom
View Server sources. It has three public surfaces:

- `effect-view-server/source-adapter` is the browser-safe contract.
- `effect-view-server/source-adapter/server` implements the generated Effect
  service and scoped source attempts.
- `effect-view-server/source-adapter/testing` provides the controllable fixture
  and reusable `@effect/vitest` conformance suites.

There is no runtime plugin registry or adapter-name dispatch. A Source
Definition and its runtime implementation are linked by the nominal
`Context.Service` created by `SourceAdapter.make(...)`.

## Define the portable contract

The adapter declares its identity and complete failure Schema once. Every
supported lifecycle also declares mandatory metrics, rejection-location, and
definition-option contracts.

```ts
import { Schema } from "effect";
import {
  SourceAdapter,
  type SourceDefinitionOptionsFamily,
} from "effect-view-server/source-adapter";

const ExampleFailure = Schema.TaggedStruct("ExampleFailure", {
  message: Schema.String,
});
const ExampleMetrics = Schema.Struct({
  connected: Schema.Boolean,
  received: Schema.BigInt,
});
const ExampleLocation = Schema.Struct({
  offset: Schema.BigInt,
});

type ExampleSourceOptions<Row extends object> = {
  readonly stream: string;
  readonly initial: Row;
};

interface ExampleSourceOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: ExampleSourceOptions<this["Row"]>;
}

export const ExampleAdapter = SourceAdapter.make({
  identity: {
    name: "example-stream",
    version: "1",
  },
  failure: ExampleFailure,
  materialized: {
    metrics: ExampleMetrics,
    rejectionLocation: ExampleLocation,
    definitionOptions: SourceAdapter.definitionOptionsFamily<ExampleSourceOptionsFamily>(),
  },
  leased: undefined,
});

export const exampleSource = <Row extends object>(stream: string, initial: Row) =>
  ExampleAdapter.materializedSource<Row>({ stream, initial });
```

`materializedSource(...)` snapshots and freezes plain-data option subtrees.
Schema, Effect, Schedule, function leaves, and adapter-owned opaque values
marked with `SourceAdapter.executable(...)` retain their exact executable
identity; other object instances are rejected so mutable clients, sockets,
Dates, Maps, and similar platform values cannot masquerade as portable options.
A leased declaration uses `leasedSource(routeBy, options)` with a non-empty
exact route-field tuple. Consumers do not need `as const`.

Every Source-Owned Topic in the SDK path has an exact required
`id: ViewServerId` and one canonical `source`:

```ts
import { Schema } from "effect";
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { exampleSource } from "./example-source";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: exampleSource("orders", {
        id: "initial",
        price: 10,
      }),
    },
  },
});
```

## Implement the generated server service

`SourceAdapterServer.make(...)` requires exactly the lifecycles declared by the
portable handle. Acquisition runs in a fresh attempt Scope. Each attempt returns
one or more non-empty, uniquely named lanes; events are sequential inside a lane
and sibling lanes run concurrently.

```ts
import { Chunk, Effect, Schedule, Stream } from "effect";
import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
import { ExampleAdapter } from "./example-source";

export const ExampleAdapterLive = SourceAdapterServer.make(ExampleAdapter, {
  materialized: {
    acquire: (input) =>
      Effect.gen(function* () {
        const mutation = yield* input.toolkit.upsert(input.definition.initial);
        const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
        return SourceAdapterServer.attempt([
          SourceAdapterServer.lane({
            id: "example",
            events: Stream.make(delivery).pipe(Stream.concat(Stream.never)),
          }),
        ]);
      }),
    metrics: ({ topic, definition, lifetimeScope, target }) => {
      void topic;
      void definition;
      void lifetimeScope;
      void target;
      return Effect.succeed({
        connected: true,
        received: 1n,
      });
    },
    retry: Schedule.recurs(0),
  },
});
```

Real adapters acquire consumers, subscriptions, iterators, callbacks, and
leases inside `acquire`. Attempt finalizers run before retry, lease release, or
runtime shutdown. Shared pools and concrete transport resources belong in the
outer adapter Layer. Both `acquire` and `metrics` also receive the same
`lifetimeScope` for one logical materialized runtime or leased-feed lifetime.
It stays stable across supervised attempt retries and closes when that logical
lifetime ends, so adapters may own frozen initial-position caches and similar
lifetime-local state without retaining it across runtime restarts. Transport
consumers and stream resources still belong to the ambient attempt Scope, not
`lifetimeScope`.

The metrics reader receives the exact View Server Topic, portable Source
Definition options, and Materialized or Leased target for the bound source.
Adapters therefore keep metrics local to each Topic binding and each leased
route even when multiple definitions share one adapter service.

The generated service remains an explicit production runtime requirement:

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { ExampleAdapterLive } from "./example-adapter-live";
import { viewServer } from "./view-server";

runViewServerRuntime(viewServer).pipe(Effect.provide(ExampleAdapterLive), NodeRuntime.runMain);
```

Without the matching Layer, composition fails before the WebSocket/HTTP server
starts. Operational attempt failure is supervised independently and does not
stop unrelated Topics or transports. If an application uses the finite
`makeViewServerRuntime` constructor instead, it must build and use the returned
runtime inside the same scope that provides the adapter Layer so shared adapter
resources remain alive until runtime close.

## Delivery, rejection, and health

The Topic-bound toolkit exposes only complete-row Upsert, canonical-ID Delete,
non-empty Delivery, and item Rejection constructors. It exposes no Runtime
Client, publish callback, subscriber, session, mutable config, Topic Store, or
raw Schema bypass.

Runtime Core applies a Delivery in order and calls settlement exactly once with
the complete application `Exit`. Applied mutations are not rolled back if a
later mutation or settlement fails. An item-local Rejection publishes sticky
Degraded health before ordered rejection settlement, then continues the lane
when settlement succeeds.

`liveClient.subscribeSourceHealth(...)` is the framework-neutral scoped
diagnostics API:

- Materialized Topics accept no route and emit active Source Health.
- Leased Topics require one exact route and emit `Inactive` or `Active`.
- Source-free Topics are rejected by the public type.
- Diagnostics do not acquire or retain a leased feed.

Source Health includes the exact adapter identity, target, status, runtime
metrics, adapter metrics, and epoch-nanosecond `bigint` sample time. Metrics are
sampled through Effect Clock once per second; lifecycle transitions and
rejections publish immediately from the cached metrics snapshot. The production
wire path remains Effect RPC WebSocket with NDJSON and configured Schemas.

## Conformance and performance

`SourceFixture.make(RowSchema)` supplies controllable Materialized and Leased definitions,
an adapter Layer, deliveries, rejections, failures, completion, metrics changes,
and finalization counters. `registerSourceAdapterConformance(...)` registers the
shared scoped Layer/TestClock suite used by Runtime Core and future published
adapters. Registration requires at least one explicitly enabled capability, and
the Driver supplies adapter-specific failure, rejection-location, and metrics
expectations so assertions remain exact without depending on the built-in
fixture's Schema. Every declared lifecycle runs the same complete contract for
ordered and concurrent lanes, Delivery settlement for success, typed failure,
defect, and interruption, Rejection continuation, retries, exhaustion, metrics,
invalid attempt metadata, and awaited exactly-once finalization. Leased
lifecycles additionally cover same-route sharing, distinct-route isolation,
diagnostics without acquisition, release, and route incongruence.

Callback-driven adapters can additionally provide `callbackBridge` and enable
`callbackBridge: true` alongside Materialized lifecycle conformance. The shared checks require bounded
backpressurable ordering, deterministic non-pausable overflow, high-water and
overflow metrics, and awaited registration finalization.

Published adapter packages can register
`registerSourceAdapterPackageConformance({ inspection, behavioral })`. The
package snapshot checks the
portable `./contract`, `./server`, and platform exports; exact and tested Effect
peer combinations; matching development peers; absence of bundled peer
runtimes; an exact probe for every lifecycle declared by the built adapter;
nominal Definition linkage; contract-linked positive and negative public type
expressions; Schema fidelity; pre-tree-shake browser dependency purity and gzip budget; and platform
`layer`/`layerConfig` resource validation plus exact runtime-service provision.
The linked-package check requires the behavioral Driver to carry the exact
adapter handle and lifecycle Definitions imported from that inspected built
`./contract`. Every built lifecycle and every Driver capability must be enabled
for the behavioral registration, which then runs the full suite against the
same Driver Layer. Type evidence counts only assertions and negative cases that
directly reference bindings imported from the exact contract entry, and the
browser build always starts from that same manifest export.

Repository package-surface validation rejects deep/internal imports and
browser/server dependency leaks. The real portable-facade browser fixture has a
32 KiB gzipped budget:

```sh
vp test run scripts/source-adapter-browser-contract.test.ts
```

The focused core benchmark covers ordered Source Lane Event processing,
rejection continuation, one-second sampling across 32 active sources, and
nominal adapter runtime resolution across 1,024 Source Definitions:

```sh
vp run @effect-view-server/runtime-core#bench:source-adapter-core
```
