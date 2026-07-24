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
Schema, Effect, Schedule, and function leaves retain their exact executable
identity; other object instances are rejected so mutable clients, sockets,
Dates, Maps, and similar platform values cannot masquerade as portable options.
A leased declaration uses `leasedSource(routeBy, options)` with a non-empty
exact route-field tuple. Consumers do not need `as const`.

Every Source-Owned Topic in the SDK path has an exact required
`id: Schema.String` and one canonical `source`:

```ts
import { Schema } from "effect";
import { defineViewServerConfig } from "effect-view-server/config";
import { exampleSource } from "./example-source";

const Order = Schema.Struct({
  id: Schema.String,
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
    metrics: ({ topic, definition, target }) => {
      void topic;
      void definition;
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
outer adapter Layer.

The metrics reader receives the exact View Server Topic, portable Source
Definition options, and Materialized or Leased target for the bound source.
Adapters therefore keep metrics local to each Topic binding and each leased
route even when multiple definitions share one adapter service.

The generated service remains an explicit production runtime requirement:

```ts
import { Effect } from "effect";
import { makeViewServerRuntime } from "effect-view-server/runtime";
import { ExampleAdapterLive } from "./example-adapter-live";
import { viewServer } from "./view-server";

const runtime = makeViewServerRuntime(viewServer).pipe(Effect.provide(ExampleAdapterLive));
```

Without the matching Layer, composition fails before the WebSocket/HTTP server
starts. Operational attempt failure is supervised independently and does not
stop unrelated Topics or transports.

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
adapters. Its Materialized contract covers ordered and concurrent lanes,
Delivery settlement for success, typed failure, defect, and interruption,
Rejection continuation, retries, exhaustion, metrics, invalid attempt metadata,
and awaited exactly-once finalization. Its Leased contract covers same-route
sharing, distinct-route isolation, diagnostics without acquisition, release,
and route incongruence.

Callback-driven adapters can additionally provide `exerciseCallbackBuffer` and
enable `callbackBuffer: true`. The shared checks require bounded
backpressurable ordering, deterministic non-pausable overflow, high-water and
overflow metrics, and awaited registration finalization.

Published adapter packages can register
`registerSourceAdapterPackageConformance(...)`. The package snapshot checks the
portable `./contract`, `./server`, and platform exports; exact and tested Effect
peer combinations; matching development peers; absence of bundled peer
runtimes; nominal Definition linkage; positive and negative public type
contracts; Schema fidelity; browser purity and gzip budget; and platform
`layer`/`layerConfig` resource validation plus exact runtime-service provision.
The package must supply a real scoped inspector Layer, so the same reusable
suite can validate its built artifact rather than relying on source-only
assertions.

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
