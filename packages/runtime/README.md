# @effect-view-server/runtime

> This is a private implementation package. Applications install
> `effect-view-server` and import the public runtime from
> `effect-view-server/runtime`.

The runtime composes Runtime Core with Effect RPC over WebSocket, fresh HTTP
health and metrics reads, and the optional TCP publisher Adapter. Source
Adapters are ordinary Effect Layers supplied by the application; this Module
does not own Kafka or gRPC clients, source declarations, or transport-specific
runtime options.

## Entrypoint

Use `NodeRuntime.runMain` at the process edge so interruption closes the
WebSocket server, TCP publisher, Runtime Core, and every supplied Source Adapter
Layer in scope.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "0.0.0.0",
    websocketPort: 8080,
    tcpPublishHost: "127.0.0.1",
    tcpPublishPort: 8081,
  }),
);
```

`runViewServerRuntime` keeps the process alive until interrupted. Its options
configure only the runtime server and optional TCP publisher.

RFC 7692 `permessage-deflate` WebSocket compression is enabled by default. Set
`websocketCompression: false` when server CPU or latency matters more than reduced
egress, and benchmark the tradeoff with representative payloads and fanout.

## Source Adapter Layers

The browser-safe config declares each source on the Topic's one canonical
`source` property. Node-only infrastructure is provided as an Effect Layer:

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { grpcNode } from "effect-view-server/grpc/node";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

const SourcesLive = Layer.mergeAll(
  kafkaNode.layer(viewServer, {
    consumerGroupPrefix: "orders-view-v1",
    regions: {
      primary: {
        bootstrapServers: "kafka.internal:9092",
      },
    },
  }),
  grpcNode.layer(viewServer, {
    orders: {
      baseUrl: "https://orders.internal.example",
    },
  }),
);

runViewServerRuntime(viewServer).pipe(Effect.provide(SourcesLive), NodeRuntime.runMain);
```

The runtime has no adapter registry or transport-name dispatch. The nominal
Source Adapter service carried by each Source Definition determines the Layer
requirement. Missing Layers fail composition before the server starts.

See:

- [`docs/runtime-config.md`](../../docs/runtime-config.md) for server options.
- [`docs/source-adapter-sdk.md`](../../docs/source-adapter-sdk.md) for the
  contract/server Layer seam.
- [`docs/kafka-source-adapter.md`](../../docs/kafka-source-adapter.md) and
  [`docs/grpc-source-adapter.md`](../../docs/grpc-source-adapter.md) for the
  first-party Layers.
- [`docs/health-and-metrics.md`](../../docs/health-and-metrics.md) for aggregate
  and exact Source Health.
- [`docs/migration-canonical-source.md`](../../docs/migration-canonical-source.md)
  for the breaking migration.

## Health and metrics

`GET /health` and `GET /metrics` perform fresh, coalesced runtime health reads.
React applications consume pushed aggregate health and exact Topic-bound Source
Health through the public hooks; they do not poll these infrastructure routes.
Adapter-specific metrics remain nested in exact Source Health instead of being
flattened into transport-specific aggregate fields.

## TCP publisher

`tcpPublishPort` enables the bounded NDJSON publisher Adapter for source-free
Topics. It defaults to `127.0.0.1` through the separate `tcpPublishHost` option
and never inherits the public WebSocket host. Source-owned Topics reject direct
publish, patch, and delete operations.

The TCP publisher uses the same Runtime Core mutation boundary as in-memory
testing. Invalid payloads fail decoding before mutation, and Runtime Core
preserves typed ownership and row-validation errors.
