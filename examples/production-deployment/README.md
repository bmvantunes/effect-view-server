# Production Deployment Example

This is a deployment recipe. The runnable source-specific applications live in
the sibling example directories.

## Runtime entrypoint

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Config, Effect } from "effect";
import { grpcNode } from "effect-view-server/grpc/node";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

const KafkaLive = kafkaNode.layerConfig(viewServer, {
  consumerGroupPrefix: Config.string("KAFKA_GROUP_PREFIX"),
  regions: {
    usa: { bootstrapServers: Config.string("KAFKA_USA_BOOTSTRAP") },
    london: { bootstrapServers: Config.string("KAFKA_LONDON_BOOTSTRAP") },
  },
});
const GrpcLive = grpcNode.layerConfig(viewServer, {
  orders: { baseUrl: Config.string("ORDERS_GRPC_URL") },
  strategies: { baseUrl: Config.string("STRATEGIES_GRPC_URL") },
});

const program = Effect.gen(function* () {
  const websocketPort = yield* Config.number("VIEW_SERVER_WEBSOCKET_PORT");
  return yield* runViewServerRuntime(viewServer, {
    host: "0.0.0.0",
    websocketPort,
    tcpPublishHost: "127.0.0.1",
    tcpPublishPort: 8081,
    healthPath: "/health",
    metricsPath: "/metrics",
  });
}).pipe(Effect.provide([KafkaLive, GrpcLive]));

NodeRuntime.runMain(program);
```

The shared config owns browser-safe Source Definitions. Kafka/gRPC clients,
endpoints, credentials, and TLS are resolved by aggregate Node Layers.

Use a deployment-unique `KAFKA_GROUP_PREFIX`. Each Kafka Topic Source owns its
start position. The current recommended deployment topology is one active View
Server process per logical deployment.

## React entrypoint

```tsx
import { ViewServerProvider } from "./view-server.config";

export function AppRoot() {
  return (
    <ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <App />
    </ViewServerProvider>
  );
}
```

Inject the browser URL at deploy time:

```html
<script>
  window.__APP_CONFIG__ = {
    VIEW_SERVER_URL: "wss://view-server.example.com/rpc",
  };
</script>
```

## Kubernetes sketch

Expose port 8080 for WebSocket, health, and metrics. Use `/health` for
startup/readiness and a TCP or process check for liveness. Keep optional TCP
publish on a private interface.

Provide `KAFKA_GROUP_PREFIX`, region broker addresses, and gRPC base URLs from
Secrets or ConfigMaps. A missing value fails Layer construction before the
runtime binds ports.

Size CPU and memory using:

```sh
vp run -w release-candidate:capacity
```

Use production-like Topic counts, rows, grouped queries, Kafka rates, gRPC Feed
Routes, and WebSocket fanout when evaluating capacity.
