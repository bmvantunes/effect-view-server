# Deployment

## Runtime process

Compose adapter Layers at the Node application edge:

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
    eu: { bootstrapServers: Config.string("KAFKA_EU_BOOTSTRAP") },
  },
});
const GrpcLive = grpcNode.layerConfig(viewServer, {
  orders: { baseUrl: Config.string("ORDERS_GRPC_URL") },
});

runViewServerRuntime(viewServer, {
  host: "0.0.0.0",
  websocketPort: 8080,
  tcpPublishHost: "127.0.0.1",
  tcpPublishPort: 8081,
  healthPath: "/health",
  metricsPath: "/metrics",
}).pipe(Effect.provide([KafkaLive, GrpcLive]), NodeRuntime.runMain);
```

`NodeRuntime.runMain` turns process signals into Effect interruption and runs
Source Attempt and Layer finalizers.

## Kubernetes

Use `GET /health` for startup/readiness, `GET /metrics` for Prometheus, and a
process or TCP liveness check. Degraded sources remain queryable and must not
cause a restart that discards retained in-memory rows.

Broker endpoints, gRPC base URLs, credentials, TLS, and pools belong to adapter
`layerConfig(...)` values. Missing configuration fails Layer construction
before server ports open.

## Network surface

- browser clients: Effect RPC WebSocket with NDJSON
- health and metrics: same HTTP/WebSocket server
- optional TCP publish: separate private host and port, source-free Topics only

Browser headers and sessions are not forwarded to upstream gRPC. Put upstream
authentication in the Node Layer and represent authorization-sensitive leased
datasets through explicit Feed Route fields.

## Recovery

Runtime Core state is in memory. Kafka can replay from a Source
Definition-owned authoritative start position. Materialized gRPC reconnects and
replays according to its upstream contract. Leased gRPC rebuilds on demand.
External TCP publishers must own replay for source-free Topics.

## Release gate

Before promoting a runtime image:

```sh
vp run -w release-candidate:capacity
```

The gate serially covers examples, builds, package seams, strict Effect
diagnostics, tests, coverage, engine/read-write/WebSocket baselines, and the
canonical Kafka and gRPC Source Adapter profiles.
