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
  retentionSweepInterval: Config.succeed("15 minutes"),
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

Kafka additionally requires `DESCRIBE_CONFIGS` permission for every configured
topic in every selected Region. Startup snapshots the effective
`cleanup.policy` and `retention.ms` in one batched Admin request per Region and
validates them against each Source Definition before any consumer, sweep,
listener, or server port starts. All discovered Region/topic failures are
reported together with redacted diagnostics.

Broker policy changes are startup-only in this version. To change
`cleanup.policy` or `retention.ms`, stop every affected View Server instance,
change the broker configuration, and restart the instances. Runtime polling is
not implied; mutating policy behind a running instance is outside the
correctness contract.

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

Kafka `match-kafka-retention` projects configured time retention into local row
deadlines. It does not observe exact segment deletion, `retention.bytes`,
tiered-storage state, or when Kafka physically removes a segment.

## Release gate

Before promoting a runtime image:

```sh
vp run -w release-candidate:capacity
```

The gate serially covers examples, builds, package seams, strict Effect
diagnostics, tests, coverage, engine/read-write/WebSocket baselines, and the
canonical Kafka and gRPC Source Adapter profiles.
