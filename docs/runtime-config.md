# Runtime Configuration

`runViewServerRuntime(viewServer, options)` is one transport-neutral scoped
Effect. Its options cover View Server concerns only:

- WebSocket/HTTP host and port
- TCP publish host and port
- authentication
- admission and subscription limits
- health cadence and routes
- query-engine limits

Kafka brokers, gRPC endpoints, credentials, TLS, pools, reconnect behavior, and
transport clients belong to adapter Layers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Config, Effect } from "effect";
import { grpcNode } from "effect-view-server/grpc/node";
import { kafkaNode } from "effect-view-server/kafka/node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

const KafkaLive = kafkaNode.layerConfig(viewServer, {
  consumerGroupPrefix: Config.string("KAFKA_CONSUMER_GROUP_PREFIX"),
  retentionSweepInterval: Config.succeed("15 minutes"),
  regions: {
    eu: {
      bootstrapServers: Config.string("KAFKA_EU_BOOTSTRAP_SERVERS"),
    },
  },
});

const GrpcLive = grpcNode.layerConfig(viewServer, {
  orders: {
    baseUrl: Config.string("ORDERS_GRPC_BASE_URL"),
  },
});

const program = runViewServerRuntime(viewServer, {
  host: "0.0.0.0",
  websocketPort: 8080,
  tcpPublishHost: "127.0.0.1",
  tcpPublishPort: 8081,
}).pipe(Effect.provide([KafkaLive, GrpcLive]));

NodeRuntime.runMain(program);
```

`layer(...)` accepts resolved values. `layerConfig(...)` keeps Effect
`Config.ConfigError` in the Layer construction channel. Runtime Effects retain
the exact unsatisfied Source Adapter services and retry-policy dependencies
until the application provides them.

## Kafka

Each Kafka Source Definition owns mandatory `cleanupPolicy`,
`retentionPolicy`, and `startFrom` declarations. The aggregate Node Layer owns
one deployment-specific `consumerGroupPrefix`, exact Region clients, and an
optional positive finite `retentionSweepInterval` (default: 15 minutes).
Missing or extra Region entries are rejected.

Layer acquisition batches `cleanup.policy` and `retention.ms` discovery once
per Region through the Platformatic Kafka Admin client. Missing permissions,
unavailable or malformed configuration, cleanup mismatch, and invalid
`retention.ms` fail the Layer before Runtime Core, consumers, sweeps, listeners,
or server ports start. These are startup contract failures, not health-only
degradation.

## gRPC

The browser-safe config contains generated service descriptors, logical client
names, requests, routes, and Mappings. The Node Layer supplies exact logical
client base URLs, interceptors, TLS, and transports. Browser headers, cookies,
and sessions are never forwarded upstream.

## TCP publish

TCP publish is an optional external mutation Adapter for source-free Topics. It
binds separately from the public WebSocket/HTTP host. Source-owned Topics reject
TCP mutations.

## Ownership and shutdown

Runtime Core owns Source Attempt scopes, settlement, retry, diagnostics, and
retained rows. Adapter Layers own reusable clients and pools. Runtime shutdown
closes Source Attempts before aggregate Layer resources, and
`NodeRuntime.runMain` turns process signals into Effect interruption so
finalizers run.
