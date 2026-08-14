# Runtime Configuration

`runViewServerRuntime(viewServer, options)` is one transport-neutral scoped
Effect. Its options cover View Server concerns only:

- WebSocket/HTTP host and port
- optional WebSocket per-message compression
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
  websocketCompression: true,
  tcpPublishHost: "127.0.0.1",
  tcpPublishPort: 8081,
  reporting: {
    heartbeatInterval: "5 seconds",
    dependenciesInterval: "30 seconds",
    changeInterval: "300 millis",
    onHeartbeat: (heartbeat) => Effect.logInfo("runtime heartbeat", heartbeat),
    onDependenciesUpdate: (dependencies) => Effect.logInfo("runtime dependencies", dependencies),
  },
}).pipe(Effect.provide([KafkaLive, GrpcLive]));

NodeRuntime.runMain(program);
```

`websocketCompression: true` enables RFC 7692 `permessage-deflate` negotiation. It is disabled by
default because compression trades server CPU and latency for network bandwidth. The production
NDJSON firehose benchmark measured TCP stream bytes through a local counting proxy and found a
roughly 96% reduction in outbound bytes with 19–25% higher
mean publish-and-fanout latency on its localhost workload. Benchmark representative payloads and
subscriber fanout before enabling it in production.

`layer(...)` accepts resolved values. `layerConfig(...)` keeps Effect
`Config.ConfigError` in the Layer construction channel. Runtime Effects retain
the exact unsatisfied Source Adapter services and retry-policy dependencies
until the application provides them.

## Runtime reporting

The optional `reporting` bag installs two server-local Effect callbacks. Both
callbacks are required when reporting is enabled. They run periodically at
independent cadences, outside the live-event hot path. A semantic dependency
change also requests both callbacks; bursts coalesce to the latest snapshot and
respect `changeInterval`, which defaults to 300 milliseconds. Callback defects
are logged and later reports continue. Periodic ticks keep their independently
configured cadences even when an interval is shorter than `changeInterval`.
Callback Effects must be closed: provide any services while constructing the
callback rather than returning an Effect with outstanding requirements.

`onHeartbeat` reports the runtime's existing internal Source status vocabulary
plus whether the current problem came from View Server code, an upstream
dependency, or both. It does not invent separate health states.

```json
{
  "status": "Ready",
  "problems": []
}
```

If Kafka is unavailable while the runtime is retrying:

```json
{
  "status": "WaitingToRetry",
  "problems": ["dependency"]
}
```

If a View Server mapping is broken, Kafka remains in the dependency inventory
and retains its last operational status:

```json
{
  "status": "Degraded",
  "problems": ["self"]
}
```

When independent problems coexist, provenance is stable and ordered:

```json
{
  "status": "Exhausted",
  "problems": ["self", "dependency"]
}
```

`onDependenciesUpdate` receives the complete configured inventory every time,
not only changed entries. Kafka contributes one target per configured Region;
gRPC contributes one target per logical client. Endpoints are the exact strings
supplied to their Node Layers. Statuses reuse Source Diagnostics states, with
the existing `Inactive` state for a configured leased dependency that has no
active Feed.

```json
[
  {
    "dependency": "grpc",
    "target": "orders",
    "endpoints": ["https://orders.grpc-tky.com"],
    "status": "Ready"
  },
  {
    "dependency": "kafka",
    "target": "oregon",
    "endpoints": ["b-1.kafka-oregon.com"],
    "status": "Ready"
  },
  {
    "dependency": "kafka",
    "target": "tokyo",
    "endpoints": ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
    "status": "WaitingToRetry"
  }
]
```

When Sources disagree, one heartbeat uses this worst-first precedence:
`Exhausted`, `WaitingToRetry`, `Reacquiring`, `Starting`, `Degraded`, then
`Ready`. Source-level `Stopping` does not participate because the Runtime owns
the single lifecycle `Stopping` heartbeat emitted before shutdown begins.

Reporting uses the existing Source lifecycle evidence; it does not actively
probe brokers or services. Kafka `Starting` means consumer acquisition is in
progress, not that the runtime is waiting for the first message. gRPC becomes
`Ready` when its server stream has been acquired, without waiting for the first
response item.

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
