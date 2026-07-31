# Operations

## Runtime topology

One authored View Server config is shared by React, remote clients, in-memory
tests, and the server runtime. The config contains browser-safe Source
Definitions only. Each deployment provides the required Kafka, gRPC, or custom
Source Adapter Layers at the process edge.

Adapter Layer options own brokers, endpoints, credentials, TLS, pools, and
replica identity. For example, each Kafka-consuming deployment supplies a
stable `consumerGroupPrefix`; the Kafka Layer derives an exact group for every
View Server Topic. Do not copy those concerns into generic runtime options.

Kafka deployments must also grant `DESCRIBE_CONFIGS` for every bound external
topic. Layer acquisition batches and validates cleanup and retention
configuration per Region before any listener or consumer starts. Treat a
broker-contract validation failure as a fatal deployment misconfiguration, not
a health-only incident. Policy changes require a coordinated
stop/change/restart because the validated broker contract is a startup
snapshot.

Use Kubernetes rolling updates carefully. A replacement pod should become ready
only after its required sources are available and aggregate runtime health is
ready.

## Health and metrics

Scrape `GET /metrics` on the same HTTP server as WebSocket RPC. Read
`GET /health` for the current aggregate and detailed Topic health. These routes
perform fresh reads of cadence-cached runtime/source state; they do not call
Kafka, gRPC, or custom upstreams per request.

Source-specific failures, targets, runtime metrics, adapter metrics, and sampled
timestamps live in canonical Topic-bound Source Health. React and
framework-neutral clients subscribe to exact diagnostics with
`useSourceHealth(...)` and `subscribeSourceHealth(...)`. Do not expect
transport-specific optional trees or flattened aggregate fields.

Useful stable aggregate alerts start with runtime and transport pressure:

```promql
view_server_runtime_status{status!="ready"} == 1
```

```promql
increase(view_server_transport_backpressure_events[5m]) > 0
```

```promql
increase(view_server_engine_topic_backpressure_events[5m]) > 0
```

```promql
max(view_server_transport_active_subscriptions)
```

Prometheus exposes fixed, low-cardinality SDK source-runtime metrics. Inspect
exact Kafka, gRPC, or custom-adapter metrics through `/health` or Source
Diagnostics instead of expecting arbitrary adapter fields to become
Prometheus series.

## Kubernetes probes

Use `GET /health` for readiness and startup checks. It returns `200` when the
runtime is ready or degraded, and a non-`200` status while the runtime is
starting or stopping. A required source whose retries are exhausted contributes
starting aggregate health until it recovers, so the probe does not route new
traffic to a runtime serving only retained rows.

```yaml
readinessProbe:
  httpGet:
    path: /health
    port: websocket
  periodSeconds: 5
  failureThreshold: 3
livenessProbe:
  tcpSocket:
    port: websocket
  periodSeconds: 10
  failureThreshold: 6
```

Degraded Source Health remains live and ready because retained rows and queries
remain available. Prefer a process-level or TCP liveness check until a separate
liveness endpoint exists.

If runtime auth is configured, either allow readiness-probe requests through
the authentication Adapter or send accepted credentials.

## Resource sizing

Capacity depends on:

- rows and row width per Topic;
- active raw and grouped queries;
- browser clients and subscriptions;
- Source Adapter input rate and active leased routes;
- WebSocket fanout and queue shape;
- selected fields and grouped aggregate width.

Run the serial release-candidate gate, then a production-shaped soak:

```sh
vp run -w release-candidate:capacity
```

Watch RSS, heap, event-loop delay, exact Source Health, WebSocket queue depth,
and backpressure. Use the Kafka and gRPC Source Adapter benchmarks to isolate
adapter overhead from runtime and query-engine capacity.

## TCP publisher

TCP publish is a private mutation Adapter. Bind it to `127.0.0.1` or a protected
private interface. It is schema-safe but remains a write path.

Use TCP publish only for source-free Topics. Source-Owned Topics reject direct
publish, patch, delete, and reset operations according to the same ownership
policy used by Runtime Client and in-memory paths.

## Failure triage

- Runtime not ready: inspect `/health`, then exact Source Health for unhealthy
  Topics.
- Adapter progress or reconnect issue: inspect that Topic's canonical adapter
  metrics and the upstream service.
- Backpressure increasing: inspect slow clients and source delivery, reduce
  fanout, or raise queue limits only after measuring memory.
- Leased route retained longer than expected: inspect active subscriptions and
  that exact Feed Route's Source Health.
- Metrics scrape returns `view_server_metrics_error 1`: inspect health
  encoding/decoding errors.
