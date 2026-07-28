# Health And Metrics

## React hooks

Health is pushed on a bounded cadence; React must not poll.

`useViewServerHealthSummary()` returns small page-chrome state: merged status,
runtime status, connection status, unhealthy Topics, and sampled nanoseconds.

`useViewServerHealth()` returns detailed transport-neutral Topic rows for an
operator page. Source-owned Topic detail projects canonical source status
instead of transport-specific optional trees.

`useSourceHealth(...)` is the explicit exact diagnostics surface:

```tsx
const kafkaHealth = useSourceHealth({ topic: "orders" });
const leasedGrpcHealth = useSourceHealth({
  topic: "ordersByRegion",
  routeBy: { region: "eu" },
});
```

Each active Source Health contains:

- adapter identity
- Materialized or exact Leased Source Target
- exact Source Status
- runtime metrics
- adapter metrics
- sampled nanoseconds
- exact typed adapter failure and rejection location when applicable

Materialized diagnostics are active. Leased diagnostics return exact
`Inactive` or `Active`; observing an inactive route does not acquire its feed.
Matching consumers share one scoped subscription and unmount/client close
releases it. There is no one-shot diagnostics interface.

## Degradation

A settled item Rejection increments exact `bigint` counters and marks the
Source, Topic, and aggregate health Degraded. The source lane remains live and
later valid deliveries continue. Ready and Degraded sources keep dependent Live
Queries ready; retry or reacquisition makes them stale; exhaustion makes them
error while retained rows and subscriptions remain.

## Infrastructure health

`GET /health` shares the WebSocket server. It reads the runtime-cached health
source, not a client atom and not the upstream adapter network. Concurrent
requests are coalesced.

- `200`: runtime is ready or degraded
- non-`200`: runtime is starting or stopping

JSON encodes `bigint` values as decimal strings. Authentication is evaluated
before the health snapshot is served.

## Prometheus metrics

`GET /metrics` projects the same canonical cached health into low-cardinality
Prometheus exposition using fixed SDK runtime metrics. Exact adapter metrics,
raw failures, Feed Routes, and detailed offsets stay in Source Diagnostics or
`GET /health`; arbitrary adapter metrics are not flattened into Prometheus
series.

If metrics cannot be encoded, the endpoint returns a scrapeable response with
`view_server_metrics_error 1`.

Health is a status plane. No health refresh or RPC belongs on the Live Query
Snapshot/Delta hot path.
