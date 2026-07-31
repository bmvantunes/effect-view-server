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

A Materialized Source enters supervision even when its first adapter-metrics
sample is schema-invalid. Until the adapter publishes its first valid cached
sample, that Topic has no active Source Health value: aggregate `sources` omits
the Materialized Topic and its diagnostics stream waits for a valid health
snapshot. This keeps unrelated Topics and transports available without
inventing adapter metrics that do not satisfy the declared Schema. Leased
aggregate entries remain present as arrays and use an empty array when no feed
has active health.

## Degradation

A settled item Rejection increments exact `bigint` counters and marks the
Source, Topic, and aggregate health Degraded. The source lane remains live and
later valid deliveries continue. Ready and Degraded sources keep dependent Live
Queries ready; retry or reacquisition makes them stale; exhaustion makes them
error while retained rows and subscriptions remain.

Kafka expiration Delete failures use a separate active maintenance-failure
ledger. A failed row remains indexed and retryable on later sweeps, ingestion
continues, and the failure becomes immediately visible at Source, Topic, and
aggregate View Server health. Repeated failures for one row generation increase
the cumulative retry counter without increasing failed-work backlog
cardinality. A successful retry or an authoritative replacement/removal clears
the active identity; the latest safe failure and cumulative counter remain
available in Kafka metrics for the logical source lifetime.

Kafka Region metrics include the declared and observed cleanup policies,
configured and resolved retention, tracked/due/failed backlog counts, expired
and authoritative-expired Delete counts, cumulative expiration retry failures,
the latest exact safe expiration failure, last sweep epoch/duration, and sweep
interval.

## Infrastructure health

`GET /health` shares the WebSocket server. It reads the runtime-cached health
source, not a client atom and not the upstream adapter network. Concurrent
requests are coalesced.

- `200`: runtime is ready or degraded
- `503`: runtime is starting or stopping
- other non-`200`: authentication, health reading, or health encoding failed

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
