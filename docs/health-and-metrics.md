# Health And Metrics

## React Hooks

React applications should use pushed health hooks, not UI polling.

`useViewServerHealthSummary()` returns the small summary shape for page chrome
and global status indicators:

- merged `status`
- runtime status
- connection status
- unhealthy topics
- max Kafka lag
- updated timestamp in nanoseconds

`useViewServerHealth()` returns detailed live health rows for status pages and
diagnostic UIs. Use the detailed hook only on pages that actually need detailed
topic/source data.

## Infrastructure Health

The runtime exposes `GET /health` on the same server as the WebSocket endpoint.
It serves the cached runtime health snapshot for infrastructure checks.

- `200`: runtime is ready.
- non-`200`: runtime is starting, degraded, or stopping.

If runtime auth is configured, health requests are authenticated before the
health snapshot is served. Kubernetes probes must either send accepted
credentials or the auth implementation must explicitly allow the health path.
The endpoint can also return `500` if reading or encoding the cached health
snapshot fails.

Internal `bigint` values, such as Kafka lag, are encoded as decimal strings in
the JSON response.

## Metrics

The runtime exposes `GET /metrics` in Prometheus text exposition format. Metrics
are derived from the same cached health snapshot as `GET /health`.

Metrics intentionally keep labels low-cardinality. Raw error messages,
route-specific leased feed keys, and detailed offsets remain available from
`GET /health` instead of labels.

If metrics cannot decode health, the endpoint returns `200` with
`view_server_metrics_error 1` so scrape failure is visible to Prometheus.

## Runtime Signals

Health covers:

- runtime status, version, uptime, and current time
- transport active connections, subscriptions, queues, and backpressure
- engine topic row counts, versions, queues, and backpressure
- Kafka region status, assignments, lag, and failure rates
- gRPC client/feed status, row counts, reconnects, and failure rates
- TCP publish ingress status when configured

Health is a status plane. It should not be used as a high-rate data stream.
