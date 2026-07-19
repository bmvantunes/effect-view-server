# Combined Sources React Example

Production-shaped TanStack Start app combining Kafka, leased gRPC, and
materialized gRPC.

Run:

```bash
docker compose up -d kafka kafka-london
vp run @effect-view-server/example-combined-sources-react#runtime
vp run @effect-view-server/example-combined-sources-react#dev
```

This example demonstrates:

- One `defineViewServerConfig` with multiple topic/source shapes.
- Kafka-owned `trades`.
- Leased gRPC-owned `orders`.
- Materialized gRPC-owned `strategies`.
- Topic-owned gRPC bindings that connect those topics to concrete generated
  client methods.
- Canonical typed `where` arrays for local filtering, with exact `routeBy`
  objects on leased-topic queries.
- Summary and detailed health in the same UI.
