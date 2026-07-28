# Materialized gRPC React Example

TanStack Start app backed by a startup materialized gRPC feed.

Run:

```bash
vp run @effect-view-server/example-grpc-materialized-react#runtime
vp run @effect-view-server/example-grpc-materialized-react#dev
```

This example demonstrates:

- `grpc.topicSources({ orders: ordersService }).materialized({ client, method,
request, map })` on the Topic's canonical `source` property.
- Browser-safe generated service descriptors in config, with concrete clients
  and base URLs owned by `grpcNode.layer(...)`.
- Runtime startup stream acquisition.
- React querying an already-retained View Server topic with canonical typed
  `where` arrays.
- Health summary for runtime/source status.
