# Materialized gRPC React Example

TanStack Start app backed by a startup materialized gRPC feed.

Run:

```bash
vp run @effect-view-server/example-grpc-materialized-react#runtime
vp run @effect-view-server/example-grpc-materialized-react#dev
```

This example demonstrates:

- `grpc.topicSources(grpcClients).materialized({ schema, key, client, method, ... })`
  topic ownership.
- A topic-owned binding from the View Server topic to the generated gRPC client
  method.
- Runtime startup stream acquisition.
- React querying an already-retained View Server topic.
- Health summary for runtime/source status.
