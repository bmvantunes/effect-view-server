# Leased gRPC React Example

TanStack Start app backed by an on-demand leased gRPC feed.

Run:

```bash
vp run @effect-view-server/example-grpc-leased-react#runtime
vp run @effect-view-server/example-grpc-leased-react#dev
```

This example demonstrates:

- `grpc.topicSources(grpcClients).leased({ schema, key, routeBy, client, method, ... })`
  source ownership.
- A topic-owned binding from the View Server topic to the generated gRPC client
  method.
- A type-enforced exact `routeBy` object in `useLiveQuery`, passed to gRPC
  without case, accent, or text normalization.
- Shared upstream route acquisition for subscribers using the same route.
- Canonical local `where` arrays on top of the leased source route, independent
  from `routeBy`.
