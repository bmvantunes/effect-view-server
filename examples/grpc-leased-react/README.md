# Leased gRPC React Example

TanStack Start app backed by an on-demand leased gRPC feed.

Run:

```bash
vp run @effect-view-server/example-grpc-leased-react#runtime
vp run @effect-view-server/example-grpc-leased-react#dev
```

This example demonstrates:

- `grpc.leased({ routeBy: [...] })` source ownership.
- Type-enforced route filters in `useLiveQuery`.
- Shared upstream route acquisition for subscribers using the same route.
- Local View Server filters on top of the leased source route.
