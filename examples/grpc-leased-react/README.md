# Leased gRPC React Example

TanStack Start app backed by an on-demand leased gRPC feed.

Run:

```bash
vp run -view-server/example-grpc-leased-react#runtime
vp run -view-server/example-grpc-leased-react#dev
```

This example demonstrates:

- `grpc.leased({ routeBy: [...] })` source ownership.
- A runtime `leasedFeed({ topic, client, method, ... })` binding from the View
  Server topic to the generated gRPC client method.
- Type-enforced route filters in `useLiveQuery`.
- Shared upstream route acquisition for subscribers using the same route.
- Local View Server filters on top of the leased source route.
