# View Server Production Guides

These guides document the current production contract for `view-server-smart`.
They intentionally describe the shipped packages and runtime behavior, not old
design sketches.

- [Public API](./public-api.md)
- [Runtime Config](./runtime-config.md)
- [Kafka Mapping](./kafka-mapping.md)
- [In-Memory Browser Testing](./in-memory-browser-testing.md)
- [Health And Metrics](./health-and-metrics.md)
- [Query Semantics](./query-semantics.md)
- [Benchmarks And Capacity](./benchmarks-and-capacity.md)
- [Releasing](./releasing.md)
- [Deployment](./deployment.md)
- [Operations](./operations.md)

The production browser transport is Effect RPC WebSocket with NDJSON
serialization. Kafka, gRPC, TCP publish, and in-memory testing all feed the same
Runtime Core mutation path.

All Live Query documentation uses the canonical filter contract: `where` is an
implicit-`AND` array of typed Field Conditions and recursive Boolean expressions.
Field-keyed filter objects and shorthand operators are invalid. See
[Query Semantics](./query-semantics.md) for the complete language.
