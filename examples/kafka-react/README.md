# Kafka React Example

TanStack Start app backed by Apache Kafka ingestion.

Run:

```bash
docker compose up -d kafka kafka-london
vp run @effect-view-server/example-kafka-react#runtime
vp run @effect-view-server/example-kafka-react#dev
```

This example demonstrates:

- Canonical Kafka JSON source mapping through
  `kafka.json(() => Schema.toCodecJson(RowSchema))`.
- Mandatory delete-only cleanup plus explicit Kafka-matched row retention,
  validated against each Region before startup.
- Region/partition/local-key canonical identity and retention diagnostics.
- Kafka source rows mapped into a View Server topic.
- React subscriptions using canonical typed `where` arrays over the normal
  WebSocket provider.
- Aggregate health plus exact Topic-bound Source Health for adapter metrics and
  lifecycle visibility.
