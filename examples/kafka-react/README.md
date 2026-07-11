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
- Kafka source rows mapped into a View Server topic.
- React subscriptions over the normal WebSocket provider.
- Health summary and detailed health rows for lag/message-rate visibility.
