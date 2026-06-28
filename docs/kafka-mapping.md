# Kafka Mapping

Kafka source topics are configured from the typed View Server config. The
mapping function receives typed decoded Kafka key/value data and must return a
row that matches the target View Server topic schema.

```ts
import { Config } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { kafka } from "@effect-view-server/config";
import { runViewServerRuntime } from "@effect-view-server/runtime";
import { viewServer } from "./view-server-config";
import { OrderKeySchema, OrderValueSchema } from "./generated/orders";

const kafkaRegions = {
  usa: Config.string("KAFKA_USA_BOOTSTRAP"),
  london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "orders-view-server",
      regions: kafkaRegions,
      topics: {
        sourceOrders: kafkaTopic({
          regions: ["usa", "london"],
          value: kafka.protobuf(OrderValueSchema),
          key: kafka.protobuf(OrderKeySchema),
          viewServerTopic: "orders",
          mapping: ({ key, value, region }) => ({
            id: key.orderId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          }),
        }),
      },
    },
  }),
);
```

`kafka.protobuf(...)` expects the Buf generated `DescMessage` descriptor symbol,
not a TypeScript value type.

## Contract

- `regions` is type-checked against the configured Kafka region names.
- `viewServerTopic` is type-checked against the configured View Server topics.
- `key` is typed from the configured key codec. If no key codec is configured,
  the key is a string.
- `value` is typed from the configured value codec.
- `mapping` output is validated against the target topic schema before publish.

## Delivery

Kafka messages are decoded, mapped, microbatched, and published through Runtime
Core with `publishMany`. Offsets are committed only after the corresponding
Runtime Core publish succeeds.

If a message fails decode or mapping, health records a decode or mapping failure
for the source topic and region. If publishing fails, the corresponding messages
remain uncommitted so Kafka can replay them.

## Restart Semantics

Runtime Core rows live in memory. There is no durable WAL/checkpoint yet. For
rebuild-after-restart semantics, configure Kafka replay from an authoritative
position such as `startFrom: "earliest"` or a fresh rebuild consumer group.

Committed consumer-group resume is useful for live at-least-once processing, but
it is not durable View Server recovery by itself.
