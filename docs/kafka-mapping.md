# Kafka Mapping

Kafka source topics are configured from the typed View Server config. The
`rowKey` function receives typed decoded Kafka key metadata and defines the
View Server row identity. The mapping function receives typed decoded Kafka
key/value data plus that row key and must return the target View Server topic
row without the configured key field. The runtime injects the configured key
field from `rowKey`, so Kafka tombstones can delete the same source-owned row
without decoding a value.

```ts
import { Config } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { defineViewServerConfig, kafka } from "effect-view-server/config";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { KafkaTrade, Order, Trade } from "./schemas";
import { OrderValueSchema } from "./generated/orders";

const kafkaRegions = {
  usa: Config.string("KAFKA_USA_BOOTSTRAP"),
  london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
};

export const viewServer = defineViewServerConfig({
  kafka: kafkaRegions,
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceOrdersUsa",
        regions: ["usa"],
        value: kafka.protobuf(OrderValueSchema),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceTradesLondon",
        regions: ["london"],
        value: kafka.json(KafkaTrade),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          symbol: value.symbol,
          side: value.side,
          quantity: value.quantity,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "orders-view-server",
    },
  }),
);
```

`kafka.protobuf(...)` expects the Buf generated `DescMessage` descriptor symbol,
not a TypeScript value type.

The region names in each `kafkaSource.regions` tuple are checked against
`config.kafka`. In the example above, `["usa"]` and `["london"]` are valid, but
`["paris"]` fails at compile time.

`rowKey` is intentionally value-independent: it receives the decoded Kafka key,
the source region, and Kafka message metadata, but not the decoded value. That
keeps row identity stable for compacted-topic tombstones and lets the runtime
delete by key without decoding a null value.

## Contract

- `regions` is type-checked against the configured Kafka region names.
- `kafkaSource` is owned by exactly one View Server topic, so the runtime cannot
  accidentally publish the same source into a different topic.
- `key` is typed from the configured key codec. If no key codec is configured,
  the key is a string.
- `value` is typed from the configured value codec.
- `map` output is validated against the target topic schema before publish.

## Delivery

Kafka messages are decoded, mapped, microbatched, and applied through Runtime
Core. Topic-owned sources upsert with source-owned storage keys, and compacted
topic tombstones delete by that same key. Offsets are committed only after the
corresponding Runtime Core mutation succeeds.

Kafka records without key bytes cannot derive a source-owned row key. The
runtime records a mapping failure, commits the record, and skips it so one
poison record cannot replay forever and stall the whole region. Tombstone
deletes are idempotent: a tombstone for an already-missing row is a successful
no-op after the delete mutation runs.

If a message fails decode or mapping, health records a decode or mapping failure
for the source topic and region. If publishing fails, the corresponding messages
remain uncommitted so Kafka can replay them.

## Restart Semantics

Runtime Core rows live in memory. There is no durable WAL/checkpoint yet. For
rebuild-after-restart semantics, configure Kafka replay from an authoritative
position such as `startFrom: "earliest"` or a fresh rebuild consumer group.

Committed consumer-group resume is useful for live at-least-once processing, but
it is not durable View Server recovery by itself.

`startFrom` is currently a runtime-level consumer policy. A single View Server
runtime cannot read one Kafka source topic from `"earliest"` and another from
`"latest"` with the same consumer group. If you need mixed start positions today,
run separate runtime instances with separate consumer groups and configs, for
example a replay/rebuild runtime using `startFrom: "earliest"` and a live-tail
runtime using `startFrom: "latest"`.
