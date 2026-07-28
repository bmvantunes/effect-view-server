# Kafka Mapping

Kafka is an ordinary Materialized Source Adapter.

```ts
import { Schema } from "effect";
import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { kafka } from "effect-view-server/kafka/contract";

const IncomingTrade = Schema.Struct({
  symbol: Schema.String,
  quantity: Schema.Number,
});
const Trade = Schema.Struct({
  id: ViewServerId,
  symbol: Schema.String,
  quantity: Schema.Number,
  region: Schema.String,
});

export const viewServer = defineViewServerConfig({
  topics: {
    trades: {
      schema: Trade,
      source: kafka.source({
        topic: "trades.v1",
        regions: ["eu", "us"],
        key: kafka.string(),
        value: kafka.json(() => Schema.toCodecJson(IncomingTrade)),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          symbol: value.symbol,
          quantity: value.quantity,
          region,
        }),
        startFrom: "earliest",
      }),
    },
  },
});
```

The adapter derives canonical Topic Row ID as `region:localRowKey`.
`localRowKey` receives the decoded key, exact region, and Kafka metadata but not
the value, so tombstones can delete the same row without decoding a missing
value. `map` returns every Topic Row field except `id`; missing, extra, or wrong
fields fail the public type contract and the final row is Schema-validated at
runtime.

## Codecs

- `kafka.bytes()` preserves bytes.
- `kafka.string()` decodes UTF-8.
- `kafka.json(() => Schema.toCodecJson(WireSchema))` uses the canonical Effect
  Schema JSON codec.
- `kafka.protobuf(MessageDescriptor)` uses a Buf descriptor.
- `kafka.codec({ name, decode })` defines a typed custom codec.

Custom codec input and errors are exported from
`effect-view-server/kafka/contract`.

## Start positions

`startFrom` is part of each Source Definition:

- `"earliest"`
- `"latest"`
- committed group with explicit fallback
- timestamp with explicit fallback
- duration-ago with explicit fallback

The Node Layer supplies the active `consumerGroupPrefix`; Runtime Core derives a
Topic-specific group so bindings cannot share progress accidentally.

## Delivery

Regions are concurrent delivery lanes. Records within a lane are decoded,
mapped, applied, and settled sequentially. Offsets commit only after Runtime
Core settles the Delivery or Rejection. Decode, Mapping, ID, and Topic-Schema
problems become safe item Rejections, mark Source Health Degraded, commit the
poison record, and allow later records to continue.

Runtime Core rows remain in memory. A committed offset is an at-least-once
delivery checkpoint, not a durable View Server snapshot. Use an authoritative
replay position when restart must rebuild all rows.
