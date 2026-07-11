import { expectTypeOf } from "@effect/vitest";
import { Context, Effect, Schema } from "effect";
import { defineViewServerConfig, kafka, type KafkaCodecError, type KafkaCodecType } from "./index";

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
});

const canonicalTradeCodec = Schema.toCodecJson(Trade);
const kafkaTradeCodec = kafka.json(() => canonicalTradeCodec);

type VersionedWireError = {
  readonly _tag: "VersionedWireError";
  readonly message: string;
};

const versionedWireCodec = kafka.codec({
  name: "trade-json-v1",
  decode: (): Effect.Effect<{ readonly id: string }, VersionedWireError> =>
    Effect.fail({ _tag: "VersionedWireError", message: "invalid v1 payload" }),
});

expectTypeOf(canonicalTradeCodec.schema).toEqualTypeOf<typeof Trade>();
expectTypeOf<KafkaCodecType<typeof kafkaTradeCodec>>().toEqualTypeOf<typeof Trade.Type>();
expectTypeOf(kafkaTradeCodec).not.toHaveProperty("schema");
expectTypeOf<KafkaCodecType<typeof versionedWireCodec>>().toEqualTypeOf<{
  readonly id: string;
}>();
expectTypeOf<KafkaCodecError<typeof versionedWireCodec>>().toEqualTypeOf<VersionedWireError>();

// @ts-expect-error raw Row Schemas are not canonical JSON codec factories
kafka.json(Trade);

// @ts-expect-error direct canonical codecs are not lazy factories
kafka.json(canonicalTradeCodec);

const structuralJsonCodec: Schema.Codec<typeof Trade.Type, Schema.Json, never, never> =
  canonicalTradeCodec;
// @ts-expect-error structural JSON codecs lack the canonical schema witness
kafka.json(() => structuralJsonCodec);

// @ts-expect-error non-JSON encoded codecs are not canonical JSON codecs
kafka.json(() => Schema.toCodecStringTree(Trade));

// @ts-expect-error canonical codecs for primitive values are not Row Schema codecs
kafka.json(() => Schema.toCodecJson(Schema.String));

// @ts-expect-error canonical JSON factories cannot hide an any schema witness
kafka.json(() => Schema.toCodecJson(JSON.parse("{}")));

// @ts-expect-error the JSON Adapter factory itself cannot be any
kafka.json(JSON.parse("{}"));

// @ts-expect-error the JSON Adapter factory cannot return any
kafka.json(() => JSON.parse("{}"));

declare const neverFactory: () => never;
// @ts-expect-error the JSON Adapter factory cannot return never
kafka.json(neverFactory);

const argumentFactory = (_schema: typeof Trade) => Schema.toCodecJson(Trade);
// @ts-expect-error the JSON Adapter requires an exact zero-argument factory
kafka.json(argumentFactory);

class DecodeService extends Context.Service<DecodeService, true>()("DecodeService") {}
class EncodeService extends Context.Service<EncodeService, true>()("EncodeService") {}

const decodeServiceString = Schema.String.pipe(
  Schema.middlewareDecoding((effect) =>
    Effect.gen(function* () {
      yield* DecodeService;
      return yield* effect;
    }),
  ),
);
const encodeServiceString = Schema.String.pipe(
  Schema.middlewareEncoding((effect) =>
    Effect.gen(function* () {
      yield* EncodeService;
      return yield* effect;
    }),
  ),
);
const DecodeServiceTrade = Schema.Struct({ id: decodeServiceString, symbol: Schema.String });
const EncodeServiceTrade = Schema.Struct({ id: encodeServiceString, symbol: Schema.String });

// @ts-expect-error Kafka JSON Row Schemas cannot require decoding services
kafka.json(() => Schema.toCodecJson(DecodeServiceTrade));

// @ts-expect-error Kafka JSON Row Schemas cannot require encoding services
kafka.json(() => Schema.toCodecJson(EncodeServiceTrade));

defineViewServerConfig({
  kafka: { usa: "localhost:9092" },
  topics: {
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "trades-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Trade)),
        rowKey: ({ key }) => key,
        map: ({ value }) => {
          expectTypeOf(value).toEqualTypeOf<typeof Trade.Type>();
          return { symbol: value.symbol };
        },
      }),
    },
  },
});

// @ts-expect-error JSON Mapping must return every non-key Topic Row field
defineViewServerConfig({
  kafka: { usa: "localhost:9092" },
  topics: {
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "trades-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Trade)),
        rowKey: ({ key }) => key,
        map: () => ({}),
      }),
    },
  },
});

// @ts-expect-error JSON Mapping must not return fields outside the Topic Row
defineViewServerConfig({
  kafka: { usa: "localhost:9092" },
  topics: {
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "trades-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Trade)),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({ symbol: value.symbol, venue: "XNAS" }),
      }),
    },
  },
});

// @ts-expect-error JSON Mapping cannot return any
defineViewServerConfig({
  kafka: { usa: "localhost:9092" },
  topics: {
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "trades-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Trade)),
        rowKey: ({ key }) => key,
        map: () => JSON.parse("{}"),
      }),
    },
  },
});
