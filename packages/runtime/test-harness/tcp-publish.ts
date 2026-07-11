import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema, SchemaGetter } from "effect";
import * as BigDecimal from "effect/BigDecimal";

export const NestedTcpOrder = Schema.Struct({
  id: Schema.String,
  meta: Schema.Struct({
    desk: Schema.String,
  }),
});

export const nestedTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: NestedTcpOrder,
      key: "id",
    },
  },
});

export const TransformTcpOrder = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
});

export const transformTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: TransformTcpOrder,
      key: "id",
    },
  },
});

export const KeyTransformTcpId = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transform((value) => `decoded-${value}`),
    encode: SchemaGetter.transform((value) =>
      value.startsWith("decoded-") ? value.slice("decoded-".length) : value,
    ),
  }),
);

export const KeyTransformTcpOrder = Schema.Struct({
  id: KeyTransformTcpId,
  price: Schema.Number,
});

export const keyTransformTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: KeyTransformTcpOrder,
      key: "id",
    },
  },
});

export const NonStringKeyTcpOrder = Schema.Struct({
  id: Schema.BigIntFromString,
  price: Schema.Number,
});

// @ts-expect-error non-string row keys are rejected by the public config contract.
export const nonStringKeyTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: NonStringKeyTcpOrder,
      key: "id",
    },
  },
});

export const UnionCodecTcpOrder = Schema.Struct({
  id: Schema.String,
  quantity: Schema.NullOr(Schema.BigInt),
});

export const unionCodecTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: UnionCodecTcpOrder,
      key: "id",
    },
  },
});

export const DefaultedTcpOrder = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
  status: Schema.String.pipe(Schema.withDecodingDefaultKey(Effect.succeed("open"))),
});

export const defaultedTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: DefaultedTcpOrder,
      key: "id",
    },
  },
});

export const JsonCodecTcpNested = Schema.Struct({
  encodedQuantity: Schema.BigIntFromString,
  runtimeAmount: Schema.BigDecimal,
  runtimeQuantity: Schema.optionalKey(Schema.BigInt),
});

export const JsonCodecTcpOrder = Schema.Struct({
  allocations: Schema.Record(Schema.String, JsonCodecTcpNested).check(Schema.isMinProperties(1)),
  fills: Schema.Array(JsonCodecTcpNested).check(Schema.isMinLength(1)),
  id: Schema.String,
  amount: Schema.BigDecimal,
  checkedOptionalMeta: Schema.optionalKey(JsonCodecTcpNested).check(Schema.isMaxProperties(0)),
  checkedSuspendedEmptyMeta: Schema.optionalKey(
    Schema.suspend(() => Schema.Struct({}).check(Schema.isMaxProperties(0))),
  ),
  checkedSuspendedMeta: Schema.optionalKey(
    Schema.suspend(() => JsonCodecTcpNested.check(Schema.isMaxProperties(0))),
  ),
  meta: JsonCodecTcpNested,
  nullableMeta: Schema.NullOr(JsonCodecTcpNested),
  optionalMeta: Schema.optionalKey(JsonCodecTcpNested),
  optionalValueMeta: Schema.optional(JsonCodecTcpNested),
  quantity: Schema.BigInt,
  suspendedMeta: Schema.suspend(() => JsonCodecTcpNested),
  tuple: Schema.Tuple([JsonCodecTcpNested]),
  tupleRest: Schema.TupleWithRest(Schema.Tuple([JsonCodecTcpNested]), [JsonCodecTcpNested]),
  tupleRestTrailing: Schema.TupleWithRest(Schema.Tuple([JsonCodecTcpNested]), [
    JsonCodecTcpNested,
    JsonCodecTcpNested,
  ]),
  unionMeta: Schema.Union([JsonCodecTcpNested, Schema.Undefined]),
});

export const jsonCodecTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: JsonCodecTcpOrder,
      key: "id",
    },
  },
});

export type JsonCodecTcpRecursiveNode = {
  readonly id: bigint;
  readonly amount: BigDecimal.BigDecimal;
  readonly runtimeQuantity: bigint;
  readonly child: JsonCodecTcpRecursiveNode | null;
};

export const JsonCodecTcpRecursiveNode: Schema.Codec<
  JsonCodecTcpRecursiveNode,
  unknown,
  never,
  never
> = Schema.suspend(
  (): Schema.Codec<JsonCodecTcpRecursiveNode, unknown, never, never> =>
    Schema.Struct({
      id: Schema.BigIntFromString,
      amount: Schema.BigDecimal,
      runtimeQuantity: Schema.BigInt,
      child: Schema.NullOr(JsonCodecTcpRecursiveNode),
    }),
);

export const JsonCodecTcpRecursiveOrder = Schema.Struct({
  id: Schema.String,
  node: JsonCodecTcpRecursiveNode,
});

export const jsonCodecTcpRecursiveViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: JsonCodecTcpRecursiveOrder,
      key: "id",
    },
  },
});

export const PositivePriceTcpOrder = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
}).check(Schema.makeFilter((row) => row.price >= 0, { expected: "price >= 0" }));

export const positivePriceTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: PositivePriceTcpOrder,
      key: "id",
    },
  },
});
