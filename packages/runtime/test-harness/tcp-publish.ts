import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";

export const NestedTcpOrder = Schema.Struct({
  id: ViewServerId,
  meta: Schema.Struct({
    desk: Schema.String,
  }),
});

export const nestedTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: NestedTcpOrder,
    },
  },
});

export const TransformTcpOrder = Schema.Struct({
  id: ViewServerId,
  quantity: Schema.BigIntFromString,
});

export const transformTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: TransformTcpOrder,
    },
  },
});

export const UnionCodecTcpOrder = Schema.Struct({
  id: ViewServerId,
  quantity: Schema.NullOr(Schema.BigInt),
});

export const unionCodecTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: UnionCodecTcpOrder,
    },
  },
});

export const DefaultedTcpOrder = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  status: Schema.String.pipe(Schema.withDecodingDefaultKey(Effect.succeed("open"))),
});

export const defaultedTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: DefaultedTcpOrder,
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
  id: ViewServerId,
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
> = Schema.suspend((): Schema.Codec<JsonCodecTcpRecursiveNode, unknown, never, never> =>
  Schema.Struct({
    id: Schema.BigIntFromString,
    amount: Schema.BigDecimal,
    runtimeQuantity: Schema.BigInt,
    child: Schema.NullOr(JsonCodecTcpRecursiveNode),
  }),
);

export const JsonCodecTcpRecursiveOrder = Schema.Struct({
  id: ViewServerId,
  node: JsonCodecTcpRecursiveNode,
});

export const jsonCodecTcpRecursiveViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: JsonCodecTcpRecursiveOrder,
    },
  },
});

export const PositivePriceTcpOrder = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
}).check(Schema.makeFilter((row) => row.price >= 0, { expected: "price >= 0" }));

export const positivePriceTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: PositivePriceTcpOrder,
    },
  },
});
