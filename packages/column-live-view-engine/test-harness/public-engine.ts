import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { createColumnLiveViewEngine, type ColumnLiveViewEngine } from "../src/index";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
  region: Schema.String,
  updatedAt: Schema.Number,
  note: Schema.optionalKey(Schema.String),
});

export const Position = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  symbol: Schema.String,
  active: Schema.Boolean,
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
});

export const Instrument = Schema.Struct({
  id: Schema.String,
  metadata: Schema.Struct({
    venue: Schema.String,
    risk: Schema.Struct({
      tier: Schema.Number,
      lot: Schema.BigInt,
    }),
  }),
  operatorLike: Schema.Struct({
    eq: Schema.String,
  }),
  operatorRangeLike: Schema.Struct({
    gte: Schema.Number,
  }),
  tags: Schema.Array(Schema.String),
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    positions: {
      schema: Position,
      key: "id",
    },
    instruments: {
      schema: Instrument,
      key: "id",
    },
  },
});

export type Topics = typeof viewServer.topics;

export type Engine = ColumnLiveViewEngine<Topics>;

export type OrderRow = typeof Order.Type;

export type PositionRow = typeof Position.Type;

export type InstrumentRow = typeof Instrument.Type;

export const orderSelect: readonly ["id", "customerId", "status", "price", "region", "updatedAt"] =
  ["id", "customerId", "status", "price", "region", "updatedAt"];

export const instrumentSelect: readonly [
  "id",
  "metadata",
  "operatorLike",
  "operatorRangeLike",
  "tags",
] = ["id", "metadata", "operatorLike", "operatorRangeLike", "tags"];

export const order = (
  id: string,
  status: OrderRow["status"],
  price: number,
  updatedAt: number,
  region = "emea",
): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status,
  price,
  region,
  updatedAt,
});

export const position = (
  id: string,
  symbol: string,
  quantity: bigint,
  price: string,
  active = true,
): PositionRow => ({
  id,
  accountId: `account-${id}`,
  symbol,
  active,
  quantity,
  price: fromStringUnsafe(price),
});

export const instrument = (
  id: string,
  venue: string,
  tier: number,
  tags: ReadonlyArray<string>,
): InstrumentRow => ({
  id,
  metadata: {
    venue,
    risk: {
      tier,
      lot: BigInt(tier),
    },
  },
  operatorLike: {
    eq: venue,
  },
  operatorRangeLike: {
    gte: tier,
  },
  tags: [...tags],
});

export const makeEngine = (): Effect.Effect<Engine> =>
  createColumnLiveViewEngine({ topics: viewServer.topics });

export const withObjectPrototypeValue = <Value, Error, Requirements>(
  field: string,
  value: unknown,
  effect: Effect.Effect<Value, Error, Requirements>,
): Effect.Effect<Value, Error, Requirements> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      Reflect.set(Object.prototype, field, value);
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        Reflect.deleteProperty(Object.prototype, field);
      }),
  );
