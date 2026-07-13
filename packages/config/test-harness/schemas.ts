import { Schema } from "effect";
import { viewSchema } from "../src/index";

export class StructuredProfile extends Schema.Class<StructuredProfile>("StructuredProfile")({
  code: Schema.String,
}) {}
viewSchema.admitClass(StructuredProfile);

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.Number,
  price: Schema.Number,
  region: Schema.String,
});

export const Position = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  symbol: Schema.String,
  active: Schema.Boolean,
  quantity: Schema.BigInt,
  optionalQuantity: Schema.Union([Schema.BigInt, Schema.Undefined]),
  price: Schema.BigDecimal,
  notional: Schema.Number,
  optionalNotional: Schema.Union([Schema.Number, Schema.Undefined]),
});
