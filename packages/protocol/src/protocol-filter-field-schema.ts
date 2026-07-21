import type { RowSchema } from "@effect-view-server/config";
import {
  viewServerFilterFieldContract,
  type ViewServerFilterFieldContract,
  type ViewServerFilterNumericKind,
} from "@effect-view-server/config/internal";
import { Schema } from "effect";

export type ProtocolFilterNumericKind = ViewServerFilterNumericKind;

export type ProtocolFilterFieldSchema = {
  readonly schema: Schema.Codec<unknown, unknown, never, never>;
  readonly numericKinds: ReadonlySet<ProtocolFilterNumericKind>;
  readonly supportsText: boolean;
};

const asProtocolFilterFieldSchema = (
  field: ViewServerFilterFieldContract | undefined,
): ProtocolFilterFieldSchema | undefined => field;

export const protocolFilterFieldSchema = (
  rowSchema: RowSchema,
  path: string,
): ProtocolFilterFieldSchema | undefined =>
  asProtocolFilterFieldSchema(viewServerFilterFieldContract(rowSchema, path));

export const protocolNumericOperandSchema = (
  field: ProtocolFilterFieldSchema,
): Schema.Codec<unknown, unknown, never, never> => {
  const number = field.numericKinds.has("number");
  const bigint = field.numericKinds.has("bigint");
  const bigDecimal = field.numericKinds.has("bigDecimal");
  if (number && bigint && bigDecimal) {
    return Schema.Union([Schema.Number, Schema.BigInt, Schema.BigDecimal]);
  }
  if (number && bigint) {
    return Schema.Union([Schema.Number, Schema.BigInt]);
  }
  if (number && bigDecimal) {
    return Schema.Union([Schema.Number, Schema.BigDecimal]);
  }
  if (bigint && bigDecimal) {
    return Schema.Union([Schema.BigInt, Schema.BigDecimal]);
  }
  if (number) {
    return Schema.Number;
  }
  if (bigint) {
    return Schema.BigInt;
  }
  return bigDecimal ? Schema.BigDecimal : Schema.Never;
};
