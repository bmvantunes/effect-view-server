import { describe, expect, it } from "@effect/vitest";
import { Schema, SchemaAST } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  protocolFilterFieldSchema,
  protocolNumericOperandSchema,
} from "./protocol-filter-field-schema";

type RecursiveNode = { readonly child: RecursiveNode | null };
const RecursiveNode: Schema.Codec<RecursiveNode, unknown, never, never> = Schema.suspend(() =>
  Schema.Struct({ child: Schema.NullOr(RecursiveNode) }),
);

class Profile extends Schema.Class<Profile>("Profile")({
  nickname: Schema.String,
}) {}

const Status = Schema.Enum({ Open: "open", Closed: "closed" });
const Priority = Schema.Enum({ Low: 1, High: 2 });
const RepeatedString = Schema.make<Schema.Codec<string, string, never, never>>(
  new SchemaAST.Union([Schema.String.ast, Schema.String.ast], "anyOf"),
);
const FilterFields = Schema.Struct({
  text: Schema.String,
  template: Schema.TemplateLiteral(["order-", Schema.String]),
  stringLiteral: Schema.Literal("open"),
  numberLiteral: Schema.Literal(1),
  bigintLiteral: Schema.Literal(1n),
  booleanLiteral: Schema.Literal(true),
  status: Status,
  priority: Priority,
  count: Schema.Number,
  sequence: Schema.BigInt,
  amount: Schema.BigDecimal,
  active: Schema.Boolean,
  nil: Schema.Null,
  profile: Schema.Struct({ country: Schema.String }),
  suspendedProfile: Schema.suspend(() => Schema.Struct({ country: Schema.String })),
  classProfile: Profile,
  mixed: Schema.Union([Schema.Number, Schema.BigInt]),
  recursive: RecursiveNode,
  repeated: RepeatedString,
});

const kinds = (field: ReturnType<typeof protocolFilterFieldSchema>): ReadonlyArray<string> =>
  field === undefined ? [] : Array.from(field.numericKinds);

describe("protocol filter field resolution", () => {
  it("resolves direct and nested scalar domains and caches both hits and misses", () => {
    const text = protocolFilterFieldSchema(FilterFields, "text");
    const nested = protocolFilterFieldSchema(FilterFields, "profile.country");
    const suspended = protocolFilterFieldSchema(FilterFields, "suspendedProfile.country");
    const classField = protocolFilterFieldSchema(FilterFields, "classProfile.nickname");
    const mixed = protocolFilterFieldSchema(FilterFields, "mixed");

    expect(text?.supportsText).toBe(true);
    expect(protocolFilterFieldSchema(FilterFields, "text")).toBe(text);
    expect(nested?.supportsText).toBe(true);
    expect(suspended?.supportsText).toBe(true);
    expect(classField?.supportsText).toBe(true);
    expect(kinds(mixed)).toStrictEqual(["number", "bigint"]);
    expect(protocolFilterFieldSchema(FilterFields, "missing")).toBeUndefined();
    expect(protocolFilterFieldSchema(FilterFields, "missing")).toBeUndefined();
    expect(
      protocolFilterFieldSchema(FilterFields, "recursive.child.child.missing"),
    ).toBeUndefined();
    expect(protocolFilterFieldSchema(FilterFields, "repeated")?.supportsText).toBe(true);
  });

  it("classifies every admitted scalar AST", () => {
    expect(protocolFilterFieldSchema(FilterFields, "template")?.supportsText).toBe(true);
    expect(protocolFilterFieldSchema(FilterFields, "stringLiteral")?.supportsText).toBe(true);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "numberLiteral"))).toStrictEqual([
      "number",
    ]);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "bigintLiteral"))).toStrictEqual([
      "bigint",
    ]);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "booleanLiteral"))).toStrictEqual([]);
    expect(protocolFilterFieldSchema(FilterFields, "status")?.supportsText).toBe(true);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "priority"))).toStrictEqual(["number"]);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "count"))).toStrictEqual(["number"]);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "sequence"))).toStrictEqual(["bigint"]);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "amount"))).toStrictEqual(["bigDecimal"]);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "active"))).toStrictEqual([]);
    expect(kinds(protocolFilterFieldSchema(FilterFields, "nil"))).toStrictEqual([]);
  });

  it("selects every declared numeric operand codec", () => {
    const number = protocolFilterFieldSchema(FilterFields, "count")!;
    const bigint = protocolFilterFieldSchema(FilterFields, "sequence")!;
    const bigDecimal = protocolFilterFieldSchema(FilterFields, "amount")!;
    const mixed = protocolFilterFieldSchema(FilterFields, "mixed")!;
    const hostile = {
      schema: Schema.String,
      numericKinds: new Set(["hostile"]),
      supportsText: false,
    };

    expect(protocolNumericOperandSchema(number)).toBe(Schema.Number);
    expect(protocolNumericOperandSchema(bigint)).toBe(Schema.BigInt);
    expect(protocolNumericOperandSchema(bigDecimal)).toBe(Schema.BigDecimal);
    const mixedSchema = protocolNumericOperandSchema(mixed);
    const mixedIs = Schema.is(mixedSchema);
    expect(mixedIs(1)).toBe(true);
    expect(mixedIs(1n)).toBe(true);
    expect(mixedIs("1")).toBe(false);
    const everyKind = { ...hostile, numericKinds: new Set(["number", "bigint", "bigDecimal"]) };
    const numberAndDecimal = {
      ...hostile,
      numericKinds: new Set(["number", "bigDecimal"]),
    };
    const bigintAndDecimal = {
      ...hostile,
      numericKinds: new Set(["bigint", "bigDecimal"]),
    };
    expect(
      Schema.is(
        // @ts-expect-error hostile internal callers can construct numeric-kind sets dynamically.
        protocolNumericOperandSchema(everyKind),
      )(BigDecimal.make(1n, 0)),
    ).toBe(true);
    expect(
      Schema.is(
        // @ts-expect-error hostile internal callers can construct numeric-kind sets dynamically.
        protocolNumericOperandSchema(numberAndDecimal),
      )(1),
    ).toBe(true);
    expect(
      Schema.is(
        // @ts-expect-error hostile internal callers can construct numeric-kind sets dynamically.
        protocolNumericOperandSchema(bigintAndDecimal),
      )(1n),
    ).toBe(true);
    // @ts-expect-error hostile internal callers can violate the closed numeric-kind union.
    expect(protocolNumericOperandSchema(hostile)).toBe(Schema.Never);
  });

  it("rejects an own field whose runtime schema value is missing", () => {
    const malformedRowSchema = { fields: { broken: undefined } };
    // @ts-expect-error hostile configuration can violate the RowSchema contract at runtime.
    expect(protocolFilterFieldSchema(malformedRowSchema, "broken")).toBeUndefined();
  });
});
