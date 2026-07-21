import { describe, expect, it } from "@effect/vitest";
import { Schema, SchemaAST } from "effect";
import {
  viewServerFilterFieldContract,
  viewServerFilterFieldContracts,
} from "./filter-field-contract";

type RecursiveNode = { readonly child: RecursiveNode | null };
const RecursiveNode: Schema.Codec<RecursiveNode, unknown, never, never> = Schema.suspend(() =>
  Schema.Struct({ child: Schema.NullOr(RecursiveNode) }),
);

class Profile extends Schema.Class<Profile>("FilterFieldProfile")({ nickname: Schema.String }) {}

const RepeatedString = Schema.make<Schema.Codec<string, string, never, never>>(
  new SchemaAST.Union([Schema.String.ast, Schema.String.ast], "anyOf"),
);

const SymbolicObject = Schema.make<Schema.Codec<object, object, never, never>>(
  new SchemaAST.Objects(
    [
      new SchemaAST.PropertySignature(Symbol("metadata"), Schema.String.ast),
      new SchemaAST.PropertySignature("country", Schema.String.ast),
    ],
    [],
  ),
);

const Row = Schema.Struct({
  text: Schema.String,
  template: Schema.TemplateLiteral(["id-", Schema.String]),
  count: Schema.Number,
  sequence: Schema.BigInt,
  amount: Schema.BigDecimal,
  active: Schema.Boolean,
  nil: Schema.Null,
  stringLiteral: Schema.Literal("open"),
  numberLiteral: Schema.Literal(1),
  bigintLiteral: Schema.Literal(1n),
  booleanLiteral: Schema.Literal(true),
  status: Schema.Enum({ Open: "open", Closed: "closed" }),
  priority: Schema.Enum({ Low: 1, High: 2 }),
  nested: Schema.Struct({ country: Schema.String }),
  suspended: Schema.suspend(() => Schema.Struct({ country: Schema.String })),
  classProfile: Profile,
  repeated: RepeatedString,
  symbolic: SymbolicObject,
  mixed: Schema.Union([Schema.Number, Schema.BigInt, Schema.BigDecimal]),
  recursive: RecursiveNode,
  structured: Schema.Array(Schema.String),
});

const kinds = (field: string): ReadonlyArray<string> =>
  Array.from(viewServerFilterFieldContract(Row, field)?.numericKinds ?? []);

describe("filter field contracts", () => {
  it("owns one cached description of every statically named scalar field", () => {
    const fields = viewServerFilterFieldContracts(Row);

    expect(viewServerFilterFieldContracts(Row)).toBe(fields);
    expect(viewServerFilterFieldContract(Row, "missing")).toBeUndefined();
    expect(fields.get("text")?.supportsText).toBe(true);
    expect(fields.get("template")?.supportsText).toBe(true);
    expect(kinds("count")).toStrictEqual(["number"]);
    expect(kinds("sequence")).toStrictEqual(["bigint"]);
    expect(kinds("amount")).toStrictEqual(["bigDecimal"]);
    expect(kinds("active")).toStrictEqual([]);
    expect(kinds("nil")).toStrictEqual([]);
    expect(fields.get("stringLiteral")?.supportsText).toBe(true);
    expect(kinds("numberLiteral")).toStrictEqual(["number"]);
    expect(kinds("bigintLiteral")).toStrictEqual(["bigint"]);
    expect(kinds("booleanLiteral")).toStrictEqual([]);
    expect(fields.get("status")?.supportsText).toBe(true);
    expect(kinds("priority")).toStrictEqual(["number"]);
    expect(fields.get("nested.country")?.segments).toStrictEqual(["nested", "country"]);
    expect(fields.get("suspended.country")?.supportsText).toBe(true);
    expect(fields.get("classProfile.nickname")?.supportsText).toBe(true);
    expect(fields.get("repeated")?.supportsText).toBe(true);
    expect(fields.get("symbolic.country")?.supportsText).toBe(true);
    expect(kinds("mixed")).toStrictEqual(["number", "bigint", "bigDecimal"]);
    expect(fields.has("recursive.child.child")).toBe(false);
    expect(fields.has("structured")).toBe(false);
  });

  it("ignores absent or malformed field tables", () => {
    expect(viewServerFilterFieldContracts(Schema.ObjectKeyword).size).toBe(0);
    expect(viewServerFilterFieldContracts({ fields: { broken: "not-a-schema" } }).size).toBe(0);
  });

  it("rejects reserved dots at every static object level", () => {
    const rootName = "profile.country";
    const nestedName = "country.code";
    const RootDotted = Schema.Struct({ [rootName]: Schema.String });
    const NestedDotted = Schema.Struct({
      profile: Schema.Struct({ [nestedName]: Schema.String }),
    });

    expect(() => viewServerFilterFieldContracts(RootDotted)).toThrow(
      "Filterable Topic Row field profile.country contains a reserved dot.",
    );
    expect(() => viewServerFilterFieldContracts(NestedDotted)).toThrow(
      "Filterable object field profile.country.code contains a reserved dot.",
    );
  });
});
