import { describe, expect, it } from "@effect/vitest";
import { Schema, SchemaAST } from "effect";
import { makeFilterFieldMetadata } from "./filter-field-metadata";

type RecursiveNode = { readonly child: RecursiveNode | null };
const RecursiveNode: Schema.Codec<RecursiveNode, unknown, never, never> = Schema.suspend(() =>
  Schema.Struct({ child: Schema.NullOr(RecursiveNode) }),
);

class Profile extends Schema.Class<Profile>("Profile")({ nickname: Schema.String }) {}

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

const Status = Schema.Enum({ Open: "open", Closed: "closed" });
const Priority = Schema.Enum({ Low: 1, High: 2 });
const FilterRow = Schema.Struct({
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
  status: Status,
  priority: Priority,
  nested: Schema.Struct({ country: Schema.String }),
  suspended: Schema.suspend(() => Schema.Struct({ country: Schema.String })),
  classProfile: Profile,
  repeated: RepeatedString,
  symbolic: SymbolicObject,
  mixed: Schema.Union([Schema.Number, Schema.BigInt]),
  recursive: RecursiveNode,
  structured: Schema.Array(Schema.String),
});

const numericKinds = (
  fields: ReturnType<typeof makeFilterFieldMetadata>,
  field: string,
): ReadonlyArray<string> => Array.from(fields.get(field)?.numericKinds ?? []);

describe("filter field metadata", () => {
  it("discovers every direct and statically nested scalar domain", () => {
    const fields = makeFilterFieldMetadata(FilterRow);

    expect(fields.get("text")?.hasString).toBe(true);
    expect(fields.get("template")?.hasString).toBe(true);
    expect(numericKinds(fields, "count")).toStrictEqual(["number"]);
    expect(numericKinds(fields, "sequence")).toStrictEqual(["bigint"]);
    expect(numericKinds(fields, "amount")).toStrictEqual(["bigDecimal"]);
    expect(numericKinds(fields, "active")).toStrictEqual([]);
    expect(numericKinds(fields, "nil")).toStrictEqual([]);
    expect(fields.get("stringLiteral")?.hasString).toBe(true);
    expect(numericKinds(fields, "numberLiteral")).toStrictEqual(["number"]);
    expect(numericKinds(fields, "bigintLiteral")).toStrictEqual(["bigint"]);
    expect(numericKinds(fields, "booleanLiteral")).toStrictEqual([]);
    expect(fields.get("status")?.hasString).toBe(true);
    expect(numericKinds(fields, "priority")).toStrictEqual(["number"]);
    expect(fields.get("nested.country")?.segments).toStrictEqual(["nested", "country"]);
    expect(fields.get("suspended.country")?.hasString).toBe(true);
    expect(fields.get("classProfile.nickname")?.hasString).toBe(true);
    expect(fields.get("repeated")?.hasString).toBe(true);
    expect(fields.get("symbolic.country")?.hasString).toBe(true);
    expect(numericKinds(fields, "mixed")).toStrictEqual(["number", "bigint"]);
    expect(fields.has("recursive.child.child")).toBe(false);
    expect(fields.has("structured")).toBe(false);
  });

  it("returns no fields when the codec has no struct field table", () => {
    expect(makeFilterFieldMetadata(Schema.ObjectKeyword).size).toBe(0);
  });

  it("ignores malformed field table values", () => {
    const malformed = { fields: { broken: "not-a-schema" } };
    // @ts-expect-error hostile configuration can violate the schema field contract at runtime.
    expect(makeFilterFieldMetadata(malformed).size).toBe(0);
  });

  it("rejects dots at the root and nested object levels", () => {
    const rootName = "profile.country";
    const nestedName = "country.code";
    const RootDotted = Schema.Struct({ [rootName]: Schema.String });
    const NestedDotted = Schema.Struct({
      profile: Schema.Struct({ [nestedName]: Schema.String }),
    });

    expect(() => makeFilterFieldMetadata(RootDotted)).toThrow(
      "Filterable Topic Row field profile.country contains a reserved dot.",
    );
    expect(() => makeFilterFieldMetadata(NestedDotted)).toThrow(
      "Filterable object field profile.country.code contains a reserved dot.",
    );
  });
});
