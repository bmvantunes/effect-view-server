import { describe, expect, it } from "@effect/vitest";
import { HashMap, HashSet, Schema } from "effect";
import { compileGroupedKeyIdentity, type GroupedKeyIdentityField } from "./grouped-key-identity";
import { makeSchemaJsonIdentity } from "./schema-json-identity";

const GroupedKeyRow = Schema.Struct({
  text: Schema.String,
  amount: Schema.Number,
  nested: Schema.Struct({ desk: Schema.String }),
  maybe: Schema.optionalKey(Schema.Union([Schema.String, Schema.Undefined])),
  hashMap: Schema.HashMap(Schema.String, Schema.String),
  hashSet: Schema.HashSet(Schema.String),
});

const groupedKeyFields: ReadonlyArray<GroupedKeyIdentityField> = [
  { field: "text", canonicalKey: makeSchemaJsonIdentity(GroupedKeyRow.fields.text).canonicalKey },
  {
    field: "amount",
    canonicalKey: makeSchemaJsonIdentity(GroupedKeyRow.fields.amount).canonicalKey,
  },
  {
    field: "nested",
    canonicalKey: makeSchemaJsonIdentity(GroupedKeyRow.fields.nested).canonicalKey,
  },
  {
    field: "maybe",
    canonicalKey: makeSchemaJsonIdentity(GroupedKeyRow.fields.maybe).canonicalKey,
  },
  {
    field: "hashMap",
    canonicalKey: makeSchemaJsonIdentity(GroupedKeyRow.fields.hashMap).canonicalKey,
  },
  {
    field: "hashSet",
    canonicalKey: makeSchemaJsonIdentity(GroupedKeyRow.fields.hashSet).canonicalKey,
  },
];

const throwingIdentity = compileGroupedKeyIdentity<object>(groupedKeyFields, "throw");
const optionalIdentity = compileGroupedKeyIdentity<object>(groupedKeyFields, "undefined");

const missingPresenceKey = JSON.stringify(["missing"]);
const presentPresenceKey = (canonicalKey: string): string =>
  JSON.stringify(["present", canonicalKey]);
const groupedKey = (
  entries: ReadonlyArray<readonly [field: string, presenceKey: string]>,
): string => JSON.stringify(entries);

const collisionLeft = "8ocpIaaa";
const collisionRight = "GpcpIaaa";

const semanticRow = (amount: number, reverse: boolean) => ({
  text: "route",
  amount,
  nested: { desk: "equities" },
  hashMap: reverse
    ? HashMap.make([collisionRight, "right"], [collisionLeft, "left"])
    : HashMap.make([collisionLeft, "left"], [collisionRight, "right"]),
  hashSet: reverse
    ? HashSet.make(collisionRight, collisionLeft)
    : HashSet.make(collisionLeft, collisionRight),
});

const expectedSemanticKey = (amount: string, maybePresenceKey: string): string =>
  groupedKey([
    ["text", presentPresenceKey('"route"')],
    ["amount", presentPresenceKey(amount)],
    ["nested", presentPresenceKey('{"desk":"equities"}')],
    ["maybe", maybePresenceKey],
    ["hashMap", presentPresenceKey(`[["${collisionLeft}","left"],["${collisionRight}","right"]]`)],
    ["hashSet", presentPresenceKey(`["${collisionLeft}","${collisionRight}"]`)],
  ]);

describe("grouped key identity", () => {
  it("shares exact multi-field composition and semantic partitions across failure adapters", () => {
    const negativeZero = semanticRow(-0, false);
    const equivalent = semanticRow(0, true);
    const expected = expectedSemanticKey("0", missingPresenceKey);

    expect(throwingIdentity.key(negativeZero)).toBe(expected);
    expect(optionalIdentity.key(negativeZero)).toBe(expected);
    expect(throwingIdentity.key(equivalent)).toBe(expected);
    expect(optionalIdentity.key(equivalent)).toBe(throwingIdentity.key(equivalent));
  });

  it("distinguishes missing from present undefined with the same exact framing", () => {
    const missing = semanticRow(1, false);
    const present = { ...missing, maybe: undefined };
    const missingKey = throwingIdentity.key(missing);
    const presentKey = throwingIdentity.key(present);

    expect(missingKey).toBe(expectedSemanticKey("1", missingPresenceKey));
    expect(presentKey).toBe(expectedSemanticKey("1", presentPresenceKey("null")));
    expect(presentKey).not.toBe(missingKey);
    expect(optionalIdentity.key(missing)).toBe(missingKey);
    expect(optionalIdentity.key(present)).toBe(presentKey);
  });

  it("applies throwing and undefined failure policies to reflection and canonicalization", () => {
    const hostileRow = new Proxy(semanticRow(1, false), {
      getOwnPropertyDescriptor() {
        throw new Error("row reflection failed");
      },
    });
    const invalidRow = {
      ...semanticRow(1, false),
      text: 1,
    };

    expect(() => throwingIdentity.key(hostileRow)).toThrow("row reflection failed");
    expect(optionalIdentity.key(hostileRow)).toBeUndefined();
    expect(() => throwingIdentity.key(invalidRow)).toThrow();
    expect(optionalIdentity.key(invalidRow)).toBeUndefined();
  });
});
