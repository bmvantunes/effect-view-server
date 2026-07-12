import { describe, expect, it } from "@effect/vitest";
import { Chunk, HashMap, HashSet, Option, Schema } from "effect";
import { makeSchemaJsonIdentity, makeSchemaJsonNormalizer } from "./schema-json-identity";

const collisionLeft = "8ocpIaaa";
const collisionRight = "GpcpIaaa";

class Profile extends Schema.Class<Profile>("Profile")({
  name: Schema.String,
}) {}

describe("Schema JSON identity", () => {
  it("canonicalizes collision-node HashMap and HashSet values independent of insertion order", () => {
    const mapIdentity = makeSchemaJsonIdentity(Schema.HashMap(Schema.String, Schema.String));
    const setIdentity = makeSchemaJsonIdentity(Schema.HashSet(Schema.String));
    const leftMap = HashMap.make([collisionLeft, "left"], [collisionRight, "right"]);
    const rightMap = HashMap.make([collisionRight, "right"], [collisionLeft, "left"]);
    const leftSet = HashSet.make(collisionLeft, collisionRight);
    const rightSet = HashSet.make(collisionRight, collisionLeft);

    expect(mapIdentity.canonicalKey(leftMap)).toBe(mapIdentity.canonicalKey(rightMap));
    expect(setIdentity.canonicalKey(leftSet)).toBe(setIdentity.canonicalKey(rightSet));
    expect(mapIdentity.canonicalJson(leftMap)).toStrictEqual(mapIdentity.canonicalJson(rightMap));
    expect(setIdentity.canonicalJson(leftSet)).toStrictEqual(setIdentity.canonicalJson(rightSet));
  });

  it("normalizes nested unordered values without changing ordered collections", () => {
    const Nested = Schema.Struct({
      values: Schema.Array(
        Schema.Option(Schema.HashMap(Schema.String, Schema.HashSet(Schema.String))),
      ),
      ordered: Schema.Chunk(Schema.String),
    });
    const identity = makeSchemaJsonIdentity(Nested);
    const left = {
      values: [
        Option.some(
          HashMap.make(
            [collisionLeft, HashSet.make(collisionLeft, collisionRight)],
            [collisionRight, HashSet.make(collisionRight, collisionLeft)],
          ),
        ),
      ],
      ordered: Chunk.make("first", "second"),
    };
    const equivalent = {
      ordered: Chunk.make("first", "second"),
      values: [
        Option.some(
          HashMap.make(
            [collisionRight, HashSet.make(collisionLeft, collisionRight)],
            [collisionLeft, HashSet.make(collisionRight, collisionLeft)],
          ),
        ),
      ],
    };
    const reordered = {
      ...equivalent,
      ordered: Chunk.make("second", "first"),
    };

    expect(identity.canonicalKey(left)).toBe(identity.canonicalKey(equivalent));
    expect(identity.canonicalKey(left)).not.toBe(identity.canonicalKey(reordered));
  });

  it("uses the canonical class codec for separately instantiated values", () => {
    const identity = makeSchemaJsonIdentity(Profile);

    expect(identity.canonicalKey(Profile.make({ name: "Ada" }))).toBe(
      identity.canonicalKey(Profile.make({ name: "Ada" })),
    );
  });

  it("keeps decoded ownership materialization separate from encoded decoding", () => {
    const profileIdentity = makeSchemaJsonIdentity(Profile);
    const profile = Profile.make({ name: "Ada" });
    const materializedProfile = profileIdentity.materializeDecoded(profile);
    const bigintIdentity = makeSchemaJsonIdentity(Schema.BigInt);

    expect(materializedProfile).toStrictEqual(profile);
    expect(materializedProfile).not.toBe(profile);
    expect(() => bigintIdentity.materializeDecoded("9007199254740993")).toThrow();
    expect(bigintIdentity.decodeEncoded("9007199254740993")).toBe(9007199254740993n);
  });

  it("selects the matching tagged-union branch before normalizing nested values", () => {
    const Tagged = Schema.Union([
      Schema.Struct({
        kind: Schema.Literal("Map"),
        value: Schema.HashMap(Schema.String, Schema.String),
      }),
      Schema.Struct({
        kind: Schema.Literal("Array"),
        value: Schema.Array(Schema.String),
      }),
    ]);
    const identity = makeSchemaJsonIdentity(Tagged);

    expect(
      identity.canonicalKey({ kind: "Array", value: [collisionRight, collisionLeft] }),
    ).not.toBe(identity.canonicalKey({ kind: "Array", value: [collisionLeft, collisionRight] }));
  });

  it("normalizes record index values and preserves own __proto__ data", () => {
    const identity = makeSchemaJsonIdentity(
      Schema.Record(Schema.String, Schema.HashSet(Schema.String)),
    );
    const left: Record<string, HashSet.HashSet<string>> = {};
    const right: Record<string, HashSet.HashSet<string>> = {};
    Object.defineProperty(left, "__proto__", {
      enumerable: true,
      value: HashSet.make(collisionLeft, collisionRight),
    });
    Object.defineProperty(right, "__proto__", {
      enumerable: true,
      value: HashSet.make(collisionRight, collisionLeft),
    });

    expect(identity.canonicalKey(left)).toBe(identity.canonicalKey(right));
  });

  it("rejects encoded values that are not strict JSON", () => {
    const identity = makeSchemaJsonIdentity(Schema.Unknown);

    expect(() => identity.canonicalKey(new Map([["key", "value"]]))).toThrow(
      "Expected a plain data record or dense array",
    );
  });

  it("keeps the encoded normalizer total for non-matching JSON shapes", () => {
    const objectNormalizer = makeSchemaJsonNormalizer(
      Schema.toCodecJson(Schema.Struct({ value: Schema.String })).ast,
    );
    const arrayNormalizer = makeSchemaJsonNormalizer(
      Schema.toCodecJson(Schema.Array(Schema.String)).ast,
    );
    const mapNormalizer = makeSchemaJsonNormalizer(
      Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.String)).ast,
    );
    const unionNormalizer = makeSchemaJsonNormalizer(
      Schema.toCodecJson(Schema.Union([Schema.String, Schema.Number])).ast,
    );

    expect(objectNormalizer("value")).toBe("value");
    expect(objectNormalizer({ other: "value" })).toStrictEqual({ other: "value" });
    expect(arrayNormalizer("value")).toBe("value");
    expect(mapNormalizer("value")).toBe("value");
    expect(unionNormalizer(false)).toBe(false);
  });

  it("normalizes suspended schemas and fixed, rest, and trailing tuple elements", () => {
    const Suspended = Schema.suspend(() =>
      Schema.Struct({ values: Schema.HashSet(Schema.String) }),
    );
    const suspendedNormalizer = makeSchemaJsonNormalizer(Schema.toCodecJson(Suspended).ast);
    const tupleNormalizer = makeSchemaJsonNormalizer(
      Schema.toCodecJson(
        Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.String, Schema.Number]),
      ).ast,
    );

    expect(suspendedNormalizer({ values: [collisionRight, collisionLeft] })).toStrictEqual({
      values: [collisionLeft, collisionRight],
    });
    expect(suspendedNormalizer({ values: [collisionRight, collisionLeft] })).toStrictEqual({
      values: [collisionLeft, collisionRight],
    });
    expect(tupleNormalizer(["head", "rest", 1])).toStrictEqual(["head", "rest", 1]);
    expect(tupleNormalizer(["head", "rest", "extra", 1])).toStrictEqual([
      "head",
      "rest",
      "extra",
      1,
    ]);
    expect(
      makeSchemaJsonNormalizer(Schema.Tuple([Schema.String]).ast)(["head", "extra"]),
    ).toStrictEqual(["head", "extra"]);
  });
});
