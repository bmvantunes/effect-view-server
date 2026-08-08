import { describe, expect, it } from "@effect/vitest";
import { Chunk, HashMap, HashSet, Option, Result, Schema } from "effect";
import {
  makeSchemaJsonIdentity,
  makeSchemaJsonNormalizer,
  makeStrictJsonSchemaGuard,
} from "./schema-json-identity";
import { StrictJsonMaterializationError } from "./strict-json-materialization";

const collisionLeft = "8ocpIaaa";
const collisionRight = "GpcpIaaa";

class Profile extends Schema.Class<Profile>("Profile")({
  name: Schema.String,
}) {}

describe("Schema JSON identity", () => {
  it("keeps the direct string key path byte-identical and validates invalid inputs", () => {
    const identity = makeSchemaJsonIdentity(Schema.String);

    expect(identity.canonicalKey('route\\"north')).toBe('"route\\\\\\"north"');
    expect(identity.canonicalJson('route\\"north')).toBe('route\\"north');
    expect(() => identity.canonicalKey(1)).toThrow();
    expect(() => makeSchemaJsonIdentity(Schema.NonEmptyString).canonicalKey("")).toThrow();
  });

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
    const objectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.Struct({ payload: Schema.ObjectKeyword }),
    );
    const suspendedObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.suspend(() => Schema.Struct({ payload: Schema.ObjectKeyword })),
    );
    const arrayObjectKeywordIdentity = makeSchemaJsonIdentity(Schema.Array(Schema.ObjectKeyword));
    const tupleObjectKeywordIdentity = makeSchemaJsonIdentity(Schema.Tuple([Schema.ObjectKeyword]));
    const tupleRestObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.String, Schema.ObjectKeyword]),
    );

    const nonJson = new Map([["key", "value"]]);

    expect(() => identity.canonicalKey(nonJson)).toThrow("Expected JSON value");
    expect(() => identity.decodeEncoded(nonJson)).toThrow(
      "Expected a plain data record or dense array",
    );
    expect(() => objectKeywordIdentity.canonicalKey({ payload: nonJson })).toThrow(
      "Expected a plain data record or dense array at $.payload",
    );
    expect(objectKeywordIdentity.canonicalKey({ payload: { venue: "xnys" } })).toBe(
      '{"payload":{"venue":"xnys"}}',
    );
    expect(() => suspendedObjectKeywordIdentity.canonicalKey({ payload: nonJson })).toThrow(
      "Expected a plain data record or dense array at $.payload",
    );
    expect(() => arrayObjectKeywordIdentity.canonicalKey([nonJson])).toThrow(
      "Expected a plain data record or dense array at $[0]",
    );
    expect(() => tupleObjectKeywordIdentity.canonicalKey([nonJson])).toThrow(
      "Expected a plain data record or dense array at $[0]",
    );
    expect(() => tupleRestObjectKeywordIdentity.canonicalKey(["head", "middle", nonJson])).toThrow(
      "Expected a plain data record or dense array at $[2]",
    );

    const nestedObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.Struct({ nested: Schema.Struct({ payload: Schema.ObjectKeyword }) }),
    );
    expect(() =>
      nestedObjectKeywordIdentity.canonicalKey({ nested: { payload: nonJson } }),
    ).toThrow("Expected a plain data record or dense array at $.nested.payload");

    const dottedKeyIdentity = makeSchemaJsonIdentity(
      Schema.Record(Schema.String, Schema.ObjectKeyword),
    );
    expect(() => dottedKeyIdentity.canonicalKey({ "foo.bar": nonJson })).toThrow(
      'Expected a plain data record or dense array at $["foo.bar"]',
    );

    let nestedAccessorReads = 0;
    const nestedAccessorValue = {};
    Object.defineProperty(nestedAccessorValue, "payload", {
      configurable: true,
      enumerable: true,
      get: () => {
        nestedAccessorReads += 1;
        return { venue: "xnys" };
      },
    });
    expect(() => objectKeywordIdentity.canonicalKey(nestedAccessorValue)).toThrow(
      "Accessor properties are not valid JSON data at $.payload",
    );
    expect(nestedAccessorReads).toBe(0);

    let arrayAccessorReads = 0;
    const arrayAccessorValue: Array<unknown> = [];
    Object.defineProperty(arrayAccessorValue, "0", {
      configurable: true,
      enumerable: true,
      get: () => {
        arrayAccessorReads += 1;
        return { venue: "xnys" };
      },
    });
    arrayAccessorValue.length = 1;
    expect(() => arrayObjectKeywordIdentity.canonicalKey(arrayAccessorValue)).toThrow(
      "Accessor properties are not valid JSON data at $[0]",
    );
    expect(arrayAccessorReads).toBe(0);

    const unionObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.Union([
        Schema.Struct({ kind: Schema.Literal("payload"), payload: Schema.ObjectKeyword }),
        Schema.String,
      ]),
    );
    let unionAccessorReads = 0;
    const unionAccessorValue = { kind: "payload" };
    Object.defineProperty(unionAccessorValue, "payload", {
      configurable: true,
      enumerable: true,
      get: () => {
        unionAccessorReads += 1;
        return { venue: "xnys" };
      },
    });
    expect(() => unionObjectKeywordIdentity.canonicalKey(unionAccessorValue)).toThrow(
      "Accessor properties are not valid JSON data at $.payload",
    );
    expect(unionAccessorReads).toBe(0);
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

  it("returns typed strict JSON guard failures", () => {
    const guard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Struct({ payload: Schema.ObjectKeyword })).ast,
    );
    expect(guard("not-an-object")).toStrictEqual(Result.succeed(undefined));
    expect(guard(null)).toStrictEqual(Result.succeed(undefined));
    expect(guard(() => undefined)).toStrictEqual(Result.succeed(undefined));
    expect(guard({})).toStrictEqual(Result.succeed(undefined));
    expect(guard({ payload: { venue: "xnys" } })).toStrictEqual(Result.succeed(undefined));
    expect(guard({ payload: new Map([["venue", "xnys"]]) })).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "unsupported-prototype",
          message: "Expected a plain data record or dense array at $.payload.",
        }),
      ),
    );

    const nonEnumerablePayload = {};
    Object.defineProperty(nonEnumerablePayload, "payload", {
      configurable: true,
      enumerable: false,
      value: { venue: "xnys" },
    });
    expect(guard(nonEnumerablePayload)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "non-enumerable-property",
          message: "Expected an enumerable data property at $.payload.",
        }),
      ),
    );

    const propertyDescriptorFailure = new Proxy(
      { payload: { venue: "xnys" } },
      {
        getOwnPropertyDescriptor: (_target, key) => {
          if (key === "payload") {
            throw new Error("property descriptor failure");
          }
          return undefined;
        },
      },
    );
    expect(guard(propertyDescriptorFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.payload.",
        }),
      ),
    );

    const unionGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Union([Schema.String, Schema.Struct({ value: Schema.String })]))
        .ast,
    );
    const cyclicUnionValue: Record<string, unknown> = { value: "cyclic" };
    cyclicUnionValue["self"] = cyclicUnionValue;
    Object.defineProperty(cyclicUnionValue, Symbol("meta"), {
      configurable: true,
      enumerable: true,
      value: true,
    });
    expect(unionGuard(cyclicUnionValue)).toStrictEqual(Result.succeed(undefined));

    const missingDescriptor = new Proxy(
      {},
      {
        ownKeys: () => ["missing"],
        getOwnPropertyDescriptor: () => undefined,
      },
    );
    expect(unionGuard(missingDescriptor)).toStrictEqual(Result.succeed(undefined));

    const recordGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Record(Schema.String, Schema.ObjectKeyword)).ast,
    );
    expect(recordGuard(missingDescriptor)).toStrictEqual(Result.succeed(undefined));
    let flappingDescriptorReads = 0;
    const flappingDescriptor = new Proxy(
      {},
      {
        ownKeys: () => ["missing"],
        getOwnPropertyDescriptor: () => {
          flappingDescriptorReads += 1;
          return flappingDescriptorReads === 1
            ? { configurable: true, enumerable: true, value: {} }
            : undefined;
        },
      },
    );
    expect(recordGuard(flappingDescriptor)).toStrictEqual(Result.succeed(undefined));

    const descriptorFailure = new Proxy(
      { value: "descriptor-failure" },
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("descriptor failure");
        },
      },
    );
    expect(unionGuard(descriptorFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.value",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.value.",
        }),
      ),
    );

    const nonStrictFailure = new Proxy(
      { value: "hostile" },
      {
        get: () => {
          throw new Error("value read failure");
        },
      },
    );
    expect(unionGuard(nonStrictFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );

    const arrayGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Array(Schema.ObjectKeyword)).ast,
    );
    expect(arrayGuard("not-an-array")).toStrictEqual(Result.succeed(undefined));
    const sparseArray: Array<unknown> = [];
    sparseArray.length = 1;
    expect(arrayGuard(sparseArray)).toStrictEqual(Result.succeed(undefined));

    const tupleGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Tuple([Schema.ObjectKeyword])).ast,
    );
    expect(tupleGuard([{}, {}])).toStrictEqual(Result.succeed(undefined));
    const revoked = Proxy.revocable({ value: "hostile" }, {});
    revoked.revoke();
    expect(unionGuard(revoked.proxy)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );
  });
});
