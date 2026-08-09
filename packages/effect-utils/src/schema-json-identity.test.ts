import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Chunk,
  Effect,
  Exit,
  HashMap,
  HashSet,
  Option,
  Redacted,
  Result,
  Schema,
  SchemaAST,
  SchemaGetter,
  SchemaTransformation,
} from "effect";
import {
  makeStrictJsonSchemaCodec,
  makeSchemaJsonIdentity,
  makeSchemaJsonNormalizer,
  makeStrictJsonSchemaGuard,
  makeStrictJsonSchemaSnapshot,
} from "./schema-json-identity";
import { StrictJsonMaterializationError } from "./strict-json-materialization";

const collisionLeft = "8ocpIaaa";
const collisionRight = "GpcpIaaa";

class Profile extends Schema.Class<Profile>("Profile")({
  name: Schema.String,
}) {}

class ObjectKeywordProfile extends Schema.Class<ObjectKeywordProfile>("ObjectKeywordProfile")({
  metadata: Schema.ObjectKeyword,
}) {}

const CustomObjectDeclaration = Schema.declareConstructor()(
  [Schema.ObjectKeyword],
  () => (input) => Effect.succeed(input),
  {
    representation: { id: "custom/Box", payload: null },
    toCodecJson: ([value]) =>
      Schema.link()(Schema.Struct({ value }), {
        decode: SchemaGetter.transform((encoded) => ({ value: encoded.value })),
        encode: SchemaGetter.transform<{ readonly value: object }, unknown>(() => ({ value: {} })),
      }),
  },
);

const CustomObjectDeclarationNoParams = Schema.declare<{ readonly value: object }>(
  (input): input is { readonly value: object } =>
    typeof input === "object" && input !== null && "value" in input,
  {
    representation: { id: "custom/NoParamBox", payload: null },
    toCodecJson: () =>
      Schema.link()(Schema.Struct({ value: Schema.ObjectKeyword }), {
        decode: SchemaGetter.transform<{ readonly value: object }, { readonly value: object }>(
          (encoded) => ({ value: encoded.value }),
        ),
        encode: SchemaGetter.transform<{ readonly value: object }, unknown>(() => ({ value: {} })),
      }),
  },
);

const CustomHashSetRepresentation = Schema.declareConstructor()(
  [Schema.String],
  () => (input) => Effect.succeed(input),
  {
    representation: { id: "custom/schema/HashSet", payload: null },
    toCodecJson: ([value]) =>
      Schema.link()(Schema.Array(value), {
        decode: SchemaGetter.passthrough(),
        encode: SchemaGetter.passthrough<readonly string[], unknown>({ strict: false }),
      }),
  },
);

const ThrowingClassDeclaration = Schema.declareConstructor()(
  [Schema.ObjectKeyword],
  () => (input) => Effect.succeed(input),
  {
    representation: { id: "custom/Class", payload: null },
  },
);
Object.defineProperty(ThrowingClassDeclaration.ast.annotations, "~constructor", {
  configurable: true,
  enumerable: true,
  value: () => {
    throw new Error("class descriptor failure");
  },
  writable: true,
});

const SpoofedOptionDeclaration = Schema.declare<{ readonly value: object }>(
  (input): input is { readonly value: object } =>
    typeof input === "object" && input !== null && "value" in input,
  {
    representation: { id: "effect/schema/Option", payload: null },
    toCodecJson: () =>
      Schema.link()(Schema.Struct({ value: Schema.ObjectKeyword }), {
        decode: SchemaGetter.transform<{ readonly value: object }, { readonly value: object }>(
          (encoded) => ({ value: encoded.value }),
        ),
        encode: SchemaGetter.transform<{ readonly value: object }, unknown>(() => ({ value: {} })),
      }),
  },
);

const SpoofedOptionDeclarationWithCopiedAnnotations = Schema.declare<{
  readonly value: object;
}>(
  (input): input is { readonly value: object } =>
    typeof input === "object" && input !== null && "value" in input,
  {
    ...Schema.Option(Schema.ObjectKeyword).ast.annotations,
    toCodecJson: () =>
      Schema.link()(Schema.Struct({ value: Schema.ObjectKeyword }), {
        decode: SchemaGetter.transform<{ readonly value: object }, { readonly value: object }>(
          (encoded) => ({ value: encoded.value }),
        ),
        encode: SchemaGetter.transform<{ readonly value: object }, unknown>(() => ({ value: {} })),
      }),
  },
);

const ThrowingRunDeclaration = Schema.declare<{ readonly value: object }>(
  (input): input is { readonly value: object } =>
    typeof input === "object" && input !== null && "value" in input,
  {
    ...Schema.Option(Schema.ObjectKeyword).ast.annotations,
    toCodecJson: () =>
      Schema.link()(Schema.Struct({ value: Schema.ObjectKeyword }), {
        decode: SchemaGetter.transform<{ readonly value: object }, { readonly value: object }>(
          (encoded) => ({ value: encoded.value }),
        ),
        encode: SchemaGetter.transform<{ readonly value: object }, unknown>(() => ({ value: {} })),
      }),
  },
);
Object.defineProperty(ThrowingRunDeclaration.ast, "run", {
  configurable: true,
  enumerable: true,
  value: () => {
    throw new Error("run failure");
  },
  writable: true,
});

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

    const optionalMapIdentity = makeSchemaJsonIdentity(
      Schema.optional(Schema.HashMap(Schema.String, Schema.String)),
    );
    const optionalSetIdentity = makeSchemaJsonIdentity(
      Schema.optional(Schema.HashSet(Schema.String)),
    );
    const optionalKeySetIdentity = makeSchemaJsonIdentity(
      Schema.optionalKey(Schema.HashSet(Schema.String)),
    );
    expect(optionalMapIdentity.canonicalKey(leftMap)).toBe(
      optionalMapIdentity.canonicalKey(rightMap),
    );
    expect(optionalSetIdentity.canonicalKey(leftSet)).toBe(
      optionalSetIdentity.canonicalKey(rightSet),
    );
    expect(optionalKeySetIdentity.canonicalKey(leftSet)).toBe(
      optionalKeySetIdentity.canonicalKey(rightSet),
    );

    let hashMapIteratorCalls = 0;
    const statefulHashMap = HashMap.make(["key", { venue: "safe" }]);
    Object.defineProperty(statefulHashMap, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => {
        hashMapIteratorCalls += 1;
        return [["key", { venue: hashMapIteratorCalls === 1 ? "safe" : "evil" }]][
          Symbol.iterator
        ]();
      },
    });
    expect(
      makeSchemaJsonIdentity(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).canonicalJson(
        statefulHashMap,
      ),
    ).toStrictEqual([["key", { venue: "safe" }]]);
    expect(hashMapIteratorCalls).toBe(1);

    const infiniteIteratorHashMap = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(infiniteIteratorHashMap, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => ({
        next: () => ({ done: false, value: ["key", { venue: "xnys" }] }),
      }),
    });
    expect(() =>
      makeSchemaJsonIdentity(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).canonicalKey(
        infiniteIteratorHashMap,
      ),
    ).toThrow("Could not inspect JSON value at $.");

    const numericRecordIdentity = makeSchemaJsonIdentity(
      Schema.Record(Schema.Number, Schema.ObjectKeyword),
    );
    expect(() =>
      numericRecordIdentity.canonicalKey({
        "1": new Map([["venue", "xnys"]]),
      }),
    ).toThrow('Expected a plain data record or dense array at $["1"].');

    const numericRecordMapIdentity = makeSchemaJsonIdentity(
      Schema.Record(Schema.Number, Schema.HashMap(Schema.String, Schema.String)),
    );
    const numericRecordLeft = {
      "1": HashMap.make([collisionLeft, "left"], [collisionRight, "right"]),
    };
    const numericRecordRight = {
      "1": HashMap.make([collisionRight, "right"], [collisionLeft, "left"]),
    };
    expect(numericRecordMapIdentity.canonicalKey(numericRecordLeft)).toBe(
      numericRecordMapIdentity.canonicalKey(numericRecordRight),
    );

    const numericOrStringRecordIdentity = makeSchemaJsonIdentity(
      Schema.Record(Schema.Union([Schema.Number, Schema.String]), Schema.ObjectKeyword),
    );
    expect(() =>
      numericOrStringRecordIdentity.canonicalKey({
        "1": new Map([["venue", "xnys"]]),
      }),
    ).toThrow('Expected a plain data record or dense array at $["1"].');

    const unionRecordAst = new SchemaAST.Objects(
      [],
      [
        new SchemaAST.IndexSignature(
          new SchemaAST.Union([Schema.Number.ast, Schema.String.ast], "anyOf"),
          Schema.ObjectKeyword.ast,
        ),
      ],
    );
    expect(
      makeStrictJsonSchemaGuard(unionRecordAst)({
        "1": new Map([["venue", "xnys"]]),
      }),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: '$["1"]',
          reason: "unsupported-prototype",
          message: 'Expected a plain data record or dense array at $["1"].',
        }),
      ),
    );
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

  it("traverses ObjectKeyword fields in admitted Class declarations", () => {
    const identity = makeSchemaJsonIdentity(ObjectKeywordProfile);

    expect(
      identity.canonicalJson(ObjectKeywordProfile.make({ metadata: { venue: "xnys" } })),
    ).toStrictEqual({ metadata: { venue: "xnys" } });
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
    const shortTupleRestObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.TupleWithRest(Schema.Tuple([]), [Schema.ObjectKeyword, Schema.ObjectKeyword]),
    );

    const nonJson = new Map([["key", "value"]]);

    expect(() => identity.canonicalKey(nonJson)).toThrow(/^Expected JSON value$/);
    expect(() => identity.decodeEncoded(nonJson)).toThrow(
      /^Expected a plain data record or dense array at \$\.$/,
    );
    expect(() => objectKeywordIdentity.canonicalKey({ payload: nonJson })).toThrow(
      /^Expected a plain data record or dense array at \$\.payload\.$/,
    );
    expect(objectKeywordIdentity.canonicalKey({ payload: { venue: "xnys" } })).toBe(
      '{"payload":{"venue":"xnys"}}',
    );
    expect(
      objectKeywordIdentity.canonicalJson(
        Object.freeze({ payload: Object.freeze({ venue: "xnys" }) }),
      ),
    ).toStrictEqual({ payload: { venue: "xnys" } });
    expect(
      arrayObjectKeywordIdentity.canonicalJson(Object.freeze([{ venue: "xnys" }])),
    ).toStrictEqual([{ venue: "xnys" }]);
    expect(
      arrayObjectKeywordIdentity.canonicalJson(Object.seal([{ venue: "xnys" }])),
    ).toStrictEqual([{ venue: "xnys" }]);
    expect(() => suspendedObjectKeywordIdentity.canonicalKey({ payload: nonJson })).toThrow(
      /^Expected a plain data record or dense array at \$\.payload\.$/,
    );
    expect(() => arrayObjectKeywordIdentity.canonicalKey([nonJson])).toThrow(
      /^Expected a plain data record or dense array at \$\[0\]\.$/,
    );
    expect(() => tupleObjectKeywordIdentity.canonicalKey([nonJson])).toThrow(
      /^Expected a plain data record or dense array at \$\[0\]\.$/,
    );
    expect(() => tupleRestObjectKeywordIdentity.canonicalKey(["head", "middle", nonJson])).toThrow(
      /^Expected a plain data record or dense array at \$\[2\]\.$/,
    );
    expect(() => shortTupleRestObjectKeywordIdentity.canonicalKey([nonJson])).toThrow(
      /^Expected a plain data record or dense array at \$\[0\]\.$/,
    );

    const nestedObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.Struct({ nested: Schema.Struct({ payload: Schema.ObjectKeyword }) }),
    );
    expect(() =>
      nestedObjectKeywordIdentity.canonicalKey({ nested: { payload: nonJson } }),
    ).toThrow(/^Expected a plain data record or dense array at \$\.nested\.payload\.$/);

    const optionalObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.Struct({
        required: Schema.ObjectKeyword,
        optional: Schema.optional(Schema.ObjectKeyword),
        optionalKey: Schema.optionalKey(Schema.ObjectKeyword),
      }),
    );
    expect(() =>
      optionalObjectKeywordIdentity.canonicalKey({
        required: { venue: "xnys" },
        optional: nonJson,
        optionalKey: nonJson,
      }),
    ).toThrow(/^Expected a plain data record or dense array at \$\.optional\.$/);
    expect(() =>
      optionalObjectKeywordIdentity.canonicalKey({
        required: { venue: "xnys" },
        optionalKey: nonJson,
      }),
    ).toThrow(/^Expected a plain data record or dense array at \$\.optionalKey\.$/);

    expect(() =>
      makeSchemaJsonIdentity(CustomObjectDeclaration).canonicalKey({ value: nonJson }),
    ).toThrow(
      /^Cannot safely validate ObjectKeyword data inside an unknown schema declaration at \$\.$/,
    );
    expect(() =>
      makeSchemaJsonIdentity(CustomObjectDeclarationNoParams).canonicalKey({ value: nonJson }),
    ).toThrow(
      /^Cannot safely validate ObjectKeyword data inside an unknown schema declaration at \$\.$/,
    );
    expect(() =>
      makeSchemaJsonIdentity(SpoofedOptionDeclaration).canonicalKey({ value: nonJson }),
    ).toThrow(
      /^Cannot safely validate ObjectKeyword data inside an unknown schema declaration at \$\.$/,
    );
    expect(() =>
      makeSchemaJsonIdentity(SpoofedOptionDeclarationWithCopiedAnnotations).canonicalKey({
        value: nonJson,
      }),
    ).toThrow(
      /^Cannot safely validate ObjectKeyword data inside an unknown schema declaration at \$\.$/,
    );
    expect(() =>
      makeSchemaJsonIdentity(ThrowingRunDeclaration).canonicalKey({ value: nonJson }),
    ).toThrow(
      /^Cannot safely validate ObjectKeyword data inside an unknown schema declaration at \$\.$/,
    );
    Object.defineProperty(ThrowingRunDeclaration.ast, "run", {
      configurable: true,
      enumerable: true,
      value: () => 123,
      writable: true,
    });
    expect(() =>
      makeSchemaJsonIdentity(ThrowingRunDeclaration).canonicalKey({ value: nonJson }),
    ).toThrow(
      /^Cannot safely validate ObjectKeyword data inside an unknown schema declaration at \$\.$/,
    );

    const dottedKeyIdentity = makeSchemaJsonIdentity(
      Schema.Record(Schema.String, Schema.ObjectKeyword),
    );
    expect(() => dottedKeyIdentity.canonicalKey({ "foo.bar": nonJson })).toThrow(
      /^Expected a plain data record or dense array at \$\["foo\.bar"\]\.$/,
    );

    const declarationIdentities = [
      ["Option", makeSchemaJsonIdentity(Schema.Option(Schema.ObjectKeyword)), Option.some(nonJson)],
      [
        "HashMap",
        makeSchemaJsonIdentity(Schema.HashMap(Schema.String, Schema.ObjectKeyword)),
        HashMap.make(["key", nonJson]),
      ],
      [
        "HashSet",
        makeSchemaJsonIdentity(Schema.HashSet(Schema.ObjectKeyword)),
        HashSet.make(nonJson),
      ],
      ["Chunk", makeSchemaJsonIdentity(Schema.Chunk(Schema.ObjectKeyword)), Chunk.make(nonJson)],
    ] as const;
    for (const [name, declarationIdentity, value] of declarationIdentities) {
      expect(() => declarationIdentity.canonicalKey(value), name).toThrow(
        /^Expected a plain data record or dense array at \$\.(?:value|entries\[0\]\[1\]|values\[0\])\.$/,
      );
    }

    const resultIdentity = makeSchemaJsonIdentity(
      Schema.Result(Schema.ObjectKeyword, Schema.ObjectKeyword),
    );
    expect(() => resultIdentity.canonicalKey(Result.succeed(nonJson))).toThrow(
      /^Expected a plain data record or dense array at \$\.success\.$/,
    );
    expect(() => resultIdentity.canonicalKey(Result.fail(nonJson))).toThrow(
      /^Expected a plain data record or dense array at \$\.failure\.$/,
    );

    const readonlyMapIdentity = makeSchemaJsonIdentity(
      Schema.ReadonlyMap(Schema.String, Schema.ObjectKeyword),
    );
    expect(() => readonlyMapIdentity.canonicalKey(new Map([["key", nonJson]]))).toThrow(
      /^Expected a plain data record or dense array at \$\.entries\[0\]\[1\]\.$/,
    );
    const accessorReadonlyMap = new Map([["key", { venue: "xnys" }]]);
    Object.defineProperty(accessorReadonlyMap, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      get: () => {
        throw new Error("iterator getter");
      },
    });
    expect(() => readonlyMapIdentity.canonicalKey(accessorReadonlyMap)).toThrow(
      /^Accessor properties are not valid JSON data at \$\[Symbol\(Symbol\.iterator\)\]\.$/,
    );
    const dataReadonlyMap = new Map([["key", { venue: "xnys" }]]);
    Object.defineProperty(dataReadonlyMap, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => [][Symbol.iterator](),
    });
    expect(() => readonlyMapIdentity.canonicalKey(dataReadonlyMap)).toThrow(
      /^Could not inspect JSON value at \$\[Symbol\(Symbol\.iterator\)\]\.$/,
    );
    const readonlySetIdentity = makeSchemaJsonIdentity(Schema.ReadonlySet(Schema.ObjectKeyword));
    expect(() => readonlySetIdentity.canonicalKey(new Set([nonJson]))).toThrow(
      /^Expected a plain data record or dense array at \$\.values\[0\]\.$/,
    );
    const accessorReadonlySet = new Set([{ venue: "xnys" }]);
    Object.defineProperty(accessorReadonlySet, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      get: () => {
        throw new Error("iterator getter");
      },
    });
    expect(() => readonlySetIdentity.canonicalKey(accessorReadonlySet)).toThrow(
      /^Accessor properties are not valid JSON data at \$\[Symbol\(Symbol\.iterator\)\]\.$/,
    );
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.ReadonlySet(Schema.ObjectKeyword)).ast)(
        "not-a-set",
      ),
    ).toStrictEqual(Result.succeed(undefined));

    const causeReasonIdentity = makeSchemaJsonIdentity(
      Schema.CauseReason(Schema.ObjectKeyword, Schema.ObjectKeyword),
    );
    expect(() => causeReasonIdentity.canonicalKey(Cause.fail(nonJson).reasons[0]!)).toThrow(
      /^Expected a plain data record or dense array at \$\.error\.$/,
    );
    expect(() => causeReasonIdentity.canonicalKey(Cause.die(nonJson).reasons[0]!)).toThrow(
      /^Expected a plain data record or dense array at \$\.defect\.$/,
    );

    const causeIdentity = makeSchemaJsonIdentity(
      Schema.Cause(Schema.ObjectKeyword, Schema.ObjectKeyword),
    );
    expect(() => causeIdentity.canonicalKey(Cause.fail(nonJson))).toThrow(
      /^Expected a plain data record or dense array at \$\.reasons\[0\]\.error\.$/,
    );

    const exitIdentity = makeSchemaJsonIdentity(
      Schema.Exit(Schema.ObjectKeyword, Schema.ObjectKeyword, Schema.ObjectKeyword),
    );
    expect(() => exitIdentity.canonicalKey(Exit.succeed(nonJson))).toThrow(
      /^Expected a plain data record or dense array at \$\.value\.$/,
    );
    expect(() => exitIdentity.canonicalKey(Exit.failCause(Cause.fail(nonJson)))).toThrow(
      /^Expected a plain data record or dense array at \$\.cause\.reasons\[0\]\.error\.$/,
    );

    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )("not-a-hash-map"),
    ).toStrictEqual(Result.succeed(undefined));
    const invalidHashMapIterator = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(invalidHashMapIterator, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => ["not-an-entry"][Symbol.iterator](),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(invalidHashMapIterator),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.entries[0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.entries[0].",
        }),
      ),
    );
    const throwingHashMapIterator = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(throwingHashMapIterator, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => {
        throw new Error("iterator failure");
      },
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(throwingHashMapIterator),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );
    const missingHashMapIterator = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(missingHashMapIterator, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: 1,
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(missingHashMapIterator),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[Symbol(Symbol.iterator)]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $[Symbol(Symbol.iterator)].",
        }),
      ),
    );
    const absentHashMapIterator = HashMap.make(["key", { venue: "xnys" }]);
    Object.setPrototypeOf(absentHashMapIterator, null);
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(absentHashMapIterator),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[Symbol(Symbol.iterator)]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $[Symbol(Symbol.iterator)].",
        }),
      ),
    );
    let hashMapIteratorReads = 0;
    const accessorHashMapIterator = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(accessorHashMapIterator, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      get: () => {
        hashMapIteratorReads += 1;
        return () => [][Symbol.iterator]();
      },
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(accessorHashMapIterator),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[Symbol(Symbol.iterator)]",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $[Symbol(Symbol.iterator)].",
        }),
      ),
    );
    expect(hashMapIteratorReads).toBe(0);
    const hashMapPrimitiveIterator = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(hashMapPrimitiveIterator, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => 1,
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(hashMapPrimitiveIterator),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[Symbol(Symbol.iterator)]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $[Symbol(Symbol.iterator)].",
        }),
      ),
    );
    let hashMapEntryReads = 0;
    const accessorHashMapEntry = ["key", { venue: "xnys" }];
    Object.defineProperty(accessorHashMapEntry, "0", {
      configurable: true,
      enumerable: true,
      get: () => {
        hashMapEntryReads += 1;
        return "key";
      },
    });
    const hashMapWithAccessorEntry = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(hashMapWithAccessorEntry, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => [accessorHashMapEntry][Symbol.iterator](),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(hashMapWithAccessorEntry),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.entries[0][0]",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.entries[0][0].",
        }),
      ),
    );
    expect(hashMapEntryReads).toBe(0);
    const sparseHashMapEntry = HashMap.make(["key", { venue: "xnys" }]);
    const sparseEntry: Array<unknown> = [];
    sparseEntry.length = 2;
    sparseEntry[0] = "key";
    Object.defineProperty(sparseHashMapEntry, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => [sparseEntry][Symbol.iterator](),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(sparseHashMapEntry),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.entries[0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.entries[0].",
        }),
      ),
    );
    let hashMapIteratorResultReads = 0;
    const iteratorResult = { done: false, value: ["key", { venue: "xnys" }] };
    Object.defineProperty(iteratorResult, "value", {
      configurable: true,
      enumerable: true,
      get: () => {
        hashMapIteratorResultReads += 1;
        return ["key", { venue: "xnys" }];
      },
    });
    let iteratorResultStep = 0;
    const hashMapWithAccessorIteratorResult = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(hashMapWithAccessorIteratorResult, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => ({
        next: () => (iteratorResultStep++ === 0 ? iteratorResult : { done: true }),
      }),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(hashMapWithAccessorIteratorResult),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[Symbol(Symbol.iterator)][0].value",
          reason: "accessor-property",
          message:
            "Accessor properties are not valid JSON data at $[Symbol(Symbol.iterator)][0].value.",
        }),
      ),
    );
    expect(hashMapIteratorResultReads).toBe(0);
    const hashMapMissingIteratorNext = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(hashMapMissingIteratorNext, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => ({}),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(hashMapMissingIteratorNext),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[Symbol(Symbol.iterator)].next",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $[Symbol(Symbol.iterator)].next.",
        }),
      ),
    );
    const hashMapThrowingIteratorNext = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(hashMapThrowingIteratorNext, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => ({
        next: () => {
          throw new Error("next failure");
        },
      }),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(hashMapThrowingIteratorNext),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );
    const hashMapPrimitiveIteratorResult = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(hashMapPrimitiveIteratorResult, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => ({ next: () => 1 }),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(hashMapPrimitiveIteratorResult),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[Symbol(Symbol.iterator)][0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $[Symbol(Symbol.iterator)][0].",
        }),
      ),
    );
    let missingIteratorValueStep = 0;
    const hashMapMissingIteratorValue = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(hashMapMissingIteratorValue, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => ({
        next: () => (missingIteratorValueStep++ === 0 ? { done: false } : { done: true }),
      }),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(hashMapMissingIteratorValue),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.entries[0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.entries[0].",
        }),
      ),
    );
    const shortHashMapEntry = HashMap.make(["key", { venue: "xnys" }]);
    Object.defineProperty(shortHashMapEntry, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => [["key"]][Symbol.iterator](),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(shortHashMapEntry),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.entries[0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.entries[0].",
        }),
      ),
    );
    const invalidLengthHashMapEntry = HashMap.make(["key", { venue: "xnys" }]);
    const invalidLengthEntry = new Proxy(["key", { venue: "xnys" }], {
      getOwnPropertyDescriptor: (target, key) =>
        key === "length"
          ? { configurable: false, enumerable: false, value: "bad", writable: true }
          : Reflect.getOwnPropertyDescriptor(target, key),
    });
    Object.defineProperty(invalidLengthHashMapEntry, Symbol.iterator, {
      configurable: true,
      enumerable: true,
      value: () => [invalidLengthEntry][Symbol.iterator](),
    });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(invalidLengthHashMapEntry),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.entries[0].length",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.entries[0].length.",
        }),
      ),
    );
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.HashSet(Schema.ObjectKeyword)).ast)(
        "not-a-hash-set",
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.Chunk(Schema.ObjectKeyword)).ast)(
        "not-a-chunk",
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.Redacted(Schema.ObjectKeyword)).ast)(
        "not-redacted",
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.Exit(Schema.ObjectKeyword, Schema.String, Schema.String)).ast,
      )("not-an-exit"),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.ReadonlyMap(Schema.String, Schema.ObjectKeyword)).ast,
      )("not-a-map"),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.Cause(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
      )("not-a-cause"),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.Option(Schema.ObjectKeyword)).ast)(
        Option.none(),
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.Option(Schema.ObjectKeyword)).ast)(
        "not-an-option",
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.Result(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
      )("not-a-result"),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.CauseReason(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
      )("not-a-reason"),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaSnapshot(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
        "encoded",
      )([["key", nonJson]]),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[0][1]",
          reason: "unsupported-prototype",
          message: "Expected a plain data record or dense array at $[0][1].",
        }),
      ),
    );
    expect(
      makeStrictJsonSchemaSnapshot(
        Schema.toCodecJson(Schema.HashSet(Schema.ObjectKeyword)).ast,
        "encoded",
      )([nonJson]),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[0]",
          reason: "unsupported-prototype",
          message: "Expected a plain data record or dense array at $[0].",
        }),
      ),
    );
    expect(
      makeStrictJsonSchemaSnapshot(
        Schema.toCodecJson(Schema.Chunk(Schema.ObjectKeyword)).ast,
        "encoded",
      )([nonJson]),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$[0]",
          reason: "unsupported-prototype",
          message: "Expected a plain data record or dense array at $[0].",
        }),
      ),
    );

    const optionGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Option(Schema.ObjectKeyword)).ast,
    );
    const missingOption = Option.some({ venue: "xnys" });
    Reflect.deleteProperty(missingOption, "value");
    expect(optionGuard(missingOption)).toStrictEqual(Result.succeed(undefined));
    let optionReads = 0;
    const accessorOption = Option.some({ venue: "xnys" });
    Object.defineProperty(accessorOption, "value", {
      configurable: true,
      enumerable: true,
      get: () => {
        optionReads += 1;
        return { venue: "xnys" };
      },
    });
    expect(optionGuard(accessorOption)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.value",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.value.",
        }),
      ),
    );
    expect(optionReads).toBe(0);
    let optionTagReads = 0;
    const accessorOptionTag = Option.some({ venue: "xnys" });
    Object.defineProperty(accessorOptionTag, "_tag", {
      configurable: true,
      enumerable: true,
      get: () => {
        optionTagReads += 1;
        return "Some";
      },
    });
    expect(optionGuard(accessorOptionTag)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$._tag",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $._tag.",
        }),
      ),
    );
    expect(optionTagReads).toBe(0);
    const nullPrototypeOption = new Proxy(
      {},
      {
        has: () => true,
        getPrototypeOf: () => null,
      },
    );
    expect(optionGuard(nullPrototypeOption)).toStrictEqual(Result.succeed(undefined));
    const hostilePrototypeOption = new Proxy(
      {},
      {
        has: () => true,
        getPrototypeOf: () => {
          throw new Error("prototype failure");
        },
      },
    );
    expect(optionGuard(hostilePrototypeOption)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$._tag",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $._tag.",
        }),
      ),
    );

    const resultGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Result(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
    );
    const unknownResultTag = Result.succeed({ venue: "xnys" });
    Object.defineProperty(unknownResultTag, "_tag", {
      configurable: true,
      enumerable: true,
      value: "Unknown",
    });
    expect(resultGuard(unknownResultTag)).toStrictEqual(Result.succeed(undefined));
    const missingSuccess = Result.succeed({ venue: "xnys" });
    Reflect.deleteProperty(missingSuccess, "success");
    expect(resultGuard(missingSuccess)).toStrictEqual(Result.succeed(undefined));
    const missingFailure = Result.fail({ venue: "xnys" });
    Reflect.deleteProperty(missingFailure, "failure");
    expect(resultGuard(missingFailure)).toStrictEqual(Result.succeed(undefined));
    let resultReads = 0;
    const accessorResult = Result.succeed({ venue: "xnys" });
    Object.defineProperty(accessorResult, "success", {
      configurable: true,
      enumerable: true,
      get: () => {
        resultReads += 1;
        return { venue: "xnys" };
      },
    });
    expect(resultGuard(accessorResult)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.success",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.success.",
        }),
      ),
    );
    expect(resultReads).toBe(0);
    let resultTagReads = 0;
    const accessorResultTag = Result.succeed({ venue: "xnys" });
    Object.defineProperty(accessorResultTag, "_tag", {
      configurable: true,
      enumerable: true,
      get: () => {
        resultTagReads += 1;
        return "Success";
      },
    });
    expect(resultGuard(accessorResultTag)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$._tag",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $._tag.",
        }),
      ),
    );
    expect(resultTagReads).toBe(0);

    const causeReasonGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.CauseReason(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
    );
    const missingErrorReason = Cause.fail({ venue: "xnys" }).reasons[0]!;
    Reflect.deleteProperty(missingErrorReason, "error");
    expect(causeReasonGuard(missingErrorReason)).toStrictEqual(Result.succeed(undefined));
    const missingDefectReason = Cause.die({ venue: "xnys" }).reasons[0]!;
    Reflect.deleteProperty(missingDefectReason, "defect");
    expect(causeReasonGuard(missingDefectReason)).toStrictEqual(Result.succeed(undefined));
    let causeReads = 0;
    const accessorReason = Cause.fail({ venue: "xnys" }).reasons[0]!;
    Object.defineProperty(accessorReason, "error", {
      configurable: true,
      enumerable: true,
      get: () => {
        causeReads += 1;
        return { venue: "xnys" };
      },
    });
    expect(causeReasonGuard(accessorReason)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.error",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.error.",
        }),
      ),
    );
    expect(causeReads).toBe(0);
    let causeTagReads = 0;
    const accessorReasonTag = Cause.fail({ venue: "xnys" }).reasons[0]!;
    Object.defineProperty(accessorReasonTag, "_tag", {
      configurable: true,
      enumerable: true,
      get: () => {
        causeTagReads += 1;
        return "Fail";
      },
    });
    expect(causeReasonGuard(accessorReasonTag)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$._tag",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $._tag.",
        }),
      ),
    );
    expect(causeTagReads).toBe(0);

    const causeGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Cause(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
    );
    const missingReasons = Cause.fail({ venue: "xnys" });
    Reflect.deleteProperty(missingReasons, "reasons");
    expect(causeGuard(missingReasons)).toStrictEqual(Result.succeed(undefined));
    const nonArrayReasons = Cause.fail({ venue: "xnys" });
    Object.defineProperty(nonArrayReasons, "reasons", {
      configurable: true,
      enumerable: true,
      value: { not: "an array" },
      writable: true,
    });
    expect(causeGuard(nonArrayReasons)).toStrictEqual(Result.succeed(undefined));
    const invalidReason = Cause.fail({ venue: "xnys" });
    Object.defineProperty(invalidReason, "reasons", {
      configurable: true,
      enumerable: true,
      value: [{}],
      writable: true,
    });
    expect(causeGuard(invalidReason)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.reasons[0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.reasons[0].",
        }),
      ),
    );
    const primitiveReason = Cause.fail({ venue: "xnys" });
    Object.defineProperty(primitiveReason, "reasons", {
      configurable: true,
      enumerable: true,
      value: [1],
      writable: true,
    });
    expect(causeGuard(primitiveReason)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.reasons[0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.reasons[0].",
        }),
      ),
    );
    const invalidFiberReason = Cause.interrupt().reasons[0]!;
    Object.defineProperty(invalidFiberReason, "fiberId", {
      configurable: true,
      enumerable: true,
      value: "bad",
      writable: true,
    });
    expect(causeReasonGuard(invalidFiberReason)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.fiberId",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.fiberId.",
        }),
      ),
    );
    const missingFiberReason = Cause.interrupt().reasons[0]!;
    Reflect.deleteProperty(missingFiberReason, "fiberId");
    expect(causeReasonGuard(missingFiberReason)).toStrictEqual(Result.succeed(undefined));
    const unknownReasonTag = Cause.interrupt().reasons[0]!;
    Object.defineProperty(unknownReasonTag, "_tag", {
      configurable: true,
      enumerable: true,
      value: "Unknown",
      writable: true,
    });
    expect(causeReasonGuard(unknownReasonTag)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );
    expect(causeGuard(Cause.fail({ venue: "xnys" }))).toStrictEqual(Result.succeed(undefined));
    let reasonEntryReads = 0;
    const accessorReasonEntry = Cause.fail({ venue: "xnys" });
    Object.defineProperty(accessorReasonEntry.reasons, "0", {
      configurable: true,
      enumerable: true,
      get: () => {
        reasonEntryReads += 1;
        return Cause.fail({ venue: "xnys" }).reasons[0];
      },
    });
    expect(causeGuard(accessorReasonEntry)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.reasons[0]",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.reasons[0].",
        }),
      ),
    );
    expect(reasonEntryReads).toBe(0);
    const sparseReasons = Cause.fail({ venue: "xnys" });
    Reflect.deleteProperty(sparseReasons.reasons, "0");
    expect(causeGuard(sparseReasons)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.reasons[0]",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.reasons[0].",
        }),
      ),
    );
    const invalidLengthReasons = Cause.fail({ venue: "xnys" });
    const invalidLengthReasonArray = new Proxy(invalidLengthReasons.reasons, {
      getOwnPropertyDescriptor: (target, key) =>
        key === "length"
          ? { configurable: false, enumerable: false, value: "bad", writable: true }
          : Reflect.getOwnPropertyDescriptor(target, key),
    });
    Object.defineProperty(invalidLengthReasons, "reasons", {
      configurable: true,
      enumerable: true,
      value: invalidLengthReasonArray,
      writable: true,
    });
    expect(causeGuard(invalidLengthReasons)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.reasons.length",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.reasons.length.",
        }),
      ),
    );

    const exitGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(
        Schema.Exit(Schema.ObjectKeyword, Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).ast,
    );
    const missingExit = Exit.succeed({ venue: "xnys" });
    Reflect.deleteProperty(missingExit, "~effect/Effect/args");
    expect(exitGuard(missingExit)).toStrictEqual(Result.succeed(undefined));
    const unknownExitTag = Exit.succeed({ venue: "xnys" });
    Object.defineProperty(unknownExitTag, "_tag", {
      configurable: true,
      enumerable: true,
      value: "Unknown",
    });
    expect(exitGuard(unknownExitTag)).toStrictEqual(Result.succeed(undefined));
    const ownValueExit = Exit.succeed({ venue: "xnys" });
    Object.defineProperty(ownValueExit, "value", {
      configurable: true,
      enumerable: true,
      value: { venue: "xnys" },
      writable: true,
    });
    expect(exitGuard(ownValueExit)).toStrictEqual(Result.succeed(undefined));
    let exitReads = 0;
    const accessorExit = Exit.succeed({ venue: "xnys" });
    Object.defineProperty(accessorExit, "value", {
      configurable: true,
      enumerable: true,
      get: () => {
        exitReads += 1;
        return { venue: "xnys" };
      },
    });
    expect(exitGuard(accessorExit)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.value",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.value.",
        }),
      ),
    );
    expect(exitReads).toBe(0);
    let exitTagReads = 0;
    const accessorExitTag = Exit.succeed({ venue: "xnys" });
    Object.defineProperty(accessorExitTag, "_tag", {
      configurable: true,
      enumerable: true,
      get: () => {
        exitTagReads += 1;
        return "Success";
      },
    });
    expect(exitGuard(accessorExitTag)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$._tag",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $._tag.",
        }),
      ),
    );
    expect(exitTagReads).toBe(0);

    const redactedGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Redacted(Schema.ObjectKeyword)).ast,
    );
    let labelReads = 0;
    const accessorLabel = Redacted.make({ venue: "xnys" });
    Object.defineProperty(accessorLabel, "label", {
      configurable: true,
      enumerable: true,
      get: () => {
        labelReads += 1;
        return "profile";
      },
    });
    expect(redactedGuard(accessorLabel)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.label",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.label.",
        }),
      ),
    );
    expect(labelReads).toBe(0);

    const redactedIdentity = makeSchemaJsonIdentity(Schema.Redacted(Schema.ObjectKeyword));
    expect(() => redactedIdentity.canonicalKey(Redacted.make(nonJson))).toThrow(
      /^Expected a plain data record or dense array at \$\.value\.$/,
    );

    const validObject = { venue: "xnys" };
    expect(
      makeSchemaJsonIdentity(Schema.Option(Schema.ObjectKeyword)).canonicalJson(
        Option.some(validObject),
      ),
    ).toStrictEqual({ _tag: "Some", value: validObject });
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.Option(Schema.ObjectKeyword)).ast)(
        Option.some(validObject),
      ),
    ).toStrictEqual(Result.succeed(undefined));
    const resultGuardForValidValues = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Result(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
    );
    expect(resultGuardForValidValues(Result.succeed(validObject))).toStrictEqual(
      Result.succeed(undefined),
    );
    expect(resultGuardForValidValues(Result.fail(validObject))).toStrictEqual(
      Result.succeed(undefined),
    );
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.CauseReason(Schema.ObjectKeyword, Schema.ObjectKeyword)).ast,
      )(Cause.die(validObject).reasons[0]!),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.Redacted(Schema.ObjectKeyword)).ast)(
        Redacted.make(validObject),
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeSchemaJsonIdentity(
        Schema.Result(Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Result.succeed(validObject)),
    ).toStrictEqual({ _tag: "Success", success: validObject });
    expect(
      makeSchemaJsonIdentity(
        Schema.Result(Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Result.fail(validObject)),
    ).toStrictEqual({ _tag: "Failure", failure: validObject });
    expect(
      makeSchemaJsonIdentity(Schema.ReadonlyMap(Schema.String, Schema.ObjectKeyword)).canonicalJson(
        new Map([["key", validObject]]),
      ),
    ).toStrictEqual([["key", validObject]]);
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.ReadonlyMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(new Map([["key", validObject]])),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeSchemaJsonIdentity(Schema.ReadonlySet(Schema.ObjectKeyword)).canonicalJson(
        new Set([validObject]),
      ),
    ).toStrictEqual([validObject]);
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.ReadonlySet(Schema.ObjectKeyword)).ast)(
        new Set([validObject]),
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeSchemaJsonIdentity(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).canonicalJson(
        HashMap.make(["key", validObject]),
      ),
    ).toStrictEqual([["key", validObject]]);
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(Schema.HashMap(Schema.String, Schema.ObjectKeyword)).ast,
      )(HashMap.make(["key", validObject])),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeSchemaJsonIdentity(Schema.HashSet(Schema.ObjectKeyword)).canonicalJson(
        HashSet.make(validObject),
      ),
    ).toStrictEqual([validObject]);
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.HashSet(Schema.ObjectKeyword)).ast)(
        HashSet.make(validObject),
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeSchemaJsonIdentity(Schema.Chunk(Schema.ObjectKeyword)).canonicalJson(
        Chunk.make(validObject),
      ),
    ).toStrictEqual([validObject]);
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(Schema.Chunk(Schema.ObjectKeyword)).ast)(
        Chunk.make(validObject),
      ),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeSchemaJsonIdentity(
        Schema.CauseReason(Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Cause.fail(validObject).reasons[0]!),
    ).toStrictEqual({ _tag: "Fail", error: validObject });
    expect(
      makeSchemaJsonIdentity(
        Schema.CauseReason(Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Cause.die(validObject).reasons[0]!),
    ).toStrictEqual({ _tag: "Die", defect: validObject });
    expect(
      makeSchemaJsonIdentity(
        Schema.CauseReason(Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Cause.interrupt().reasons[0]!),
    ).toStrictEqual({ _tag: "Interrupt", fiberId: null });
    expect(
      makeSchemaJsonIdentity(
        Schema.Cause(Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Cause.fail(validObject)),
    ).toStrictEqual([{ _tag: "Fail", error: validObject }]);
    expect(
      makeSchemaJsonIdentity(
        Schema.Exit(Schema.ObjectKeyword, Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Exit.succeed(validObject)),
    ).toStrictEqual({ _tag: "Success", value: validObject });
    expect(
      makeStrictJsonSchemaGuard(
        Schema.toCodecJson(
          Schema.Exit(Schema.ObjectKeyword, Schema.ObjectKeyword, Schema.ObjectKeyword),
        ).ast,
      )(Exit.failCause(Cause.interrupt())),
    ).toStrictEqual(Result.succeed(undefined));
    expect(
      makeSchemaJsonIdentity(
        Schema.Exit(Schema.ObjectKeyword, Schema.ObjectKeyword, Schema.ObjectKeyword),
      ).canonicalJson(Exit.failCause(Cause.fail(validObject))),
    ).toStrictEqual({ _tag: "Failure", cause: [{ _tag: "Fail", error: validObject }] });
    expect(
      makeSchemaJsonIdentity(Schema.Redacted(Schema.ObjectKeyword)).canonicalJson(
        Redacted.make(validObject),
      ),
    ).toStrictEqual(validObject);
    expect(
      makeSchemaJsonIdentity(Schema.Redacted(Schema.ObjectKeyword)).canonicalJson(
        Redacted.make(validObject, { label: "profile" }),
      ),
    ).toStrictEqual(validObject);

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

    const transformedObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.String.pipe(
        Schema.encodeTo(Schema.Struct({ payload: Schema.ObjectKeyword }), {
          decode: SchemaGetter.transform(() => "decoded"),
          encode: SchemaGetter.transform(() => ({
            payload: new Map([["venue", "xnys"]]),
          })),
        }),
      ),
    );
    expect(() => transformedObjectKeywordIdentity.canonicalKey("input")).toThrow(
      /^Expected a plain data record or dense array at \$\.payload\.$/,
    );

    const transformedUnionObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.Union([
        Schema.String.pipe(
          Schema.encodeTo(Schema.Struct({ payload: Schema.ObjectKeyword }), {
            decode: SchemaGetter.transform(() => "decoded"),
            encode: SchemaGetter.transform(() => ({
              payload: new Map([["venue", "xnys"]]),
            })),
          }),
        ),
        Schema.Number,
      ]),
    );
    expect(() => transformedUnionObjectKeywordIdentity.canonicalKey("input")).toThrow(
      /^Expected a plain data record or dense array at \$\.payload\.$/,
    );

    let transformedEncodeCalls = 0;
    const validTransformedObjectKeywordIdentity = makeSchemaJsonIdentity(
      Schema.String.pipe(
        Schema.encodeTo(Schema.Struct({ payload: Schema.ObjectKeyword }), {
          decode: SchemaGetter.transform(() => "decoded"),
          encode: SchemaGetter.transform(() => {
            transformedEncodeCalls += 1;
            return { payload: { venue: "xnys" } };
          }),
        }),
      ),
    );
    expect(validTransformedObjectKeywordIdentity.canonicalKey("input")).toBe(
      '{"payload":{"venue":"xnys"}}',
    );
    expect(transformedEncodeCalls).toBe(1);

    const decodedHashMapSchema = Schema.Struct({
      payload: Schema.HashMap(Schema.String, Schema.String),
    }).pipe(
      Schema.encodeTo(Schema.Struct({ payload: Schema.ObjectKeyword }), {
        decode: SchemaGetter.transform(() => ({ payload: HashMap.empty<string, string>() })),
        encode: SchemaGetter.transform((value) => ({
          payload: Object.fromEntries(HashMap.toEntries(value.payload)),
        })),
      }),
    );
    expect(
      makeSchemaJsonIdentity(decodedHashMapSchema).canonicalJson({
        payload: HashMap.make(["venue", "xnys"]),
      }),
    ).toStrictEqual({ payload: { venue: "xnys" } });
  });

  it("guards transformed record keys on the encoded side", () => {
    const transformedKey = Schema.String.pipe(Schema.decode(SchemaTransformation.snakeToCamel()));
    const identity = makeSchemaJsonIdentity(Schema.Record(transformedKey, Schema.ObjectKeyword));

    expect(identity.canonicalJson({ payloadValue: { venue: "xnys" } })).toStrictEqual({
      payload_value: { venue: "xnys" },
    });
    expect(() => identity.canonicalKey({ payloadValue: new Map([["venue", "xnys"]]) })).toThrow(
      /^Expected a plain data record or dense array at \$\.payloadValue\.$/,
    );
  });

  it("passes the guarded ObjectKeyword snapshot into JSON encoding", () => {
    let reads = 0;
    const mutablePayload = new Proxy(
      { venue: "xnys" },
      {
        get: (target, key, receiver) => {
          if (key === "venue") {
            reads += 1;
            return reads === 1 ? new Map([["venue", "xnys"]]) : Reflect.get(target, key, receiver);
          }
          return Reflect.get(target, key, receiver);
        },
      },
    );
    const identity = makeSchemaJsonIdentity(
      Schema.String.pipe(
        Schema.encodeTo(Schema.Struct({ payload: Schema.ObjectKeyword }), {
          decode: SchemaGetter.transform(() => "decoded"),
          encode: SchemaGetter.transform(() => ({ payload: mutablePayload })),
        }),
      ),
    );

    expect(identity.canonicalJson("input")).toStrictEqual({ payload: { venue: "xnys" } });
    expect(reads).toBe(0);
  });

  it("freezes optional ObjectKeyword observations before JSON encoding", () => {
    let descriptorReads = 0;
    const flappingOptionalPayload = new Proxy(
      {},
      {
        ownKeys: () => ["payload"],
        getOwnPropertyDescriptor: () => {
          descriptorReads += 1;
          return descriptorReads === 1
            ? undefined
            : {
                configurable: true,
                enumerable: true,
                value: new Map([["venue", "xnys"]]),
              };
        },
      },
    );
    const identity = makeSchemaJsonIdentity(
      Schema.String.pipe(
        Schema.encodeTo(Schema.Struct({ payload: Schema.optional(Schema.ObjectKeyword) }), {
          decode: SchemaGetter.transform(() => "decoded"),
          encode: SchemaGetter.transform(() => flappingOptionalPayload),
        }),
      ),
    );

    expect(identity.canonicalJson("input")).toStrictEqual({});
    expect(descriptorReads).toBe(1);
  });

  it("shares accessor scans across recursive union guards", () => {
    type RecursiveNode = {
      readonly child: RecursiveNode | null;
      readonly metadata: object;
    };
    let RecursiveNode: Schema.Codec<RecursiveNode, unknown, never, never>;
    RecursiveNode = Schema.suspend(() =>
      Schema.Struct({
        child: Schema.NullOr(RecursiveNode),
        metadata: Schema.ObjectKeyword,
      }),
    );

    let descriptorReads = 0;
    const wrap = (child: unknown): unknown => {
      const target = { child, metadata: { venue: "xnys" } };
      return new Proxy(target, {
        getOwnPropertyDescriptor: (object, key) => {
          descriptorReads += 1;
          return Reflect.getOwnPropertyDescriptor(object, key);
        },
      });
    };
    let value: unknown = null;
    for (let index = 0; index < 24; index += 1) {
      value = wrap(value);
    }

    const guard = makeStrictJsonSchemaGuard(Schema.toCodecJson(RecursiveNode).ast);
    expect(guard(value)).toStrictEqual(Result.succeed(undefined));
    expect(descriptorReads).toBeLessThanOrEqual(24 * 4);
  });

  it("bounds descriptor graph walks for union values", () => {
    const unionGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Union([Schema.String, Schema.Any])).ast,
    );
    const wideValue = Array.from({ length: 10_001 }, () => ({}));

    const result = unionGuard(wideValue);
    expect(
      Result.match(result, {
        onFailure: (error) => error.reason,
        onSuccess: () => "success",
      }),
    ).toBe("reflection-failure");
  });

  it("uses descriptor-safe snapshots for recursive collection unions", () => {
    type RichNode = {
      readonly child: null | ReadonlyArray<RichNode> | Option.Option<RichNode>;
      readonly payload: object;
      readonly extras: unknown;
    };
    let RichNode: Schema.Codec<RichNode, unknown, never, never>;
    RichNode = Schema.suspend(() =>
      Schema.Struct({
        child: Schema.Union([
          Schema.Null,
          Schema.Union([Schema.Array(RichNode), Schema.Option(RichNode)]),
        ]),
        payload: Schema.ObjectKeyword,
        extras: Schema.Any,
      }),
    );
    const shared = { label: "shared" };
    const value: RichNode = {
      child: [
        {
          child: null,
          payload: { venue: "xnys" },
          extras: { array: [1], map: new Map(), set: new Set(), fn: () => undefined, shared },
        },
      ],
      payload: { venue: "xnys" },
      extras: { shared },
    };

    const guard = makeStrictJsonSchemaGuard(Schema.toCodecJson(RichNode).ast);
    expect(guard(value)).toStrictEqual(Result.succeed(undefined));

    const hostilePrototype = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error("prototype failure");
        },
      },
    );
    expect(guard(hostilePrototype)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );
  });

  it("covers recursive union declaration and reflection boundaries", () => {
    let ArrayRecursive: Schema.Codec<unknown, unknown, never, never>;
    ArrayRecursive = Schema.suspend(() =>
      Schema.Union([Schema.Null, Schema.Array(ArrayRecursive)]),
    );
    const arrayGuard = makeStrictJsonSchemaGuard(Schema.toCodecJson(ArrayRecursive).ast);
    const shared: Array<unknown> = [null];
    const repeated = [shared, shared];
    Object.defineProperty(repeated, "map", { enumerable: true, value: new Map() });
    Object.defineProperty(repeated, "set", { enumerable: true, value: new Set() });
    Object.defineProperty(repeated, "fn", { enumerable: true, value: () => undefined });
    expect(arrayGuard(repeated)).toStrictEqual(Result.succeed(undefined));
    expect(arrayGuard(() => undefined)).toStrictEqual(Result.succeed(undefined));

    const wideArray = Array.from({ length: 10_001 }, () => []);
    const deepArrayResult = arrayGuard(wideArray);
    expect(
      Result.match(deepArrayResult, {
        onFailure: (error) => error.reason,
        onSuccess: () => "success",
      }),
    ).toBe("reflection-failure");

    const originalWeakMapHas = Object.getOwnPropertyDescriptor(WeakMap.prototype, "has")!;
    Object.defineProperty(WeakMap.prototype, "has", {
      configurable: true,
      value: () => false,
    });
    const missingSnapshotResult = arrayGuard([null]);
    Object.defineProperty(WeakMap.prototype, "has", {
      configurable: true,
      ...originalWeakMapHas,
    });
    expect(
      Result.match(missingSnapshotResult, {
        onFailure: (error) => error.reason,
        onSuccess: () => "success",
      }),
    ).toBe("reflection-failure");

    let TupleRecursive: Schema.Codec<unknown, unknown, never, never>;
    TupleRecursive = Schema.suspend(() =>
      Schema.Union([Schema.Null, Schema.Tuple([TupleRecursive])]),
    );
    const tupleGuard = makeStrictJsonSchemaGuard(Schema.toCodecJson(TupleRecursive).ast);
    expect(tupleGuard([[null]])).toStrictEqual(Result.succeed(undefined));
    const wrappedTupleGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Tuple([TupleRecursive])).ast,
    );
    expect(wrappedTupleGuard([[null]])).toStrictEqual(Result.succeed(undefined));

    const declarationUnionGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Option(Schema.Union([Schema.String, Schema.Number]))).ast,
    );
    expect(declarationUnionGuard(Option.some("value"))).toStrictEqual(Result.succeed(undefined));

    let optionRecursive: Schema.Codec<unknown, unknown, never, never>;
    optionRecursive = Schema.suspend(() =>
      Schema.Union([Schema.Null, Schema.Option(optionRecursive)]),
    );
    expect(makeStrictJsonSchemaGuard(Schema.toCodecJson(optionRecursive).ast)(null)).toStrictEqual(
      Result.succeed(undefined),
    );

    let nestedUnionRecursive: Schema.Codec<unknown, unknown, never, never>;
    nestedUnionRecursive = Schema.suspend(() =>
      Schema.Union([Schema.Null, Schema.Union([Schema.Null, nestedUnionRecursive])]),
    );
    expect(
      makeStrictJsonSchemaGuard(Schema.toCodecJson(nestedUnionRecursive).ast)(null),
    ).toStrictEqual(Result.succeed(undefined));

    expect(makeStrictJsonSchemaGuard(ThrowingClassDeclaration.ast)({})).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "unsupported-schema",
          message:
            "Cannot safely validate ObjectKeyword data inside an unknown schema declaration at $.",
        }),
      ),
    );

    const getPrototypeFailure = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error("prototype failure");
        },
      },
    );
    expect(arrayGuard(getPrototypeFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );

    const ownKeysFailure = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error("keys failure");
        },
      },
    );
    expect(arrayGuard(ownKeysFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );

    const descriptorFailure = new Proxy(
      {},
      {
        ownKeys: () => ["value"],
        getOwnPropertyDescriptor: () => {
          throw new Error("descriptor failure");
        },
      },
    );
    expect(arrayGuard(descriptorFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.value",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.value.",
        }),
      ),
    );

    const missingDescriptor = new Proxy(
      {},
      {
        ownKeys: () => ["value"],
        getOwnPropertyDescriptor: () => undefined,
      },
    );
    expect(arrayGuard(missingDescriptor)).toStrictEqual(Result.succeed(undefined));

    const accessorDescriptor = new Proxy(
      {},
      {
        ownKeys: () => ["value"],
        getOwnPropertyDescriptor: () => ({
          configurable: true,
          enumerable: true,
          get: () => null,
        }),
      },
    );
    expect(arrayGuard(accessorDescriptor)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.value",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.value.",
        }),
      ),
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

    const customHashSetNormalizer = makeSchemaJsonNormalizer(
      Schema.toCodecJson(CustomHashSetRepresentation).ast,
    );
    expect(customHashSetNormalizer(["b", "a"])).toStrictEqual(["b", "a"]);
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
    expect(
      makeStrictJsonSchemaCodec(Schema.Struct({ payload: Schema.ObjectKeyword })).strictEncoded({
        payload: new Map([["venue", "xnys"]]),
      }),
    ).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "unsupported-prototype",
          message: "Expected a plain data record or dense array at $.payload.",
        }),
      ),
    );
    const strictEncodedSuccess = makeStrictJsonSchemaCodec(
      Schema.Struct({ payload: Schema.ObjectKeyword }),
    ).strictEncoded({ payload: { venue: "xnys" } });
    expect(
      Result.match(strictEncodedSuccess, {
        onFailure: (error) => Result.fail(error),
        onSuccess: (validate) => validate({ payload: { venue: "xnys" } }),
      }),
    ).toStrictEqual(Result.succeed({ payload: { venue: "xnys" } }));

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

    expect(recordGuard({ value: null })).toStrictEqual(Result.succeed(undefined));
    const unmatchedRecordGuard = makeStrictJsonSchemaGuard(
      Schema.toCodecJson(Schema.Record(Schema.Number, Schema.ObjectKeyword)).ast,
    );
    expect(unmatchedRecordGuard({ other: {} })).toStrictEqual(Result.succeed(undefined));

    let cloneDescriptorReads = 0;
    const cloneDescriptorFlapping = new Proxy(
      {},
      {
        ownKeys: () => ["payload"],
        getOwnPropertyDescriptor: () => {
          cloneDescriptorReads += 1;
          return cloneDescriptorReads === 3
            ? undefined
            : { configurable: true, enumerable: true, value: { venue: "xnys" } };
        },
      },
    );
    const recordSnapshot = makeStrictJsonSchemaSnapshot(
      Schema.toCodecJson(Schema.Record(Schema.String, Schema.ObjectKeyword)).ast,
    );
    expect(recordSnapshot(cloneDescriptorFlapping)).toStrictEqual(
      Result.succeed({ payload: { venue: "xnys" } }),
    );
    const missingSnapshotDescriptor = new Proxy(
      {},
      {
        ownKeys: () => ["payload"],
        getOwnPropertyDescriptor: () => undefined,
      },
    );
    expect(recordSnapshot(missingSnapshotDescriptor)).toStrictEqual(Result.succeed({}));

    const keysFailure = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error("keys failure");
        },
      },
    );
    expect(recordGuard(keysFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );

    const defaultSnapshot = makeStrictJsonSchemaSnapshot(
      Schema.toCodecJson(Schema.Struct({ payload: Schema.ObjectKeyword })).ast,
    );
    const optionalSnapshot = makeStrictJsonSchemaSnapshot(
      Schema.toCodecJson(Schema.Struct({ optional: Schema.optional(Schema.String) })).ast,
    );
    expect(optionalSnapshot({})).toStrictEqual(Result.succeed({}));
    expect(defaultSnapshot({ payload: { venue: "xnys" } })).toStrictEqual(
      Result.succeed({ payload: { venue: "xnys" } }),
    );
    expect(defaultSnapshot({ payload: new Map([["venue", "xnys"]]) })).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "unsupported-prototype",
          message: "Expected a plain data record or dense array at $.payload.",
        }),
      ),
    );
    expect(defaultSnapshot(keysFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );
    const snapshotDescriptorFailure = new Proxy(
      { payload: { venue: "xnys" } },
      {
        getOwnPropertyDescriptor: () => {
          throw new Error("descriptor failure");
        },
      },
    );
    expect(defaultSnapshot(snapshotDescriptorFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.payload.",
        }),
      ),
    );
    const prototypeFailure = new Proxy(
      { payload: { venue: "xnys" } },
      {
        getPrototypeOf: () => {
          throw new Error("prototype failure");
        },
      },
    );
    expect(defaultSnapshot(prototypeFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.",
        }),
      ),
    );
    const hiddenSnapshotValue = {};
    Object.defineProperty(hiddenSnapshotValue, "payload", {
      configurable: true,
      enumerable: false,
      value: { venue: "xnys" },
    });
    expect(defaultSnapshot(hiddenSnapshotValue)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "non-enumerable-property",
          message: "Expected an enumerable data property at $.payload.",
        }),
      ),
    );
    const accessorSnapshotValue = {};
    Object.defineProperty(accessorSnapshotValue, "payload", {
      configurable: true,
      enumerable: true,
      get: () => ({ venue: "xnys" }),
    });
    expect(defaultSnapshot(accessorSnapshotValue)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.payload",
          reason: "accessor-property",
          message: "Accessor properties are not valid JSON data at $.payload.",
        }),
      ),
    );

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
    const unionSnapshot = makeStrictJsonSchemaSnapshot(
      Schema.toCodecJson(Schema.Union([Schema.String, Schema.Struct({ value: Schema.String })]))
        .ast,
    );
    expect(unionSnapshot(nonStrictFailure)).toStrictEqual(
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
    const lengthFailure = new Proxy([], {
      getOwnPropertyDescriptor: (_target, key) => {
        if (key === "length") {
          throw new Error("length read failure");
        }
        return undefined;
      },
    });
    expect(arrayGuard(lengthFailure)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.length",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.length.",
        }),
      ),
    );
    const invalidArrayLength = new Proxy([], {
      getOwnPropertyDescriptor: (target, key) =>
        key === "length"
          ? { configurable: false, enumerable: false, value: "bad", writable: true }
          : Reflect.getOwnPropertyDescriptor(target, key),
    });
    expect(arrayGuard(invalidArrayLength)).toStrictEqual(
      Result.fail(
        StrictJsonMaterializationError.make({
          path: "$.length",
          reason: "reflection-failure",
          message: "Could not inspect JSON value at $.length.",
        }),
      ),
    );
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
