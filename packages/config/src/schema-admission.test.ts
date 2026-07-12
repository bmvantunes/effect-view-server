import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Equivalence, Schema, SchemaGetter } from "effect";
import {
  defineViewServerConfig,
  viewSchema,
  viewServerUnsupportedRuntimeFieldDomain,
} from "./index";

import { StructuredProfile } from "../test-harness/schemas";

describe("Topic schema admission", () => {
  it("classifies supported and unsupported runtime value domains", () => {
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Date)).toBe("Date");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeUtc)).toBe("DateTimeUtc");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeUtcFromString)).toBe(
      "DateTimeUtc",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeZoned)).toBe("DateTimeZoned");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeZonedFromString)).toBe(
      "DateTimeZoned",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Duration)).toBe("Duration");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Error())).toBe("Error");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Error({ includeStack: true }))).toBe(
      "ErrorWithStack",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Error({ excludeCause: true }))).toBe(
      "ErrorWithoutCause",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Error({ includeStack: true, excludeCause: true }),
      ),
    ).toBe("ErrorWithStackWithoutCause");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Symbol)).toBe("Symbol");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZone)).toBe("TimeZone");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneFromString)).toBe("TimeZone");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneNamed)).toBe("TimeZoneNamed");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneNamedFromString)).toBe(
      "TimeZoneNamed",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneOffset)).toBe("TimeZoneOffset");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Trim)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8Array)).toBe("Uint8Array");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8ArrayFromBase64)).toBe("Uint8Array");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8ArrayFromBase64Url)).toBe(
      "Uint8Array",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8ArrayFromHex)).toBe("Uint8Array");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.suspend(() => Schema.Date))).toBe("Date");
    const recursiveSuspend: Schema.Schema<string> = Schema.suspend(() => recursiveSuspend);
    expect(viewServerUnsupportedRuntimeFieldDomain(recursiveSuspend)).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([recursiveSuspend, Schema.String])),
    ).toBe("ambiguous JSON codec union");
    type RecursiveNode = {
      readonly child: RecursiveNode | null;
    };
    const RecursiveNode: Schema.Codec<RecursiveNode, unknown, never, never> = Schema.suspend(() =>
      Schema.Struct({ child: Schema.NullOr(RecursiveNode) }),
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(RecursiveNode)).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.decodeTo(Schema.Duration, {
            decode: SchemaGetter.transform(() => Duration.millis(1)),
            encode: SchemaGetter.transform(() => "1"),
          }),
        ),
      ),
    ).toBe("Duration");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.encodeTo(Schema.Duration, {
            decode: SchemaGetter.transform(() => "1"),
            encode: SchemaGetter.transform(() => Duration.millis(1)),
          }),
        ),
      ),
    ).toBe("Duration");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Array(Schema.RegExp))).toBe("RegExp");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Tuple([Schema.RegExp]))).toBe("RegExp");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Date]),
      ),
    ).toBe("Date");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Record(Schema.Symbol, Schema.String)),
    ).toBe("Symbol");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Record(Schema.String, Schema.String)),
    ).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Record(Schema.String, Schema.URL))).toBe(
      "URL",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.BigInt, Schema.Number])),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.BigDecimal, Schema.Number])),
    ).toBe("mixed numeric domain: bigDecimal, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Literal(1), Schema.Literal(2n)]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.String, Schema.File])),
    ).toBe("File");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.decodeTo(Schema.Union([Schema.Number, Schema.BigInt]), {
            decode: SchemaGetter.transform(() => 1n),
            encode: SchemaGetter.transform(() => "1"),
          }),
        ),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.encodeTo(Schema.Union([Schema.Number, Schema.BigInt]), {
            decode: SchemaGetter.transform(() => "1"),
            encode: SchemaGetter.transform(() => 1n),
          }),
        ),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Struct({
          nested: Schema.Struct({
            href: Schema.URL,
          }),
        }),
      ),
    ).toBe("URL");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Option(Schema.Date))).toBe("Date");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Option(Schema.Union([Schema.Number, Schema.BigInt])),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.Option(Schema.String))).toBe(
      undefined,
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Redacted(Schema.Duration))).toBe(
      "Duration",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Struct({
          nested: Schema.Struct({
            amount: Schema.Union([Schema.Number, Schema.BigInt]),
          }),
        }),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Record(Schema.String, Schema.Union([Schema.Number, Schema.BigInt])),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Tuple([Schema.Union([Schema.Number, Schema.BigInt])]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.TupleWithRest(Schema.Tuple([Schema.String]), [
          Schema.Union([Schema.Number, Schema.BigInt]),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.String,
          Schema.Struct({
            amount: Schema.Union([Schema.Number, Schema.BigInt]),
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Array(Schema.Number), Schema.Array(Schema.BigInt)]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.Number]), Schema.Tuple([Schema.BigInt])]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Array(Schema.Number), Schema.Tuple([Schema.BigInt])]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Struct({
            amount: Schema.BigInt,
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            payload: Schema.Array(Schema.Number),
          }),
          Schema.Struct({
            payload: Schema.Tuple([Schema.BigInt]),
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Record(Schema.String, Schema.Number),
          Schema.Struct({
            amount: Schema.BigInt,
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Record(Schema.String, Schema.BigInt),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.Number]), Schema.Array(Schema.BigInt)]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Number]),
          Schema.Tuple([Schema.BigInt]),
        ]),
      ),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Number]),
          Schema.Tuple([Schema.String, Schema.BigInt]),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Number, Schema.Array(Schema.BigInt)]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.Number]), Schema.Record(Schema.String, Schema.BigInt)]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Tuple([Schema.BigInt]),
        ]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Array(Schema.BigInt),
        ]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            price: Schema.Number,
          }),
          Schema.Struct({
            quantity: Schema.BigInt,
          }),
        ]),
      ),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Struct({
          quantity: Schema.BigInt,
          price: Schema.Number,
        }),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.String]),
      ),
    ).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Struct({ id: Schema.String }))).toBe(
      undefined,
    );
    expect(viewServerUnsupportedRuntimeFieldDomain({})).toBe(undefined);
  });

  it("validates the complete row schema, not only its fields", () => {
    const Row = Schema.Struct({
      id: Schema.String,
      status: Schema.String,
    });
    const EquivalentRow = Row.pipe(
      Schema.overrideToEquivalence(() =>
        Equivalence.make((left, right) => left.id === right.id && left.status === right.status),
      ),
    );
    const LossyRow = Object.assign(
      Row.pipe(
        Schema.decodeTo(Row, {
          decode: SchemaGetter.transform((value) => value),
          encode: SchemaGetter.transform((value) => ({ ...value, status: "same" })),
        }),
      ),
      { fields: Row.fields },
    );

    expect(viewServerUnsupportedRuntimeFieldDomain(EquivalentRow)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(LossyRow)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(() =>
      defineViewServerConfig({
        topics: {
          equivalent: {
            schema: EquivalentRow,
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic equivalent row schema uses unsupported runtime domain: custom equivalence without canonical identity witness",
    );
    expect(() =>
      defineViewServerConfig({
        topics: {
          lossy: {
            schema: LossyRow,
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic lossy row schema uses unsupported runtime domain: custom codec transformation without canonical identity witness",
    );
  });

  it("rejects safe exposed fields that diverge from the row schema AST", () => {
    const DivergentRow = Schema.Struct({
      id: Schema.String,
      value: Schema.String,
    });
    expect(Reflect.set(DivergentRow.fields, "value", Schema.Number)).toBe(true);

    expect(() =>
      defineViewServerConfig({
        topics: {
          divergent: {
            schema: DivergentRow,
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic divergent exposed row fields do not match the row schema AST.");
  });

  it("admits passthrough optional defaults and rejects omit or lossy optional encodings", () => {
    const PassthroughDefault = Schema.String.pipe(
      Schema.withDecodingDefaultKey(Effect.succeed("open")),
    );
    const OmittedDefault = Schema.String.pipe(
      Schema.withDecodingDefaultKey(Effect.succeed("open"), { encodingStrategy: "omit" }),
    );
    const LossyOptional = Schema.optionalKey(Schema.String).pipe(
      Schema.decodeTo(Schema.String, {
        decode: SchemaGetter.withDefault(Effect.succeed("open")),
        encode: SchemaGetter.omit(),
      }),
    );

    expect(viewServerUnsupportedRuntimeFieldDomain(PassthroughDefault)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(OmittedDefault)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(LossyOptional)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(() =>
      defineViewServerConfig({
        topics: {
          defaults: {
            schema: Schema.Struct({
              id: Schema.String,
              passthrough: PassthroughDefault,
            }),
            key: "id",
          },
        },
      }),
    ).not.toThrow();
    expect(() =>
      defineViewServerConfig({
        topics: {
          omitted: {
            schema: Schema.Struct({ id: Schema.String, value: OmittedDefault }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic omitted field value uses unsupported runtime domain: custom codec transformation without canonical identity witness",
    );
  });

  it("rejects malformed row and field schemas from untyped callers", () => {
    const InvalidFieldRow = Schema.Struct({
      id: Schema.String,
      value: Schema.String,
    });
    Object.defineProperty(InvalidFieldRow.fields, "value", {
      configurable: true,
      enumerable: true,
      value: {},
      writable: true,
    });

    expect(() =>
      defineViewServerConfig({
        topics: {
          invalidRow: {
            // @ts-expect-error Runtime admission protects untyped callers that do not provide a Struct.
            schema: {},
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic invalidRow row schema must be an Effect Schema Struct.");
    expect(() =>
      defineViewServerConfig({
        topics: {
          invalidField: {
            schema: InvalidFieldRow,
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic invalidField field value must be an Effect Schema.");
  });

  it("rejects canonical JSON codecs that cannot preserve semantic identity", () => {
    // @ts-expect-error Runtime admission also protects untyped JavaScript callers.
    const UndefinedLiteral = Schema.Literal(undefined);

    expect(viewServerUnsupportedRuntimeFieldDomain(UndefinedLiteral)).toBe(
      "non-JSON literal: undefined",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.Null, Schema.Undefined])),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.String, Schema.BigInt])),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Struct({
          nested: Schema.Union([Schema.String, Schema.BigInt]),
        }),
      ),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        viewSchema.Chunk(Schema.Union([Schema.Null, Schema.Undefined])),
      ),
    ).toBe("ambiguous JSON codec union");

    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.String, Schema.Undefined])),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Literal("alpha"), Schema.Literal("beta")]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.Boolean, Schema.Number])),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.Literal(1), Schema.Number])),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.Literal(true), Schema.Boolean])),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.TemplateLiteral(["order-", Schema.String]), Schema.Number]),
      ),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.TemplateLiteral(["order-", Schema.String]), Schema.Finite]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.ObjectKeyword, Schema.String])),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.Unknown, Schema.String])),
    ).toBe("ambiguous JSON codec union");

    const Status = Schema.Enum({ Alpha: "alpha", Beta: "beta" });
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.Literal("gamma"), Status])),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.Literal("alpha"), Status])),
    ).toBe("ambiguous JSON codec union");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Status, Schema.String]))).toBe(
      "ambiguous JSON codec union",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.String, Status]))).toBe(
      "ambiguous JSON codec union",
    );

    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.String]), Schema.Tuple([Schema.String, Schema.String])]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.String]), Schema.Tuple([Schema.Number])]),
      ),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.String]), Schema.Tuple([Schema.Finite])]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.String]), Schema.Tuple([Schema.Literal("alpha")])]),
      ),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.String]), Schema.Array(Schema.String)]),
      ),
    ).toBe("ambiguous JSON codec union");

    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([viewSchema.Option(Schema.String), Schema.String]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.String, viewSchema.Option(Schema.String)]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.TaggedStruct("Alpha", {
            value: Schema.String,
          }),
          Schema.TaggedStruct("Beta", {
            value: Schema.String,
          }),
        ]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            kind: Schema.optionalKey(Schema.Literal("Alpha")),
            value: Schema.String,
          }),
          Schema.Struct({
            kind: Schema.Literal("Beta"),
            value: Schema.String,
          }),
        ]),
      ),
    ).toBe("ambiguous JSON codec union");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            kind: Schema.Literal("Alpha"),
            value: Schema.String,
          }),
          Schema.Struct({
            kind: Schema.optionalKey(Schema.Literal("Beta")),
            value: Schema.String,
          }),
        ]),
      ),
    ).toBe("ambiguous JSON codec union");

    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.ReadonlyMap(Schema.String, Schema.String)),
    ).toBe("ReadonlyMap");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.ReadonlySet(Schema.String))).toBe(
      "ReadonlySet",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(viewSchema.HashMap(Schema.String, Schema.String)),
    ).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.HashSet(Schema.String))).toBe(
      undefined,
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.Option(Schema.String))).toBe(
      undefined,
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.Chunk(Schema.String))).toBe(
      undefined,
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.BigDecimal)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(StructuredProfile)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain({ ast: Schema.String.ast })).toBe(undefined);

    expect(() =>
      defineViewServerConfig({
        topics: {
          ambiguous: {
            schema: Schema.Struct({
              id: Schema.String,
              value: Schema.Union([Schema.Null, Schema.Undefined]),
            }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic ambiguous field value uses unsupported runtime domain: ambiguous JSON codec union",
    );
  });

  it("rejects custom equivalence without a canonical identity witness", () => {
    const CaseInsensitiveString = Schema.String.pipe(
      Schema.overrideToEquivalence(() =>
        Equivalence.make((left, right) => left.toLowerCase() === right.toLowerCase()),
      ),
    );
    const EquivalentScalarUnion = Schema.Union([Schema.String, Schema.Undefined]).pipe(
      Schema.overrideToEquivalence(() => Equivalence.make((left, right) => left === right)),
    );
    const EquivalentSuspendedString = Schema.suspend(() => Schema.String).pipe(
      Schema.overrideToEquivalence(() => Equivalence.make((left, right) => left === right)),
    );
    const EquivalentBigDecimal = Schema.BigDecimal.pipe(
      Schema.overrideToEquivalence(() => Equivalence.make((left, right) => left === right)),
    );
    const EquivalentStruct = Schema.Struct({ value: Schema.String }).pipe(
      Schema.overrideToEquivalence(() => Equivalence.make((left, right) => left === right)),
    );
    const EquivalentClass = StructuredProfile.pipe(
      Schema.overrideToEquivalence(() => Equivalence.make((left, right) => left === right)),
    );

    expect(viewServerUnsupportedRuntimeFieldDomain(CaseInsensitiveString)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(EquivalentScalarUnion)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(EquivalentSuspendedString)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(EquivalentBigDecimal)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(EquivalentStruct)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(EquivalentClass)).toBe(
      "custom equivalence without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.BigDecimal)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(StructuredProfile)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.Option(Schema.String))).toBe(
      undefined,
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.Chunk(Schema.String))).toBe(
      undefined,
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(viewSchema.HashMap(Schema.String, Schema.String)),
    ).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(viewSchema.HashSet(Schema.String))).toBe(
      undefined,
    );

    expect(() =>
      defineViewServerConfig({
        topics: {
          customEquivalence: {
            schema: Schema.Struct({
              id: Schema.String,
              label: CaseInsensitiveString,
            }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic customEquivalence field label uses unsupported runtime domain: custom equivalence without canonical identity witness",
    );
  });

  it("rejects unrecognized or surrogate-colliding codec transformations", () => {
    class OpaqueValue {}
    const Lossy = Schema.String.pipe(
      Schema.decodeTo(Schema.String, {
        decode: SchemaGetter.transform((value) => value),
        encode: SchemaGetter.transform(() => "same"),
      }),
    );

    expect(viewServerUnsupportedRuntimeFieldDomain(Lossy)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.StringFromBase64)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.StringFromBase64Url)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.StringFromHex)).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(Schema.withDecodingDefaultKey(Effect.succeed("open"))),
      ),
    ).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.instanceOf(OpaqueValue))).toBe(
      "custom codec transformation without canonical identity witness",
    );
    expect(() =>
      defineViewServerConfig({
        topics: {
          lossy: {
            schema: Schema.Struct({ id: Schema.String, value: Lossy }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic lossy field value uses unsupported runtime domain: custom codec transformation without canonical identity witness",
    );
  });

  it("rejects topic fields with unsupported runtime value domains", () => {
    expect(() =>
      defineViewServerConfig({
        topics: {
          dated: {
            schema: Schema.Struct({
              id: Schema.String,
              createdAt: Schema.Date,
            }),
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic dated field createdAt uses unsupported runtime domain: Date");
    expect(() =>
      defineViewServerConfig({
        topics: {
          nested: {
            schema: Schema.Struct({
              id: Schema.String,
              metadata: Schema.Struct({
                latency: Schema.Duration,
              }),
            }),
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic nested field metadata uses unsupported runtime domain: Duration");
    expect(() =>
      defineViewServerConfig({
        topics: {
          mixedNumeric: {
            schema: Schema.Struct({
              id: Schema.String,
              amount: Schema.Union([Schema.BigInt, Schema.Number]),
            }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic mixedNumeric field amount uses unsupported runtime domain: mixed numeric domain: bigint, number",
    );
    expect(() =>
      defineViewServerConfig({
        topics: {
          nestedMixedNumeric: {
            schema: Schema.Struct({
              id: Schema.String,
              payload: Schema.Struct({
                amount: Schema.Union([Schema.BigInt, Schema.Number]),
              }),
            }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic nestedMixedNumeric field payload uses unsupported runtime domain: mixed numeric domain: bigint, number",
    );
  });
});
