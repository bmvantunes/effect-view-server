import { describe, expect, it } from "@effect/vitest";
import { Duration, Schema, SchemaGetter } from "effect";
import {
  defineViewServerConfig,
  kafka,
  viewServerSchemaFieldMetadata,
  viewServerUnsupportedRuntimeFieldDomain,
} from "./index";
import { isKafkaTopicSourceDefinition, makeKafkaSourceTopicsForConfig } from "./internal";

import { viewServer } from "../test-harness/live-query";
import { Order } from "../test-harness/schemas";

describe("Topic schema configuration", () => {
  it("derives schema field metadata for query validation", () => {
    expect(viewServerSchemaFieldMetadata(Schema.Number)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.BigInt)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: true,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(viewServerSchemaFieldMetadata(Schema.BigDecimal)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.BigInt])),
    ).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.Number])),
    ).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literal(1))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literals([1, 2]))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literal(1n))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.Number, Schema.Undefined])),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.Undefined])),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Undefined)).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Union([Schema.Undefined]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Union([]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(undefined)).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata("not-a-schema")).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata({ ast: "not-an-effect-ast" })).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literals(["open", "closed"]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: true,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(
      viewServerSchemaFieldMetadata(
        Schema.Union([
          Schema.Struct({ id: Schema.String }),
          Schema.Struct({ id: Schema.String, name: Schema.String }),
        ]),
      ),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: true,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Struct({ id: Schema.String }))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: true,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Array(Schema.String))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: false,
    });
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
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Option(Schema.String))).toBe(undefined);
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
    ).toBe(undefined);
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
    ).toBe(undefined);
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

  it("does not expose executable React or runtime placeholders from config", () => {
    expect(Object.keys(viewServer)).toStrictEqual(["topics", "defineRuntimeOptions"]);
    expect(makeKafkaSourceTopicsForConfig(viewServer)).toStrictEqual([]);
    expect(isKafkaTopicSourceDefinition({})).toBe(false);
    expect(
      isKafkaTopicSourceDefinition({
        topic: "orders",
        regions: [],
        value: kafka.json(() => Schema.toCodecJson(Order)),
        rowKey: () => "order-1",
        map: () => ({
          id: "order-1",
          customerId: "customer-1",
          status: "open",
          price: 1,
          region: "usa",
          updatedAt: 1,
        }),
      }),
    ).toBe(false);
    expect(
      isKafkaTopicSourceDefinition({
        topic: "orders",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Order)),
        map: () => ({
          id: "order-1",
          customerId: "customer-1",
          status: "open",
          price: 1,
          region: "usa",
          updatedAt: 1,
        }),
      }),
    ).toBe(false);
    expect(
      isKafkaTopicSourceDefinition({
        topic: "orders",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Order)),
        key: undefined,
        rowKey: () => "order-1",
        map: () => ({
          id: "order-1",
          customerId: "customer-1",
          status: "open",
          price: 1,
          region: "usa",
          updatedAt: 1,
        }),
      }),
    ).toBe(false);
    const extraKeySource = kafka.source({
      topic: "orders",
      regions: ["usa"],
      value: kafka.json(() => Schema.toCodecJson(Order)),
      rowKey: ({ key }) => key,
      map: () => ({
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 1,
        region: "usa",
        updatedAt: 1,
      }),
    });
    Object.defineProperty(extraKeySource, "extra", {
      value: true,
    });
    expect(isKafkaTopicSourceDefinition(extraKeySource)).toBe(false);

    const inheritedKeySource = Object.create({
      key: kafka.bytes(),
    });
    Object.assign(inheritedKeySource, {
      topic: "orders",
      regions: ["usa"],
      value: kafka.json(() => Schema.toCodecJson(Order)),
      rowKey: () => "order-1",
      map: () => ({
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 1,
        region: "usa",
        updatedAt: 1,
      }),
    });
    expect(isKafkaTopicSourceDefinition(inheritedKeySource)).toBe(false);
  });
});
