import { describe, expect, it } from "@effect/vitest";
import { Schema, SchemaAST } from "effect";
import { defineViewServerConfig, kafka, viewSchema, viewServerSchemaFieldMetadata } from "./index";
import { snapshotViewServerTopics, viewServerRowSchemaFieldsMatchAst } from "./config-ownership";
import { isKafkaTopicSourceDefinition, makeKafkaSourceTopicsForConfig } from "./internal";

import { viewServer } from "../test-harness/live-query";
import { Order, StructuredProfile } from "../test-harness/schemas";

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
    expect(viewServerSchemaFieldMetadata(StructuredProfile)).toStrictEqual({
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
  });

  it("snapshots caller-owned config state while preserving Class construction", () => {
    class OwnedProfile extends Schema.Class<OwnedProfile>("OwnedProfile")({
      id: Schema.String,
      code: Schema.String,
    }) {}
    viewSchema.admitClass(OwnedProfile);

    const topic: {
      schema: typeof OwnedProfile;
      key: "id" | "code";
    } = {
      schema: OwnedProfile,
      key: "id",
    };
    const topics = { profiles: topic };
    const input = { topics };
    const originalAst = OwnedProfile.ast;
    const originalFields = OwnedProfile.fields;
    const config = defineViewServerConfig(input);
    const made = config.topics.profiles.schema.make({ id: "made", code: "alpha" });
    const constructed = new config.topics.profiles.schema({
      id: "constructed",
      code: "beta",
    });

    expect(made).toBeInstanceOf(OwnedProfile);
    expect(constructed).toBeInstanceOf(OwnedProfile);
    expect(Object.isFrozen(config)).toBe(true);
    expect(Object.isFrozen(config.topics)).toBe(true);
    expect(Object.isFrozen(config.topics.profiles)).toBe(true);
    expect(Object.isFrozen(config.topics.profiles.schema.fields)).toBe(true);
    expect(Object.isFrozen(input)).toBe(false);
    expect(Object.isFrozen(topics)).toBe(false);
    expect(Object.isFrozen(topic)).toBe(false);
    expect(Object.isFrozen(OwnedProfile)).toBe(false);
    expect(Object.isFrozen(originalFields)).toBe(false);

    const regions = ["usa"];
    const routeBy = ["id"];
    const sourceTopics = {
      profiles: {
        schema: OwnedProfile,
        key: "id",
        kafkaSource: { regions },
        grpcSource: { routeBy },
      },
    };
    const sourceSnapshot = snapshotViewServerTopics(sourceTopics);

    expect(Object.isFrozen(sourceSnapshot.profiles.kafkaSource)).toBe(true);
    expect(Object.isFrozen(sourceSnapshot.profiles.kafkaSource.regions)).toBe(true);
    expect(Object.isFrozen(sourceSnapshot.profiles.grpcSource)).toBe(true);
    expect(Object.isFrozen(sourceSnapshot.profiles.grpcSource.routeBy)).toBe(true);
    expect(Object.isFrozen(sourceTopics.profiles.kafkaSource)).toBe(false);
    expect(Object.isFrozen(sourceTopics.profiles.grpcSource)).toBe(false);
    expect(Object.isFrozen(regions)).toBe(false);
    expect(Object.isFrozen(routeBy)).toBe(false);

    topic.key = "code";
    Object.defineProperty(topics, "secondary", {
      configurable: true,
      enumerable: true,
      value: { schema: OwnedProfile, key: "id" },
    });
    Object.defineProperty(OwnedProfile, "ast", {
      configurable: true,
      value: Schema.Struct({ id: Schema.String, code: Schema.Number }).ast,
    });
    Object.defineProperty(originalFields, "code", {
      configurable: true,
      enumerable: true,
      value: Schema.Number,
      writable: true,
    });
    regions.push("london");
    routeBy.push("code");

    expect(config.topics.profiles.key).toBe("id");
    expect(Object.keys(config.topics)).toStrictEqual(["profiles"]);
    expect(config.topics.profiles.schema.ast).toBe(originalAst);
    expect(config.topics.profiles.schema.fields.code).toBe(Schema.String);
    expect(sourceSnapshot.profiles.kafkaSource.regions).toStrictEqual(["usa"]);
    expect(sourceSnapshot.profiles.grpcSource.routeBy).toStrictEqual(["id"]);
    expect(Reflect.set(config.topics.profiles.schema, "extra", true)).toBe(false);
    expect(Reflect.defineProperty(config.topics.profiles.schema, "extra", { value: true })).toBe(
      false,
    );
    expect(Reflect.deleteProperty(config.topics.profiles.schema, "fields")).toBe(false);
    expect(Reflect.setPrototypeOf(config.topics.profiles.schema, OwnedProfile)).toBe(false);
    expect(Reflect.preventExtensions(config.topics.profiles.schema)).toBe(false);
    expect(config.topics.profiles.schema.make({ id: "isolated", code: "gamma" })).toBeInstanceOf(
      OwnedProfile,
    );
  });

  it("defensively rejects malformed row field and AST relationships", () => {
    const plainAst = Schema.String.ast;
    const indexedAst = Schema.Record(Schema.String, Schema.String).ast;
    const symbolAst = new SchemaAST.Objects(
      [new SchemaAST.PropertySignature(Symbol.for("field"), Schema.String.ast)],
      [],
    );
    const duplicateProperties = [new SchemaAST.PropertySignature("id", Schema.String.ast)];
    const duplicateAst = new SchemaAST.Objects(duplicateProperties, []);
    duplicateProperties.push(new SchemaAST.PropertySignature("id", Schema.String.ast));
    class Profile extends Schema.Class<Profile>("MalformedProfile")({
      id: Schema.String,
    }) {}
    viewSchema.admitClass(Profile);
    const missingTypeParameter = new Proxy(Profile.ast, {
      get(target, property) {
        return property === "typeParameters" ? [undefined] : Reflect.get(target, property, target);
      },
    });
    const missingField = Schema.Struct({ id: Schema.String });
    expect(Reflect.deleteProperty(missingField.fields, "id")).toBe(true);

    expect([
      // @ts-expect-error hostile callers can supply non-Schema row metadata.
      viewServerRowSchemaFieldsMatchAst({ ast: plainAst, fields: {} }),
      // @ts-expect-error hostile callers can supply indexed row metadata.
      viewServerRowSchemaFieldsMatchAst({ ast: indexedAst, fields: {} }),
      // @ts-expect-error hostile callers can supply symbol field metadata.
      viewServerRowSchemaFieldsMatchAst({ ast: symbolAst, fields: {} }),
      // @ts-expect-error hostile callers can supply duplicate AST fields.
      viewServerRowSchemaFieldsMatchAst({ ast: duplicateAst, fields: { id: Schema.String } }),
      // @ts-expect-error hostile callers can supply malformed declarations.
      viewServerRowSchemaFieldsMatchAst({ ast: missingTypeParameter, fields: {} }),
      viewServerRowSchemaFieldsMatchAst(missingField),
    ]).toStrictEqual([false, false, false, false, false, false]);
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
