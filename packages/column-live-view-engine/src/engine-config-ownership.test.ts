import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  viewSchema,
  viewServerUnsupportedRuntimeFieldDomain,
} from "@effect-view-server/config";
import { viewServerRowSchemaFieldsMatchAst } from "@effect-view-server/config/internal";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine, InvalidRowError } from "./index";

const UnregisteredString = Schema.declare((value): value is string => typeof value === "string");

const makeStoreConstructionProbe = () => {
  let astReads = 0;
  const observedField = new Proxy(Schema.String, {
    get(target, property) {
      if (property === "ast") {
        astReads += 1;
      }
      return Reflect.get(target, property, target);
    },
  });
  const schema = Schema.Struct({
    id: Schema.String,
    observed: observedField,
  });
  astReads = 0;
  Schema.isSchema(observedField);
  viewServerUnsupportedRuntimeFieldDomain(observedField);
  viewServerRowSchemaFieldsMatchAst(schema);
  const validationAstReads = astReads;
  astReads = 0;
  return {
    astReads: () => astReads,
    schema,
    validationAstReads,
  };
};

class OwnedOrder extends Schema.Class<OwnedOrder>("OwnedOrder")({
  id: Schema.String,
  status: Schema.String,
}) {}
viewSchema.admitClass(OwnedOrder);

describe("ColumnLiveViewEngine config ownership", () => {
  it.effect("constructs from an owned config snapshot", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Schema.Struct({ id: Schema.String, status: Schema.String }),
            key: "id",
          },
        },
      });

      const engine = yield* createColumnLiveViewEngine({ topics: config.topics });
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "status"],
      });

      expect(snapshot).toStrictEqual({
        rows: [],
        status: "ready",
        statusCode: "Ready",
        totalRows: 0,
        version: 0,
      });
      yield* engine.close();
    }),
  );

  it.effect("rejects a raw structural config with an unregistered declaration", () =>
    Effect.gen(function* () {
      const schema = Schema.Struct({
        id: Schema.String,
        value: UnregisteredString,
      });

      const error = yield* Effect.flip(
        createColumnLiveViewEngine({
          topics: {
            unsafe: {
              schema,
              key: "id",
            },
          },
        }),
      );

      expect(error).toBeInstanceOf(InvalidRowError);
      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "unsafe",
        message:
          "Topic field value uses unsupported runtime domain: custom codec transformation without canonical identity witness",
      });
    }),
  );

  it.effect("rejects malformed exposed fields during construction", () =>
    Effect.gen(function* () {
      const schema = Schema.Struct({
        id: Schema.String,
        value: Schema.String,
      });
      expect(Reflect.set(schema.fields, "value", "not-an-effect-schema")).toBe(true);

      const error = yield* Effect.flip(
        createColumnLiveViewEngine({
          topics: {
            malformed: {
              schema,
              key: "id",
            },
          },
        }),
      );

      expect(error).toBeInstanceOf(InvalidRowError);
      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "malformed",
        message: "Topic field value must be an Effect Schema.",
      });
    }),
  );

  it.effect("rejects safe exposed fields that diverge from the row schema AST", () =>
    Effect.gen(function* () {
      const schema = Schema.Struct({
        id: Schema.String,
        value: Schema.String,
      });
      expect(Reflect.set(schema.fields, "value", Schema.Number)).toBe(true);

      const error = yield* Effect.flip(
        createColumnLiveViewEngine({
          topics: {
            divergent: {
              schema,
              key: "id",
            },
          },
        }),
      );

      expect(error).toBeInstanceOf(InvalidRowError);
      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "divergent",
        message: "Topic exposed row fields do not match the row schema AST.",
      });
    }),
  );

  it.effect("turns hostile topic registry inspection into a typed construction error", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Schema.Struct({ id: Schema.String }),
            key: "id",
          },
        },
      });
      const hostileTopics = new Proxy(config.topics, {
        ownKeys() {
          throw new Error("topic keys unavailable");
        },
      });

      const error = yield* Effect.flip(createColumnLiveViewEngine({ topics: hostileTopics }));

      expect(error).toBeInstanceOf(InvalidRowError);
      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "<engine-config>",
        message: "Topic schemas could not be safely inspected during engine construction.",
      });
    }),
  );

  it.effect("validates every root schema before constructing any TopicStore", () =>
    Effect.gen(function* () {
      const probe = makeStoreConstructionProbe();
      const rootUnsafeSchema = Schema.Struct({
        id: Schema.String,
        value: UnregisteredString,
      });
      expect(Reflect.set(rootUnsafeSchema.fields, "value", Schema.String)).toBe(true);

      const error = yield* Effect.flip(
        createColumnLiveViewEngine({
          topics: {
            observed: {
              schema: probe.schema,
              key: "id",
            },
            unsafe: {
              schema: rootUnsafeSchema,
              key: "id",
            },
          },
        }),
      );

      expect(error).toBeInstanceOf(InvalidRowError);
      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "unsafe",
        message:
          "Topic row schema uses unsupported runtime domain: custom codec transformation without canonical identity witness",
      });
      expect(probe.astReads()).toBe(probe.validationAstReads);
    }),
  );

  it.effect("validates every exposed field before constructing any TopicStore", () =>
    Effect.gen(function* () {
      const probe = makeStoreConstructionProbe();
      const fieldUnsafeSchema = Schema.Struct({
        id: Schema.String,
        value: Schema.String,
      });
      expect(Reflect.set(fieldUnsafeSchema.fields, "value", UnregisteredString)).toBe(true);

      const error = yield* Effect.flip(
        createColumnLiveViewEngine({
          topics: {
            observed: {
              schema: probe.schema,
              key: "id",
            },
            unsafe: {
              schema: fieldUnsafeSchema,
              key: "id",
            },
          },
        }),
      );

      expect(error).toBeInstanceOf(InvalidRowError);
      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "unsafe",
        message:
          "Topic field value uses unsupported runtime domain: custom codec transformation without canonical identity witness",
      });
      expect(probe.astReads()).toBe(probe.validationAstReads);
    }),
  );

  it.effect("keeps an owned root Class proxy constructable", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: OwnedOrder,
            key: "id",
          },
        },
      });
      const row = new config.topics.orders.schema({ id: "1", status: "open" });

      expect(row).toBeInstanceOf(OwnedOrder);

      const engine = yield* createColumnLiveViewEngine({ topics: config.topics });
      yield* engine.publish("orders", row);
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "status"],
      });

      expect(snapshot.rows).toStrictEqual([{ id: "1", status: "open" }]);
      yield* engine.close();
    }),
  );
});
