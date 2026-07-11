import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  createColumnLiveViewEngine,
  EngineClosedError,
  InvalidRowError,
  InvalidTopicError,
  type ColumnLiveViewEngineConfig,
} from "./index";
import { makeEngine, order, Order, viewServer } from "../test-harness/public-engine";
import type { Topics } from "../test-harness/public-engine";

describe("ColumnLiveViewEngine validation", () => {
  it.effect("fails invalid row publishes with a typed schema error", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const error = yield* Effect.flip(engine.publish("orders", order("1", "open", Number.NaN, 1)));

      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: expect.stringContaining("a finite number"),
      });
      expect(error).toBeInstanceOf(InvalidRowError);
    }),
  );

  it.effect("clones non-plain object row fields before exposing them", () =>
    Effect.gen(function* () {
      const WithPayload = Schema.Struct({
        id: Schema.String,
        payload: Schema.ObjectKeyword,
      });
      const engine = yield* createColumnLiveViewEngine({
        topics: {
          payloads: {
            schema: WithPayload,
            key: "id",
          },
        },
      });
      const payload = new Map([["venue", "xnys"]]);

      const emptyObjectKeywordFilter = yield* engine.snapshot("payloads", {
        select: ["id"],
        where: {
          payload: { venue: "xlon" },
        },
      });
      expect(emptyObjectKeywordFilter.rows).toStrictEqual([]);

      yield* engine.publish("payloads", { id: "1", payload });
      yield* engine.publish("payloads", { id: "2", payload: { venue: "xlon" } });

      const snapshot = yield* engine.snapshot("payloads", { select: ["id", "payload"] });
      expect(snapshot.rows[0]?.payload).toStrictEqual(payload);
      expect(snapshot.rows[0]?.payload).not.toBe(payload);

      const objectFilter = yield* engine.snapshot("payloads", {
        select: ["id", "payload"],
        where: {
          payload: { venue: "xlon" },
        },
      });
      expect(objectFilter.rows).toStrictEqual([{ id: "2", payload: { venue: "xlon" } }]);
    }),
  );

  it.effect("rejects non-cloneable object rows and query filters through typed errors", () =>
    Effect.gen(function* () {
      const WithPayload = Schema.Struct({
        id: Schema.String,
        payload: Schema.ObjectKeyword,
      });
      const engine = yield* createColumnLiveViewEngine({
        topics: {
          payloads: {
            schema: WithPayload,
            key: "id",
          },
        },
      });

      const rowError = yield* Effect.flip(
        engine.publish("payloads", { id: "1", payload: new WeakMap() }),
      );
      expect(rowError._tag).toBe("InvalidRowError");

      yield* engine.publish("payloads", { id: "2", payload: { venue: "xnys" } });
      const queryError = yield* Effect.flip(
        engine.snapshot("payloads", {
          select: ["id"],
          where: {
            payload: new WeakMap(),
          },
        }),
      );
      expect(queryError._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("keeps a runtime guard for unsafely cast invalid key configs", () =>
    Effect.gen(function* () {
      const invalidKeyConfig = {
        topics: {
          orders: {
            schema: Order,
            key: "missing",
          },
        },
      };
      // @ts-expect-error invalid configs can still reach runtime through untyped callers.
      const engine = yield* createColumnLiveViewEngine(invalidKeyConfig);

      const error = yield* Effect.flip(engine.publish("orders", order("1", "open", 10, 1)));

      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Key field missing"),
      });
    }),
  );

  it.effect("keeps runtime guards for non-struct topic schemas", () =>
    Effect.gen(function* () {
      const nonStructSchemaConfig = {
        topics: {
          loose: {
            schema: Schema.ObjectKeyword,
            key: "id",
          },
        },
      };
      // @ts-expect-error invalid configs can still reach runtime through untyped callers.
      const engine = yield* createColumnLiveViewEngine(nonStructSchemaConfig);
      const query: object = { select: ["id"] };

      const error = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("loose", query),
      );

      expect(error).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "loose",
        message: expect.stringContaining("select"),
      });
    }),
  );

  it.effect("fails missing-key patches and key-changing patches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const missing = yield* Effect.flip(engine.patch("orders", "missing", { price: 20 }));
      expect(missing).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Cannot patch missing key"),
      });

      yield* engine.publish("orders", order("1", "open", 10, 1));
      const nonPlainPatch = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send non-object patches.
        engine.patch("orders", "1", null),
      );
      expect(nonPlainPatch).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Patch must be a plain object"),
      });

      const symbolPatch = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send symbol patch fields.
        engine.patch("orders", "1", {
          [Symbol("bad")]: 20,
        }),
      );
      expect(symbolPatch).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Patch contains unknown field: Symbol(bad)"),
      });

      const changedKey = yield* Effect.flip(engine.patch("orders", "1", { id: "2" }));
      expect(changedKey).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("must not change"),
      });

      const beforeUnknownPatch = yield* engine.health();
      const unknownField = yield* Effect.flip(
        engine.patch("orders", "1", {
          // @ts-expect-error hostile runtime callers can still send unknown patch fields.
          prcie: 20,
        }),
      );
      const afterUnknownPatch = yield* engine.health();
      expect(unknownField).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Patch contains unknown field: prcie"),
      });
      expect(afterUnknownPatch.version).toBe(beforeUnknownPatch.version);
    }),
  );

  it.effect("keeps runtime guards for untyped grouped aggregate query callers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const groupedRuntimeQuery: object = {
        groupBy: ["missing"],
        aggregates: { count: { aggFunc: "count" } },
      };

      const error = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", groupedRuntimeQuery),
      );

      expect(error).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
      });
      expect(error.message).toContain("unknown field: missing");

      const subscribeError = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.subscribe("orders", groupedRuntimeQuery),
      );
      expect(subscribeError._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("does not throw defects for nullish malformed raw queries", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const nullError = yield* Effect.flip(
        // @ts-expect-error malformed untyped callers are still handled by the Effect error channel.
        engine.snapshot("orders", null),
      );
      expect(nullError).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("Raw query must be a plain object"),
      });

      const undefinedError = yield* Effect.flip(
        // @ts-expect-error malformed untyped callers are still handled by the Effect error channel.
        engine.subscribe("orders", null),
      );
      expect(undefinedError._tag).toBe("InvalidQueryError");

      const undefinedSnapshot = yield* Effect.flip(
        // @ts-expect-error undefined is rejected because raw queries must select columns.
        engine.snapshot("orders", undefined),
      );
      expect(undefinedSnapshot._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("fails malformed raw query shapes through the typed error channel", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const invalidWhereQuery: object = {
        select: ["id"],
        where: "bad",
      };
      const invalidWhere = yield* Effect.flip(
        // @ts-expect-error malformed runtime query where must be rejected.
        engine.snapshot("orders", invalidWhereQuery),
      );
      expect(invalidWhere).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
        message: expect.stringContaining("where"),
      });

      const invalidWhereArrayQuery: object = {
        select: ["id"],
        where: [],
      };
      const invalidWhereArray = yield* Effect.flip(
        // @ts-expect-error malformed runtime query where array must be rejected.
        engine.snapshot("orders", invalidWhereArrayQuery),
      );
      expect(invalidWhereArray._tag).toBe("InvalidQueryError");

      // @ts-expect-error runtime validation still rejects hostile untyped inputs.
      const invalidTopLevelArray = yield* Effect.flip(engine.snapshot("orders", []));
      expect(invalidTopLevelArray._tag).toBe("InvalidQueryError");

      // @ts-expect-error runtime validation still rejects hostile untyped inputs.
      const invalidTopLevelMap = yield* Effect.flip(engine.snapshot("orders", new Map()));
      expect(invalidTopLevelMap).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("plain object"),
      });

      const invalidWhereMapQuery: object = {
        select: ["id"],
        where: new Map([["status", "open"]]),
      };
      // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
      const invalidWhereMap = yield* Effect.flip(engine.snapshot("orders", invalidWhereMapQuery));
      expect(invalidWhereMap).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where"),
      });

      const unknownTopLevelRawQuery: object = {
        select: ["id"],
        where: {
          status: "open",
        },
        whre: {
          status: "closed",
        },
      };
      const unknownTopLevelKey = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", unknownTopLevelRawQuery),
      );
      expect(unknownTopLevelKey).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
        message: expect.stringContaining("unsupported key: whre"),
      });

      const invalidOrderByQuery: object = {
        select: ["id"],
        orderBy: "bad",
      };
      const invalidOrderBy = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy must be rejected.
        engine.snapshot("orders", invalidOrderByQuery),
      );
      expect(invalidOrderBy._tag).toBe("InvalidQueryError");

      const invalidFields = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query select must be rejected.
          select: "id",
        }),
      );
      expect(invalidFields._tag).toBe("InvalidQueryError");

      const invalidFieldEntryQuery: object = {
        select: [1],
      };
      const invalidFieldEntry = yield* Effect.flip(
        engine.snapshot(
          "orders",
          // @ts-expect-error malformed runtime query field entries must be rejected.
          invalidFieldEntryQuery,
        ),
      );
      expect(invalidFieldEntry._tag).toBe("InvalidQueryError");

      const emptySelectQuery: { readonly select: ReadonlyArray<unknown> } = {
        select: [],
      };
      const invalidEmptySelect = yield* Effect.flip(
        // @ts-expect-error hostile empty select is still handled by runtime guards.
        engine.snapshot("orders", emptySelectQuery),
      );
      expect(invalidEmptySelect._tag).toBe("InvalidQueryError");

      const invalidOffsetQuery: object = {
        select: ["id"],
        offset: "0",
      };
      const invalidOffset = yield* Effect.flip(
        // @ts-expect-error malformed runtime query offset must be rejected.
        engine.snapshot("orders", invalidOffsetQuery),
      );
      expect(invalidOffset).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetNaN = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          offset: Number.NaN,
        }),
      );
      expect(invalidOffsetNaN).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetNegative = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          offset: -1,
        }),
      );
      expect(invalidOffsetNegative).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetFraction = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          offset: 0.5,
        }),
      );
      expect(invalidOffsetFraction).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidLimitQuery: object = {
        select: ["id"],
        limit: "1",
      };
      const invalidLimit = yield* Effect.flip(
        // @ts-expect-error malformed runtime query limit must be rejected.
        engine.snapshot("orders", invalidLimitQuery),
      );
      expect(invalidLimit).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("limit"),
      });

      const invalidLimitInfinity = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          limit: Number.POSITIVE_INFINITY,
        }),
      );
      expect(invalidLimitInfinity).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("limit"),
      });

      const invalidOrderByEntryQuery: object = {
        select: ["id"],
        orderBy: ["bad"],
      };
      const invalidOrderByEntry = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy entry must be rejected.
        engine.snapshot("orders", invalidOrderByEntryQuery),
      );
      expect(invalidOrderByEntry._tag).toBe("InvalidQueryError");

      const invalidOrderByExtraKeyQuery: object = {
        select: ["id"],
        orderBy: [
          {
            field: "price",
            direction: "asc",
            typo: true,
          },
        ],
      };
      const invalidOrderByExtraKey = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", invalidOrderByExtraKeyQuery),
      );
      expect(invalidOrderByExtraKey).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported key: typo"),
      });

      const invalidOrderByFieldQuery: object = {
        select: ["id"],
        orderBy: [
          {
            direction: "asc",
          },
        ],
      };
      const invalidOrderByField = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy field must be rejected.
        engine.snapshot("orders", invalidOrderByFieldQuery),
      );
      expect(invalidOrderByField._tag).toBe("InvalidQueryError");

      const unknownOrderByFieldQuery: object = {
        select: ["id"],
        orderBy: [
          {
            field: "prcie",
            direction: "asc",
          },
        ],
      };
      const unknownOrderByField = yield* Effect.flip(
        // @ts-expect-error runtime query unknown orderBy fields must be rejected.
        engine.snapshot("orders", unknownOrderByFieldQuery),
      );
      expect(unknownOrderByField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("orderBy"),
      });

      const unknownProjectionFieldQuery: object = {
        select: ["prcie"],
      };
      const unknownProjectionField = yield* Effect.flip(
        engine.snapshot(
          "orders",
          // @ts-expect-error runtime query unknown projected fields must be rejected.
          unknownProjectionFieldQuery,
        ),
      );
      expect(unknownProjectionField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("select"),
      });

      const unknownWhereFieldQuery: object = {
        select: ["id"],
        where: {
          prcie: 10,
        },
      };
      const unknownWhereField = yield* Effect.flip(
        // @ts-expect-error runtime query unknown where fields must be rejected.
        engine.snapshot("orders", unknownWhereFieldQuery),
      );
      expect(unknownWhereField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where"),
      });

      const unknownFilterOperatorQuery: object = {
        select: ["id"],
        where: {
          status: { equals: "open" },
        },
      };
      const unknownFilterOperator = yield* Effect.flip(
        // @ts-expect-error runtime query unknown filter operators must be rejected.
        engine.snapshot("orders", unknownFilterOperatorQuery),
      );
      expect(unknownFilterOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const mixedKnownAndUnknownFilterOperator = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          where: {
            // @ts-expect-error runtime query unknown filter operators must be rejected.
            status: { eq: "open", typo: true },
          },
        }),
      );
      expect(mixedKnownAndUnknownFilterOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const invalidOrderByDirectionQuery: object = {
        select: ["id"],
        orderBy: [
          {
            field: "price",
            direction: "sideways",
          },
        ],
      };
      const invalidOrderByDirection = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy direction must be rejected.
        engine.snapshot("orders", invalidOrderByDirectionQuery),
      );
      expect(invalidOrderByDirection._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("fails unknown topics and closes the engine idempotently", () =>
    Effect.gen(function* () {
      const looseConfig: ColumnLiveViewEngineConfig<Topics> = { topics: viewServer.topics };
      const engine = yield* createColumnLiveViewEngine(looseConfig);
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      const missingTopicConfig: ColumnLiveViewEngineConfig<Record<string, Topics["orders"]>> = {
        topics: {
          orders: viewServer.topics.orders,
        },
      };
      const looseEngine = yield* createColumnLiveViewEngine(missingTopicConfig);
      const missing = yield* Effect.flip(looseEngine.snapshot("missing", { select: ["id"] }));
      expect(missing._tag).toBe("InvalidTopicError");
      expect(missing).toBeInstanceOf(InvalidTopicError);

      yield* engine.close();
      yield* engine.close();
      yield* subscription.close();

      const closedHealth = yield* engine.health();
      expect(closedHealth.status).toBe("stopping");

      const closedError = yield* Effect.flip(engine.publish("orders", order("1", "open", 10, 1)));
      expect(closedError._tag).toBe("EngineClosedError");
      expect(closedError).toBeInstanceOf(EngineClosedError);
    }),
  );
});
