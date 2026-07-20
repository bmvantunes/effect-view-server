import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  createColumnLiveViewEngine,
  EngineClosedError,
  InvalidQueryError,
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

  it.effect("rejects non-JSON object row fields and preserves plain records", () =>
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
      const mapError = yield* Effect.flip(
        engine.publish("payloads", {
          id: "1",
          payload: new Map([["venue", "xnys"]]),
        }),
      );
      expect(mapError).toBeInstanceOf(InvalidRowError);
      expect(mapError).toStrictEqual(
        InvalidRowError.make({
          topic: "payloads",
          message:
            "StrictJsonMaterializationError: Expected a plain data record or dense array at $.payload.",
        }),
      );

      yield* engine.publish("payloads", { id: "2", payload: { venue: "xlon" } });

      const snapshot = yield* engine.snapshot("payloads", { select: ["id", "payload"] });
      expect(snapshot).toStrictEqual({
        rows: [{ id: "2", payload: { venue: "xlon" } }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
    }),
  );

  it.effect("rejects non-cloneable object rows", () =>
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

  it.effect("rejects non-struct topic schemas during construction", () =>
    Effect.gen(function* () {
      const nonStructSchemaConfig = {
        topics: {
          loose: {
            schema: Schema.ObjectKeyword,
            key: "id",
          },
        },
      };
      const error = yield* Effect.flip(
        // @ts-expect-error invalid configs can still reach runtime through untyped callers.
        createColumnLiveViewEngine(nonStructSchemaConfig),
      );

      expect(error).toBeInstanceOf(InvalidRowError);
      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "loose",
        message: "Topic row schema must be an Effect Schema Struct.",
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

  it.effect("rejects accessor and non-enumerable patch fields without reading caller values", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      let accessorReads = 0;
      let descriptorReads = 0;
      const accessorTarget = {};
      Object.defineProperty(accessorTarget, "price", {
        enumerable: true,
        get: () => {
          accessorReads += 1;
          return 20;
        },
      });
      const accessorPatch = new Proxy(accessorTarget, {
        getOwnPropertyDescriptor(target, property) {
          if (property === "price") {
            descriptorReads += 1;
          }
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      });
      const nonEnumerablePatch = {};
      Object.defineProperty(nonEnumerablePatch, "price", {
        enumerable: false,
        value: 20,
      });
      const hostilePrototypePatch = new Proxy(
        {},
        {
          getPrototypeOf() {
            throw new Error("prototype unavailable");
          },
        },
      );
      const hostileKeysPatch = new Proxy(
        {},
        {
          ownKeys() {
            throw new Error("keys unavailable");
          },
        },
      );
      const hostileDescriptorPatch = new Proxy(
        { price: 20 },
        {
          getOwnPropertyDescriptor() {
            throw new Error("descriptor unavailable");
          },
        },
      );

      const accessorError = yield* Effect.flip(engine.patch("orders", "1", accessorPatch));
      const nonEnumerableError = yield* Effect.flip(
        engine.patch("orders", "1", nonEnumerablePatch),
      );
      const prototypeError = yield* Effect.flip(engine.patch("orders", "1", hostilePrototypePatch));
      const keysError = yield* Effect.flip(engine.patch("orders", "1", hostileKeysPatch));
      const descriptorError = yield* Effect.flip(
        engine.patch("orders", "1", hostileDescriptorPatch),
      );
      const snapshot = yield* engine.snapshot("orders", {
        select: ["id", "price"],
      });

      expect(accessorError).toBeInstanceOf(InvalidRowError);
      expect(accessorError).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: expect.stringContaining("Patch field must be a data property: price"),
      });
      expect(nonEnumerableError).toBeInstanceOf(InvalidRowError);
      expect(nonEnumerableError).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: expect.stringContaining("Patch field must be enumerable: price"),
      });
      expect(prototypeError).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: "Could not inspect patch object.",
      });
      expect(keysError).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: "Could not inspect patch fields.",
      });
      expect(descriptorError).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: "Could not inspect patch field: price.",
      });
      expect(accessorReads).toBe(0);
      expect(descriptorReads).toBe(1);
      expect(snapshot).toStrictEqual({
        rows: [{ id: "1", price: 10 }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
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
      expect(nullError).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Query input could not be snapshotted.",
        }),
      );

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

  it.effect("captures raw and grouped one-shot query inputs when snapshot is called", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 20, 2, "amer"),
        order("3", "closed", 30, 3, "amer"),
      ]);

      let arrayPropertyReads = 0;
      const selectTarget: ["id", "price"] = ["id", "price"];
      const select = new Proxy(selectTarget, {
        get: () => {
          arrayPropertyReads += 1;
          throw new Error("select property access must not run");
        },
      });
      const whereTarget: Array<{
        field: "status";
        type: "equals";
        filter: "open" | "closed";
      }> = [{ field: "status", type: "equals", filter: "open" }];
      const where = new Proxy(whereTarget, {
        get: () => {
          arrayPropertyReads += 1;
          throw new Error("where property access must not run");
        },
      });
      const rawOrderByTarget: Array<{
        field: "price";
        direction: "asc" | "desc";
      }> = [{ field: "price", direction: "asc" }];
      const rawOrderBy = new Proxy(rawOrderByTarget, {
        get: () => {
          arrayPropertyReads += 1;
          throw new Error("raw orderBy property access must not run");
        },
      });
      const rawQuery = { select, where, orderBy: rawOrderBy, offset: 0, limit: 1 };
      const rawSnapshot = engine.snapshot("orders", rawQuery);

      const groupByTarget: ["status" | "region"] = ["status"];
      const groupBy = new Proxy(groupByTarget, {
        get: () => {
          arrayPropertyReads += 1;
          throw new Error("groupBy property access must not run");
        },
      });
      const groupedOrderByTarget: Array<{
        aggregate: "rowCount";
        direction: "asc" | "desc";
      }> = [{ aggregate: "rowCount", direction: "desc" }];
      const groupedOrderBy = new Proxy(groupedOrderByTarget, {
        get: () => {
          arrayPropertyReads += 1;
          throw new Error("grouped orderBy property access must not run");
        },
      });
      const groupedAggregates: {
        rowCount: { aggFunc: "count" };
      } = { rowCount: { aggFunc: "count" } };
      const groupedQuery = {
        groupBy,
        aggregates: groupedAggregates,
        orderBy: groupedOrderBy,
        offset: 0,
        limit: 1,
      };
      const groupedSnapshot = engine.snapshot("orders", groupedQuery);

      whereTarget[0]!.filter = "closed";
      rawOrderByTarget[0]!.direction = "desc";
      rawQuery.offset = 1;
      rawQuery.limit = 2;
      groupByTarget[0] = "region";
      groupedOrderByTarget[0]!.direction = "asc";
      groupedQuery.offset = 1;
      groupedQuery.limit = 2;

      expect(yield* rawSnapshot).toStrictEqual({
        rows: [{ id: "1", price: 10 }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 2,
        version: 1,
      });
      expect(yield* groupedSnapshot).toStrictEqual({
        rows: [{ status: "open", rowCount: 2n }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 2,
        version: 1,
      });
      expect(arrayPropertyReads).toBe(0);
    }),
  );

  it.effect("maps hostile one-shot query ownership into typed query errors", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      let accessorReads = 0;
      const accessorQuery = {};
      Object.defineProperty(accessorQuery, "select", {
        enumerable: true,
        get: () => {
          accessorReads += 1;
          throw new Error("query accessor must not run");
        },
      });
      const accessorSelect: Array<unknown> = [];
      Object.defineProperty(accessorSelect, "0", {
        enumerable: true,
        get: () => {
          accessorReads += 1;
          throw new Error("select accessor must not run");
        },
      });
      accessorSelect.length = 1;
      const revokedQuery = Proxy.revocable({ select: ["id"] }, {});
      revokedQuery.revoke();

      const rootAccessorError = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", accessorQuery),
      );
      const arrayAccessorError = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", { select: accessorSelect }),
      );
      const revokedError = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", revokedQuery.proxy),
      );

      for (const error of [rootAccessorError, arrayAccessorError, revokedError]) {
        expect(error).toStrictEqual(
          InvalidQueryError.make({
            message: "Query input could not be snapshotted.",
            topic: "orders",
          }),
        );
      }
      expect(accessorReads).toBe(0);
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

      const emptyWhere = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [],
      });
      expect(emptyWhere.rows).toStrictEqual([{ id: "1" }]);

      // @ts-expect-error runtime validation still rejects hostile untyped inputs.
      const invalidTopLevelArray = yield* Effect.flip(engine.snapshot("orders", []));
      expect(invalidTopLevelArray._tag).toBe("InvalidQueryError");

      // @ts-expect-error runtime validation still rejects hostile untyped inputs.
      const invalidTopLevelMap = yield* Effect.flip(engine.snapshot("orders", new Map()));
      expect(invalidTopLevelMap).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Query input could not be snapshotted.",
        }),
      );

      const invalidWhereMapQuery: object = {
        select: ["id"],
        where: new Map([["status", "open"]]),
      };
      // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
      const invalidWhereMap = yield* Effect.flip(engine.snapshot("orders", invalidWhereMapQuery));
      expect(invalidWhereMap).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Query input could not be snapshotted.",
        }),
      );

      const unknownTopLevelRawQuery: object = {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
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

      const decoratedSelect = ["id"];
      Object.defineProperty(decoratedSelect, "metadata", { enumerable: true, value: true });
      const decoratedSelectQuery: object = { select: decoratedSelect };
      const decoratedSelectError = yield* Effect.flip(
        // @ts-expect-error decorated query arrays are rejected by the runtime boundary.
        engine.snapshot("orders", decoratedSelectQuery),
      );
      expect(decoratedSelectError).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Raw query select must be a non-empty array of strings.",
        }),
      );

      const decoratedOrderBy = [{ field: "price", direction: "asc" }];
      Object.defineProperty(decoratedOrderBy, "metadata", { enumerable: true, value: true });
      const decoratedOrderByQuery: object = { select: ["id"], orderBy: decoratedOrderBy };
      const decoratedOrderByError = yield* Effect.flip(
        // @ts-expect-error decorated query arrays are rejected by the runtime boundary.
        engine.snapshot("orders", decoratedOrderByQuery),
      );
      expect(decoratedOrderByError).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Raw query orderBy must be a dense array without extra properties.",
        }),
      );

      const invalidFields = yield* Effect.flip(
        // @ts-expect-error malformed runtime query select must be rejected.
        engine.snapshot("orders", {
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
      expect(invalidOffsetNaN).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Query input could not be snapshotted.",
        }),
      );

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
      expect(invalidLimitInfinity).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Query input could not be snapshotted.",
        }),
      );

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
        where: [{ field: "prcie", type: "equals", filter: 10 }],
      };
      const unknownWhereField = yield* Effect.flip(
        // @ts-expect-error runtime query unknown where fields must be rejected.
        engine.snapshot("orders", unknownWhereFieldQuery),
      );
      expect(unknownWhereField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unknown or non-filterable field"),
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
