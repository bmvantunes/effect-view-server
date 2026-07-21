import { fromStringUnsafe } from "effect/BigDecimal";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import {
  acquireRawQueryExecution,
  activeQueryTestInterface,
  activeQueryTestMetadata,
  activeStoreRawQueryExecutionCount,
  createActiveQueryRegistry,
  preparedGroupedPlanCompilationCount,
  preparedRawPlanCompilationCount,
  releaseRawQueryExecution,
} from "../test-harness/active-query-interface";
import type { RawQueryExecutionReleaseToken } from "./active-query";
import { prepareRuntimeRawQuery } from "./raw-query-compiler";
import {
  acquireTopicStoreRuntimeGroupedQueryExecution,
  acquireTopicStoreRuntimeRawQueryExecution,
  publishTopicStoreRow,
  releaseTopicStoreMaterializedQueryExecutionToken,
  releaseTopicStoreRawQueryExecution,
  TopicStore,
} from "./topic-store";
import { defaultGroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";

const invalidRow = (_topic: string, message: string): Error => new Error(message);
describe("column-live-view-engine Active Query sharing", () => {
  it.effect(
    "compiles one canonical grouped plan for equivalent large deep filters",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "shared-large-grouped-filter",
          Schema.Struct({
            id: Schema.String,
            customerId: Schema.String,
            status: Schema.String,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(
          store,
          { id: "order-1", customerId: "customer-1", status: "open" },
          invalidRow,
        );

        const candidates = Array.from({ length: 50_000 }, (_value, index) => `customer-${index}`);
        let nestedWhere: unknown = {
          field: "customerId",
          type: "in",
          filter: candidates,
        };
        for (let depth = 0; depth < 256; depth += 1) {
          nestedWhere =
            depth % 2 === 0
              ? {
                  type: "AND",
                  conditions: [nestedWhere, { field: "status", type: "equals", filter: "open" }],
                }
              : {
                  type: "OR",
                  conditions: [
                    nestedWhere,
                    {
                      field: "customerId",
                      type: "equals",
                      filter: `missing-${depth}`,
                    },
                  ],
                };
        }
        const query = {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [nestedWhere],
        };
        const releaseTokens: Array<string> = [];
        for (let subscriber = 0; subscriber < 12; subscriber += 1) {
          const acquired = yield* acquireTopicStoreRuntimeGroupedQueryExecution(
            store,
            query,
            defaultGroupedIncrementalAdmissionLimits,
          );
          releaseTokens.push(acquired.releaseToken);
          expect(acquired.execution.initial(`grouped-subscriber-${subscriber}`).totalRows).toBe(1);
        }

        const queryInterface = activeQueryTestInterface(store);
        expect(yield* preparedGroupedPlanCompilationCount(queryInterface)).toBe(1);
        expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(1);
        const canonicalEntry = queryInterface.activeQueries.materialized.values().next().value;
        expect(canonicalEntry?.canonicalCompiled.plan.cacheKey).toBe(releaseTokens[0]);
        expect(canonicalEntry?.refs).toBe(12);

        yield* publishTopicStoreRow(
          store,
          { id: "order-2", customerId: "customer-2", status: "open" },
          invalidRow,
        );
        expect(queryInterface.changesSince(1)).toBeDefined();

        for (const token of releaseTokens) {
          yield* releaseTopicStoreMaterializedQueryExecutionToken(store, token);
        }
        expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(0);
        expect(yield* preparedGroupedPlanCompilationCount(queryInterface)).toBe(1);

        yield* publishTopicStoreRow(
          store,
          { id: "order-3", customerId: "customer-3", status: "closed" },
          invalidRow,
        );
        expect(queryInterface.changesSince(2)).toBeUndefined();
      }),
    30_000,
  );

  it.effect(
    "compiles one canonical 50k membership plan for equivalent runtime subscribers",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "shared-large-membership",
          Schema.Struct({
            id: Schema.String,
            customerId: Schema.String,
            region: Schema.String,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(
          store,
          { id: "order-1", customerId: "customer-1", region: "emea" },
          invalidRow,
        );

        const candidates = Array.from({ length: 50_000 }, (_value, index) => `customer-${index}`);
        const releaseTokens: Array<RawQueryExecutionReleaseToken> = [];
        for (let subscriber = 0; subscriber < 12; subscriber += 1) {
          const acquired = yield* acquireTopicStoreRuntimeRawQueryExecution(store, {
            select: subscriber % 2 === 0 ? ["id"] : ["id", "region"],
            where: [
              {
                field: "customerId",
                type: "in",
                filter: candidates,
              },
            ],
            offset: subscriber % 3,
            limit: 1,
          });
          releaseTokens.push(acquired.releaseToken);
          expect(acquired.execution.initial(`subscriber-${subscriber}`).totalRows).toBe(1);
        }

        const queryInterface = activeQueryTestInterface(store);
        expect(yield* preparedRawPlanCompilationCount(queryInterface)).toBe(1);
        expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(1);
        const canonicalEntry = queryInterface.activeQueries.raw.values().next().value;
        expect(canonicalEntry?.canonicalPlan.predicate.plan.filters).toHaveLength(1);
        expect(canonicalEntry?.refs).toBe(12);

        for (const token of releaseTokens) {
          yield* releaseTopicStoreRawQueryExecution(store, token);
        }
        expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(0);
        expect(yield* preparedRawPlanCompilationCount(queryInterface)).toBe(1);
      }),
    30_000,
  );

  it.effect("reuses execution state for identical compiled raw queries", () =>
    Effect.gen(function* () {
      const rowSchema = Schema.Struct({
        id: Schema.String,
        status: Schema.String,
        score: Schema.Number,
        count: Schema.BigInt,
        value: Schema.BigDecimal,
      });
      const store = new TopicStore("scores", rowSchema, "id", () => {});

      yield* publishTopicStoreRow(
        store,
        {
          id: "1",
          status: "open",
          score: 10,
          count: 1n,
          value: fromStringUnsafe("1.00"),
        },
        invalidRow,
      );
      yield* publishTopicStoreRow(
        store,
        {
          id: "2",
          status: "closed",
          score: 20,
          count: 2n,
          value: fromStringUnsafe("2.00"),
        },
        invalidRow,
      );

      const compiled = yield* prepareRuntimeRawQuery("scores", activeQueryTestMetadata(store), {
        select: ["id", "score", "count", "value"],
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [
          {
            field: "score",
            direction: "desc",
          },
        ],
      });

      const firstExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        compiled,
      );
      const secondExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        compiled,
      );
      expect(Object.isFrozen(firstExecution)).toBe(true);
      expect(() => Object.assign(firstExecution, secondExecution)).toThrowError(TypeError);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);

      const firstCursor = firstExecution.createCursor();
      const secondCursor = secondExecution.createCursor();

      const initialFirst = firstExecution.initial("query-a");
      const initialSecond = secondExecution.initial("query-b");
      expect(initialFirst.rows).toStrictEqual(initialSecond.rows);
      expect(initialFirst.totalRows).toBe(1);
      expect(initialFirst.keys).toStrictEqual(["1"]);

      const beforePublishFirst = yield* firstExecution.next("query-a", firstCursor);
      const beforePublishSecond = yield* secondExecution.next("query-b", secondCursor);
      expect(beforePublishFirst._tag).toBe("None");
      expect(beforePublishSecond._tag).toBe("None");

      yield* publishTopicStoreRow(
        store,
        {
          id: "3",
          status: "open",
          score: 5,
          count: 3n,
          value: fromStringUnsafe("3.00"),
        },
        invalidRow,
      );

      const afterPublishFirst = yield* firstExecution.next("query-a", firstCursor);
      const afterPublishSecond = yield* secondExecution.next("query-b", secondCursor);
      expect(afterPublishFirst._tag).toBe("Some");
      expect(afterPublishSecond._tag).toBe("Some");

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), compiled);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);
      const afterRefcountDecrement = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        compiled,
      );
      expect(afterRefcountDecrement.initial("query-c").totalRows).toBe(2);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), compiled);
      yield* releaseRawQueryExecution(activeQueryTestInterface(store), compiled);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(0);

      const afterRefcountExhausted = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        compiled,
      );
      expect(afterRefcountExhausted.initial("query-d").totalRows).toBe(2);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);
      yield* releaseRawQueryExecution(activeQueryTestInterface(store), compiled);
    }),
  );

  it.effect("keeps execution caches local to the active query registry", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "registry-isolation",
        Schema.Struct({
          id: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", score: 1 }, invalidRow);

      const compiled = yield* prepareRuntimeRawQuery(
        "registry-isolation",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );
      const queryInterface = activeQueryTestInterface(store);
      const isolatedQueryInterface = {
        ...queryInterface,
        activeQueries: createActiveQueryRegistry(),
      };

      yield* acquireRawQueryExecution(queryInterface, compiled);
      yield* acquireRawQueryExecution(isolatedQueryInterface, compiled);

      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(1);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedQueryInterface)).toBe(1);

      yield* releaseRawQueryExecution(queryInterface, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(0);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedQueryInterface)).toBe(1);

      yield* releaseRawQueryExecution(isolatedQueryInterface, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedQueryInterface)).toBe(0);
    }),
  );

  it.effect("shares base evaluation across different projections", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "projection-sharing",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);

      const idOnly = yield* prepareRuntimeRawQuery(
        "projection-sharing",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );
      const idAndScore = yield* prepareRuntimeRawQuery(
        "projection-sharing",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );

      const idOnlyExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        idOnly,
      );
      const idAndScoreExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        idAndScore,
      );

      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);
      expect(idOnlyExecution.initial("ids").rows).toStrictEqual([{ id: "b" }, { id: "a" }]);
      expect(idAndScoreExecution.initial("scores").rows).toStrictEqual([
        { id: "b", score: 2 },
        { id: "a", score: 1 },
      ]);

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), idOnly);
      yield* releaseRawQueryExecution(activeQueryTestInterface(store), idAndScore);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(0);
    }),
  );

  it.effect("shares base evaluation across different windows", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-sharing",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const firstWindow = yield* prepareRuntimeRawQuery(
        "window-sharing",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 0,
          limit: 1,
        },
      );
      const secondWindow = yield* prepareRuntimeRawQuery(
        "window-sharing",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 1,
          limit: 1,
        },
      );

      const firstExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        firstWindow,
      );
      expect(firstExecution.initial("first").rows).toStrictEqual([{ id: "c", score: 3 }]);

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), secondWindow);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);
      expect(firstExecution.initial("first-after-unknown-release").rows).toStrictEqual([
        { id: "c", score: 3 },
      ]);

      const secondExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        secondWindow,
      );
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);
      expect(secondExecution.initial("second").rows).toStrictEqual([{ id: "b" }]);

      const firstCursor = firstExecution.createCursor();
      const secondCursor = secondExecution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);

      const firstDelta = yield* firstExecution.next("first", firstCursor);
      const secondDelta = yield* secondExecution.next("second", secondCursor);
      expect(Option.getOrThrow(firstDelta)).toStrictEqual({
        type: "delta",
        topic: "window-sharing",
        queryId: "first",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "c",
          },
          {
            type: "insert",
            key: "d",
            row: {
              id: "d",
              score: 4,
            },
            index: 0,
          },
        ],
        totalRows: 4,
      });
      expect(Option.getOrThrow(secondDelta)).toStrictEqual({
        type: "delta",
        topic: "window-sharing",
        queryId: "second",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "b",
          },
          {
            type: "insert",
            key: "c",
            row: {
              id: "c",
            },
            index: 0,
          },
        ],
        totalRows: 4,
      });

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), firstWindow);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);
      expect(secondExecution.initial("second-after-release").rows).toStrictEqual([{ id: "c" }]);

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), secondWindow);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(0);

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), secondWindow);
    }),
  );

  it.effect("shrinks shared base windows immediately when larger windows release", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-shrink",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const wideWindow = yield* prepareRuntimeRawQuery(
        "window-shrink",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 0,
          limit: 3,
        },
      );
      const narrowWindow = yield* prepareRuntimeRawQuery(
        "window-shrink",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 1,
          limit: 1,
        },
      );

      const wideExecution = yield* acquireRawQueryExecution(observedQueryInterface, wideWindow);
      expect(wideExecution.initial("wide").keys).toStrictEqual(["d", "c", "b"]);

      const narrowExecution = yield* acquireRawQueryExecution(observedQueryInterface, narrowWindow);
      expect(narrowExecution.initial("narrow").keys).toStrictEqual(["c"]);

      yield* releaseRawQueryExecution(observedQueryInterface, wideWindow);
      expect(scanLimits).toStrictEqual([4, 3]);
      expect(narrowExecution.initial("narrow-after-shrink").keys).toStrictEqual(["c"]);

      yield* releaseRawQueryExecution(observedQueryInterface, narrowWindow);
    }),
  );

  it.effect("compacts unbounded shared base windows when unbounded windows release", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-unbounded",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const boundedWindow = yield* prepareRuntimeRawQuery(
        "window-unbounded",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 1,
          limit: 1,
        },
      );
      const unboundedWindow = yield* prepareRuntimeRawQuery(
        "window-unbounded",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );

      const boundedExecution = yield* acquireRawQueryExecution(
        observedQueryInterface,
        boundedWindow,
      );
      expect(boundedExecution.initial("bounded").keys).toStrictEqual(["b"]);

      const unboundedExecution = yield* acquireRawQueryExecution(
        observedQueryInterface,
        unboundedWindow,
      );
      expect(unboundedExecution.initial("unbounded").keys).toStrictEqual(["c", "b", "a"]);

      yield* releaseRawQueryExecution(observedQueryInterface, unboundedWindow);
      expect(scanLimits).toStrictEqual([3, undefined, 3]);
      expect(boundedExecution.initial("bounded-after-compact").keys).toStrictEqual(["b"]);

      yield* releaseRawQueryExecution(observedQueryInterface, boundedWindow);
    }),
  );

  it.effect("does not expand shared base rows for zero-limit windows", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-zero-limit",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const zeroWindow = yield* prepareRuntimeRawQuery(
        "window-zero-limit",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 10,
          limit: 0,
        },
      );

      const zeroExecution = yield* acquireRawQueryExecution(observedQueryInterface, zeroWindow);
      const initial = zeroExecution.initial("zero");
      expect(scanLimits).toStrictEqual([0]);
      expect(initial.rows).toStrictEqual([]);
      expect(initial.keys).toStrictEqual([]);
      expect(initial.totalRows).toBe(2);

      yield* releaseRawQueryExecution(observedQueryInterface, zeroWindow);
    }),
  );

  it.effect("covers query cache key paths for numeric, bigint, and BigDecimal filters", () =>
    Effect.gen(function* () {
      const numericStore = new TopicStore(
        "numbers",
        Schema.Struct({ id: Schema.String, score: Schema.Number }),
        "id",
        () => {},
      );
      const bigintStore = new TopicStore(
        "bigints",
        Schema.Struct({ id: Schema.String, amount: Schema.BigInt }),
        "id",
        () => {},
      );
      const decimalStore = new TopicStore(
        "decimals",
        Schema.Struct({ id: Schema.String, price: Schema.BigDecimal }),
        "id",
        () => {},
      );

      yield* publishTopicStoreRow(numericStore, { id: "a", score: 10 }, invalidRow);
      yield* publishTopicStoreRow(bigintStore, { id: "b", amount: 5n }, invalidRow);
      yield* publishTopicStoreRow(
        decimalStore,
        { id: "c", price: fromStringUnsafe("1.23") },
        invalidRow,
      );

      const infFilter = yield* Effect.flip(
        prepareRuntimeRawQuery("numbers", activeQueryTestMetadata(numericStore), {
          select: ["id", "score"],
          where: [{ field: "score", type: "equals", filter: Number.POSITIVE_INFINITY }],
        }),
      );
      const nanFilter = yield* Effect.flip(
        prepareRuntimeRawQuery("numbers", activeQueryTestMetadata(numericStore), {
          select: ["id", "score"],
          where: [{ field: "score", type: "equals", filter: Number.NaN }],
        }),
      );
      const zeroFilter = yield* prepareRuntimeRawQuery(
        "numbers",
        activeQueryTestMetadata(numericStore),
        {
          select: ["id", "score"],
          where: [{ field: "score", type: "equals", filter: 10 }],
        },
      );
      const positiveZeroFilter = yield* prepareRuntimeRawQuery(
        "numbers",
        activeQueryTestMetadata(numericStore),
        {
          select: ["id", "score"],
          where: [{ field: "score", type: "equals", filter: 0 }],
        },
      );
      const negativeZeroFilter = yield* prepareRuntimeRawQuery(
        "numbers",
        activeQueryTestMetadata(numericStore),
        {
          select: ["id", "score"],
          where: [{ field: "score", type: "equals", filter: -0 }],
        },
      );
      const offsetFilter = yield* prepareRuntimeRawQuery(
        "numbers",
        activeQueryTestMetadata(numericStore),
        {
          select: ["id", "score"],
          offset: 1,
        },
      );
      const bigIntFilter = yield* prepareRuntimeRawQuery(
        "bigints",
        activeQueryTestMetadata(bigintStore),
        {
          select: ["id", "amount"],
          where: [{ field: "amount", type: "equals", filter: 3n }],
        },
      );
      const decimalFilter = yield* prepareRuntimeRawQuery(
        "decimals",
        activeQueryTestMetadata(decimalStore),
        {
          select: ["id", "price"],
          where: [{ field: "price", type: "equals", filter: fromStringUnsafe("1.23") }],
        },
      );

      const zeroExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(numericStore),
        zeroFilter,
      );
      yield* acquireRawQueryExecution(activeQueryTestInterface(numericStore), positiveZeroFilter);
      yield* acquireRawQueryExecution(activeQueryTestInterface(numericStore), negativeZeroFilter);
      const offsetExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(numericStore),
        offsetFilter,
      );
      const bigintExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(bigintStore),
        bigIntFilter,
      );
      const decimalExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(decimalStore),
        decimalFilter,
      );

      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(numericStore))).toBe(
        3,
      );
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(bigintStore))).toBe(
        1,
      );
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(decimalStore))).toBe(
        1,
      );

      const zeroCursor = zeroExecution.createCursor();
      const offsetCursor = offsetExecution.createCursor();
      const bigintCursor = bigintExecution.createCursor();
      const decimalCursor = decimalExecution.createCursor();

      expect(infFilter.message).toBe("Filter numbers must be finite.");
      expect(nanFilter.message).toBe("Filter numbers must be finite.");
      expect(zeroExecution.initial("q").totalRows).toBe(1);
      expect(offsetExecution.initial("q").totalRows).toBe(1);
      expect(bigintExecution.initial("q").totalRows).toBe(0);
      expect(decimalExecution.initial("q").totalRows).toBe(1);

      expect((yield* zeroExecution.next("q", zeroCursor))._tag).toBe("None");
      expect((yield* offsetExecution.next("q", offsetCursor))._tag).toBe("None");
      expect((yield* bigintExecution.next("q", bigintCursor))._tag).toBe("None");
      expect((yield* decimalExecution.next("q", decimalCursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(activeQueryTestInterface(numericStore), zeroFilter);
      yield* releaseRawQueryExecution(activeQueryTestInterface(numericStore), positiveZeroFilter);
      yield* releaseRawQueryExecution(activeQueryTestInterface(numericStore), negativeZeroFilter);
      yield* releaseRawQueryExecution(activeQueryTestInterface(numericStore), offsetFilter);
      yield* releaseRawQueryExecution(activeQueryTestInterface(bigintStore), bigIntFilter);
      yield* releaseRawQueryExecution(activeQueryTestInterface(decimalStore), decimalFilter);
    }),
  );

  it.effect("covers cache keys for nested scalar and boolean filter values", () =>
    Effect.gen(function* () {
      const eventStore = new TopicStore(
        "events",
        Schema.Struct({
          id: Schema.String,
          label: Schema.String,
          tags: Schema.Array(Schema.String),
          metadata: Schema.Struct({
            kind: Schema.String,
            scope: Schema.String,
          }),
          active: Schema.Boolean,
        }),
        "id",
        () => {},
      );

      yield* publishTopicStoreRow(
        eventStore,
        {
          id: "a",
          label: "foo",
          tags: ["open", "closed"],
          metadata: { kind: "test", scope: "global" },
          active: true,
        },
        invalidRow,
      );

      const nestedScalarFilter = yield* prepareRuntimeRawQuery(
        "events",
        activeQueryTestMetadata(eventStore),
        {
          select: ["id", "metadata"],
          where: [{ field: "metadata.kind", type: "equals", filter: "test" }],
        },
      );
      const booleanFilter = yield* prepareRuntimeRawQuery(
        "events",
        activeQueryTestMetadata(eventStore),
        {
          select: ["id", "active"],
          where: [{ field: "active", type: "equals", filter: true }],
        },
      );

      const nestedScalarExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(eventStore),
        nestedScalarFilter,
      );
      const booleanExecution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(eventStore),
        booleanFilter,
      );

      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(eventStore))).toBe(
        2,
      );

      expect(nestedScalarExecution.initial("query").totalRows).toBe(1);
      expect(booleanExecution.initial("query").totalRows).toBe(1);

      const nestedScalarCursor = nestedScalarExecution.createCursor();
      const booleanCursor = booleanExecution.createCursor();

      expect((yield* nestedScalarExecution.next("query", nestedScalarCursor))._tag).toBe("None");
      expect((yield* booleanExecution.next("query", booleanCursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(activeQueryTestInterface(eventStore), nestedScalarFilter);
      yield* releaseRawQueryExecution(activeQueryTestInterface(eventStore), booleanFilter);
    }),
  );

  it.effect("does not collide delimiter-bearing field names in cache keys", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "delimiter-fields",
        Schema.Struct({
          id: Schema.String,
          a: Schema.Number,
          b: Schema.Number,
          "a:asc;b": Schema.Number,
        }),
        "id",
        () => {},
      );
      const splitFieldsQuery = yield* prepareRuntimeRawQuery(
        "delimiter-fields",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          orderBy: [
            {
              field: "a",
              direction: "asc",
            },
            {
              field: "b",
              direction: "desc",
            },
          ],
        },
      );
      const delimiterFieldQuery = yield* prepareRuntimeRawQuery(
        "delimiter-fields",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          orderBy: [
            {
              field: "a:asc;b",
              direction: "desc",
            },
          ],
        },
      );

      yield* acquireRawQueryExecution(activeQueryTestInterface(store), splitFieldsQuery);
      yield* acquireRawQueryExecution(activeQueryTestInterface(store), delimiterFieldQuery);

      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(2);

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), splitFieldsQuery);
      yield* releaseRawQueryExecution(activeQueryTestInterface(store), delimiterFieldQuery);
    }),
  );

  it.effect("rejects non-serializable filter values before cache-keying", () =>
    Effect.gen(function* () {
      const firstFunction = () => "first";
      const firstSymbol = Symbol("first");
      const firstMap = new Map([["marker", "first"]]);
      const store = new TopicStore(
        "special-non-serializable",
        Schema.Struct({
          id: Schema.String,
          marker: Schema.String,
        }),
        "id",
        () => {},
      );

      const functionFilter = yield* Effect.flip(
        prepareRuntimeRawQuery("special-non-serializable", activeQueryTestMetadata(store), {
          select: ["id", "marker"],
          where: [{ field: "marker", type: "equals", filter: firstFunction }],
        }),
      );
      expect(functionFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("does not satisfy its configured schema"),
      });

      const symbolFilter = yield* Effect.flip(
        prepareRuntimeRawQuery("special-non-serializable", activeQueryTestMetadata(store), {
          select: ["id", "marker"],
          where: [{ field: "marker", type: "equals", filter: firstSymbol }],
        }),
      );
      expect(symbolFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("does not satisfy its configured schema"),
      });

      const mapFilter = yield* Effect.flip(
        prepareRuntimeRawQuery("special-non-serializable", activeQueryTestMetadata(store), {
          select: ["id", "marker"],
          where: [{ field: "marker", type: "equals", filter: firstMap }],
        }),
      );
      expect(mapFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("does not satisfy its configured schema"),
      });
    }),
  );
});
