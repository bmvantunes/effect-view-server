import { Effect, Option, Schema, Stream } from "effect";
import { describe, expect, it } from "@effect/vitest";
import { ViewServerId } from "@effect-view-server/config";
import { prepareRuntimeRawQuery } from "./raw-query-compiler";
import {
  compareRuntimeFilterExpressionStructure,
  normalizeWhere,
  type RuntimeFilterExpression,
} from "./filter-expression";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import { rawQueryCompilerMetadata } from "./raw-query-metadata";
import { compileRawPredicate } from "./raw-predicate-compiler";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";
import type { TopicRowChangeBatch } from "./row-scan";
import { makeEngine, order } from "../test-harness/public-engine";
import { scanTopicRawWindow, type TopicRawWindowScanState } from "./topic-raw-window-scanner";
import type { TopicRawWindowScanPlan } from "./raw-window-scan";
import { evaluateCompiledGroupedQuery, prepareRuntimeGroupedQuery } from "./grouped-query-compiler";

const Row = Schema.Struct({
  id: ViewServerId,
  status: Schema.Literals(["open", "closed"]),
  value: Schema.Number,
});

const metadata = rawQueryCompilerMetadata(Row);

describe("explicit match-none filter", () => {
  it("normalizes source-native FALSE and composes Boolean operators", () => {
    const falseWhere = normalizeWhere([{ type: "FALSE" }], metadata.filterFields);
    const secondFalseWhere = normalizeWhere([{ type: "FALSE" }], metadata.filterFields);
    const doubleNotFalse = normalizeWhere(
      [{ type: "NOT", condition: { type: "NOT", condition: { type: "FALSE" } } }],
      metadata.filterFields,
    );
    const andFalse = normalizeWhere(
      [
        {
          type: "AND",
          conditions: [{ field: "status", type: "equals", filter: "open" }, { type: "FALSE" }],
        },
      ],
      metadata.filterFields,
    );
    const andTrue = normalizeWhere(
      [
        {
          type: "AND",
          conditions: [
            { type: "NOT", condition: { type: "FALSE" } },
            { field: "status", type: "equals", filter: "open" },
          ],
        },
      ],
      metadata.filterFields,
    );
    const orTrue = normalizeWhere(
      [
        {
          type: "OR",
          conditions: [
            { type: "NOT", condition: { type: "FALSE" } },
            { field: "status", type: "equals", filter: "open" },
          ],
        },
      ],
      metadata.filterFields,
    );
    const emptyInOrFalse = normalizeWhere(
      [
        {
          type: "OR",
          conditions: [{ type: "FALSE" }, { field: "status", type: "in", filter: [] }],
        },
      ],
      metadata.filterFields,
    );
    const allTrue = normalizeWhere(
      [
        {
          type: "AND",
          conditions: [
            { type: "NOT", condition: { type: "FALSE" } },
            { type: "NOT", condition: { type: "FALSE" } },
          ],
        },
      ],
      metadata.filterFields,
    );
    const allFalse = normalizeWhere(
      [{ type: "OR", conditions: [{ type: "FALSE" }, { type: "FALSE" }] }],
      metadata.filterFields,
    );

    expect(falseWhere?._tag).toBe("false");
    expect(falseWhere?.key).toBe("expression:FALSE");
    expect(secondFalseWhere?.key).toBe(falseWhere?.key);
    expect(doubleNotFalse?._tag).toBe("false");
    expect(andFalse?._tag).toBe("false");
    expect(andTrue?.key).toBe(
      normalizeWhere([{ field: "status", type: "equals", filter: "open" }], metadata.filterFields)
        ?.key,
    );
    expect(orTrue).toBeUndefined();
    expect(emptyInOrFalse?._tag).toBe("false");
    expect(allTrue).toBeUndefined();
    expect(allFalse?._tag).toBe("false");
  });

  it("compiles source constants and short-circuits Boolean siblings", () => {
    const falseWhere = normalizeWhere([{ type: "FALSE" }], metadata.filterFields);
    const notFalse = normalizeWhere(
      [{ type: "NOT", condition: { type: "FALSE" } }],
      metadata.filterFields,
    );
    const andFalse = normalizeWhere(
      [
        {
          type: "AND",
          conditions: [{ field: "status", type: "equals", filter: "open" }, { type: "FALSE" }],
        },
      ],
      metadata.filterFields,
    );
    const orFalse = normalizeWhere(
      [
        {
          type: "OR",
          conditions: [{ field: "status", type: "equals", filter: "open" }, { type: "FALSE" }],
        },
      ],
      metadata.filterFields,
    );
    const unknownCondition: RuntimeFilterExpression = {
      _tag: "condition",
      key: "unknown-condition",
      field: "status",
      type: "equals",
      caseSensitive: false,
      accentSensitive: false,
      filter: "open",
    };
    const falseWithUnknown: RuntimeFilterExpression = {
      _tag: "group",
      key: "false-with-unknown",
      type: "OR",
      conditions: [{ _tag: "false", key: "expression:FALSE" }, unknownCondition],
    };
    const trueWithUnknown: RuntimeFilterExpression = {
      _tag: "group",
      key: "true-with-unknown",
      type: "AND",
      conditions: [{ _tag: "true", key: "expression:TRUE" }, unknownCondition],
    };

    expect(
      compileRawPredicate<typeof Row.Type>(metadata, falseWhere).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(false);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, notFalse).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, andFalse).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(false);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, orFalse).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
    const compiledTrue = compileRawPredicate<typeof Row.Type>(metadata, {
      _tag: "true",
      key: "expression:TRUE",
    });
    expect(compiledTrue.plan.callbackRequired).toBe(false);
    expect(compiledTrue.matches({ id: "a", status: "open", value: 1 })).toBe(true);
    const notTrue: RuntimeFilterExpression = {
      _tag: "NOT",
      key: "not-true",
      condition: { _tag: "true", key: "expression:TRUE" },
    };
    const notUnknown: RuntimeFilterExpression = {
      _tag: "NOT",
      key: "not-unknown",
      condition: {
        _tag: "condition",
        key: "unknown-condition-not",
        field: "status",
        type: "equals",
        caseSensitive: false,
        accentSensitive: false,
        filter: "open",
      },
    };
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, notTrue).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(false);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, notUnknown).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(false);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, falseWithUnknown).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, trueWithUnknown).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
  });

  it("handles shared source constants in recursive expression graphs", () => {
    const unknownCondition: RuntimeFilterExpression = {
      _tag: "condition",
      key: "unknown-condition",
      field: "status",
      type: "equals",
      caseSensitive: false,
      accentSensitive: false,
      filter: "open",
    };
    const sharedTrue: RuntimeFilterExpression = { _tag: "true", key: "expression:TRUE" };
    const sharedFalse: RuntimeFilterExpression = {
      _tag: "false",
      key: "expression:FALSE",
    };
    const sharedDagTrue: RuntimeFilterExpression = {
      _tag: "group",
      key: "shared-true-with-unknown",
      type: "AND",
      conditions: [sharedTrue, sharedTrue, unknownCondition],
    };
    const sharedDagFalse: RuntimeFilterExpression = {
      _tag: "group",
      key: "shared-false-with-unknown",
      type: "OR",
      conditions: [sharedFalse, sharedFalse, unknownCondition],
    };
    const allTrueGroup: RuntimeFilterExpression = {
      _tag: "group",
      key: "all-true",
      type: "AND",
      conditions: [sharedTrue, sharedTrue],
    };
    const allFalseGroup: RuntimeFilterExpression = {
      _tag: "group",
      key: "all-false",
      type: "OR",
      conditions: [sharedFalse, sharedFalse],
    };
    const allTrueOrGroup: RuntimeFilterExpression = {
      _tag: "group",
      key: "all-true-or",
      type: "OR",
      conditions: [sharedTrue, sharedTrue],
    };
    const sharedDag: RuntimeFilterExpression = {
      _tag: "group",
      key: "expression:OR:2",
      type: "OR",
      conditions: [sharedFalse, sharedFalse],
    };

    expect(
      compareRuntimeFilterExpressionStructure(
        { _tag: "false", key: "expression:FALSE" },
        { _tag: "false", key: "expression:FALSE" },
      ),
    ).toBe(0);
    expect(
      compareRuntimeFilterExpressionStructure(
        { _tag: "true", key: "expression:TRUE" },
        { _tag: "true", key: "expression:TRUE" },
      ),
    ).toBe(0);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, allTrueGroup).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, allFalseGroup).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(false);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, allTrueOrGroup).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, sharedDagTrue).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, sharedDagFalse).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(true);
    expect(
      compileRawPredicate<typeof Row.Type>(metadata, sharedDag).matches({
        id: "a",
        status: "open",
        value: 1,
      }),
    ).toBe(false);
  });

  it.live("keeps raw, grouped, and live snapshots empty as rows arrive", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", []);
      // The compiler metadata above uses a minimal row, while the shared engine harness exposes
      // only its orders topic. FALSE is field-independent, so the harness row shape is sufficient.
      const raw = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [{ type: "FALSE" }],
      });
      const grouped = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ type: "FALSE" }],
      });
      expect(raw.rows).toStrictEqual([]);
      expect(raw.totalRows).toBe(0);
      expect(grouped.rows).toStrictEqual([]);
      expect(grouped.totalRows).toBe(0);

      const subscription = yield* engine.subscribe("orders", {
        select: ["id"],
        where: [{ type: "FALSE" }],
      });
      const initial = Option.getOrThrow(yield* Stream.runHead(subscription.events));
      const groupedSubscription = yield* engine.subscribe("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ type: "FALSE" }],
      });
      const groupedInitial = Option.getOrThrow(yield* Stream.runHead(groupedSubscription.events));
      yield* engine.publish("orders", order("future", "open", 10, 1));
      const afterPublishRaw = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [{ type: "FALSE" }],
      });
      const afterPublishGrouped = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ type: "FALSE" }],
      });
      const rawNext = yield* Stream.runHead(subscription.events).pipe(Effect.timeout("20 millis"));
      const groupedNext = yield* Stream.runHead(groupedSubscription.events).pipe(
        Effect.timeout("20 millis"),
      );
      yield* subscription.close();
      yield* groupedSubscription.close();
      expect(initial).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: expect.any(String),
        version: expect.any(Number),
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(afterPublishRaw.rows).toStrictEqual([]);
      expect(afterPublishRaw.totalRows).toBe(0);
      expect(afterPublishGrouped.rows).toStrictEqual([]);
      expect(afterPublishGrouped.totalRows).toBe(0);
      expect(Option.isNone(rawNext)).toBe(true);
      expect(Option.isNone(groupedNext)).toBe(true);
      expect(groupedInitial).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: expect.any(String),
        version: expect.any(Number),
        keys: [],
        rows: [],
        totalRows: 0,
      });
    }),
  );

  it("compiles constant FALSE without invoking an expensive sibling or row scanner", () => {
    const compiled = compileRawPredicate<typeof Row.Type>(metadata, {
      _tag: "group",
      key: "expensive-and-false",
      type: "AND",
      conditions: [
        {
          _tag: "false",
          key: "expression:FALSE",
        },
        {
          key: "expensive",
          _tag: "condition",
          field: "status",
          type: "equals",
          caseSensitive: false,
          accentSensitive: false,
          filter: "open",
        },
      ],
    });
    let siblingReads = 0;
    const row: typeof Row.Type = {
      id: "a",
      get status(): "open" {
        siblingReads += 1;
        return "open";
      },
      value: 1,
    };
    expect(compiled.plan.alwaysFalse).toBe(true);
    expect(compiled.matches(row)).toBe(false);
    expect(siblingReads).toBe(0);

    const state = {
      columns: new Map(),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: new Map(),
      slots: [{ key: "a", row: { id: "a" } }],
    } satisfies TopicRawWindowScanState;
    const plan = {
      predicate: compiled.plan,
      orderBy: [],
      matches: () => {
        throw new Error("constant FALSE must not visit rows");
      },
      compare: () => 0,
      offset: 0,
      limit: undefined,
    } satisfies TopicRawWindowScanPlan<object>;
    expect(scanTopicRawWindow(state, plan)).toStrictEqual({
      keys: [],
      window: [],
      totalRows: 0,
    });
  });

  it.effect("preserves constant FALSE through a partitioned raw plan", () =>
    Effect.gen(function* () {
      let partitionMatches = 0;
      const partition: ColumnLiveViewEngineQueryPartition = {
        key: "partition",
        ownedStorageKeys: () => ["a"],
        matches: () => {
          partitionMatches += 1;
          return true;
        },
      };
      const compiled = yield* prepareRuntimeRawQuery(
        "rows",
        metadata,
        {
          select: ["id"],
          where: [{ type: "FALSE" }],
        },
        partition,
      );

      expect(compiled.plan.partitionKey).toBe("partition");
      expect(compiled.plan.predicate.plan.alwaysFalse).toBe(true);
      expect(compiled.plan.predicate.plan.callbackRequired).toBe(false);
      expect(compiled.plan.predicate.plan.callbackSkippable).toBe(true);
      expect(compiled.plan.predicate.matches({ id: "a" })).toBe(false);
      expect(partitionMatches).toBe(0);
    }),
  );

  it.effect("skips grouped row scans for a constant FALSE plan", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRuntimeGroupedQuery("rows", metadata, {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ type: "FALSE" }],
      });
      let scanCalls = 0;
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: () => {
            scanCalls += 1;
          },
          version: () => 1,
        },
        compiled,
      );

      expect(compiled.plan.alwaysFalse).toBe(true);
      expect(scanCalls).toBe(0);
      expect(evaluation).toStrictEqual({
        rows: [],
        keys: [],
        totalRows: 0,
        version: 1,
        window: [],
      });
    }),
  );

  it.effect("keeps a constant FALSE grouped incremental execution empty", () =>
    Effect.gen(function* () {
      let version = 0;
      const batches: ReadonlyArray<TopicRowChangeBatch<object>> = [{ version: 1, changes: [] }];
      let scanCalls = 0;
      const compiled = yield* prepareRuntimeGroupedQuery("rows", metadata, {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ type: "FALSE" }],
      });
      const execution = makeIncrementalGroupedQueryExecution(
        {
          changesSince: () => batches,
          scanRows: () => {
            scanCalls += 1;
          },
          version: () => version,
        },
        compiled,
        () => undefined,
      );

      expect(execution.latest()).toStrictEqual({
        rows: [],
        keys: [],
        totalRows: 0,
        version: 0,
        window: [],
      });
      version = 1;
      expect(execution.latest()).toStrictEqual({
        rows: [],
        keys: [],
        totalRows: 0,
        version: 1,
        window: [],
      });
      expect(scanCalls).toBe(0);
    }),
  );
});
