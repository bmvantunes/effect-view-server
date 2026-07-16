import { describe, expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Option, Result, Schema } from "effect";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeQueryTestInterface,
  activeQueryTestMetadata,
  activeStoreRawQueryExecutionCount,
  createActiveQueryRegistry,
  releaseMaterializedQueryExecution,
  releaseRawQueryExecution,
} from "../test-harness/active-query-interface";
import { replaceRetainedMatchingEntryAtIndex } from "./active-raw-query";
import { prepareRuntimeRawQuery } from "./raw-query-compiler";
import { makeQueryResultSemantics } from "./query-result-semantics";
import { publishTopicStoreRow, TopicStore } from "./topic-store";

const invalidRow = (_topic: string, message: string): Error => new Error(message);
const emptyResultSemantics = makeQueryResultSemantics([]);

describe("column-live-view-engine Active Query materialization", () => {
  it("falls back when an indexed retained replacement points at a different key", () => {
    const windowEntries = [
      { key: "a", row: { id: "a", score: 2 } },
      { key: "b", row: { id: "b", score: 1 } },
    ];
    const replaced = replaceRetainedMatchingEntryAtIndex(
      windowEntries,
      0,
      "b",
      { id: "b", score: 3 },
      (left, right) => right.row.score - left.row.score,
      50,
    );

    expect(replaced).toBeUndefined();
    expect(windowEntries).toStrictEqual([
      { key: "a", row: { id: "a", score: 2 } },
      { key: "b", row: { id: "b", score: 1 } },
    ]);
  });

  it.effect("binds a storage projection once per raw execution lease", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "projection-bind-frequency",
        Schema.Struct({
          id: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", score: 1 }, invalidRow);
      const queryInterface = activeQueryTestInterface(store);
      const storageProjection = Option.getOrThrow(
        Option.fromUndefinedOr(queryInterface.storageProjection),
      );
      let projectionReads = 0;
      const observedQueryInterface = {
        ...queryInterface,
        get storageProjection() {
          projectionReads += 1;
          return storageProjection;
        },
      };
      const compiled = yield* prepareRuntimeRawQuery(
        "projection-bind-frequency",
        activeQueryTestMetadata(store),
        { select: ["id", "score"] },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      const cursor = execution.createCursor();
      expect(execution.initial("query").rows).toStrictEqual([{ id: "a", score: 1 }]);
      expect((yield* execution.next("query", cursor))._tag).toBe("None");
      yield* publishTopicStoreRow(store, { id: "b", score: 2 }, invalidRow);
      expect((yield* execution.next("query", cursor))._tag).toBe("Some");
      expect(projectionReads).toBe(1);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("rejects incompatible projection proofs before acquiring raw execution ownership", () =>
    Effect.gen(function* () {
      const target = new TopicStore(
        "projection-ownership-target",
        Schema.Struct({
          id: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      const incompatible = new TopicStore(
        "projection-ownership-incompatible",
        Schema.Struct({
          id: Schema.String,
          score: Schema.String,
        }),
        "id",
        () => {},
      );
      const targetQueryInterface = activeQueryTestInterface(target);
      const targetCompiled = yield* prepareRuntimeRawQuery(
        "projection-ownership-target",
        activeQueryTestMetadata(target),
        { select: ["id"] },
      );
      const incompatibleCompiled = yield* prepareRuntimeRawQuery(
        "projection-ownership-incompatible",
        activeQueryTestMetadata(incompatible),
        { select: ["id"] },
      );

      const emptyRegistryExit = yield* Effect.exit(
        acquireRawQueryExecution(targetQueryInterface, incompatibleCompiled),
      );
      const emptyRegistryError = Exit.match(emptyRegistryExit, {
        onFailure: (cause) =>
          Result.match(Cause.findDefect(cause), {
            onFailure: () => "missing projection binding error",
            onSuccess: (defect) =>
              defect instanceof Error ? defect.message : "unexpected non-error defect",
          }),
        onSuccess: () => "unexpected projection binding success",
      });
      expect(emptyRegistryError).toBe(
        "Topic Storage projection schema does not match its compiled proof.",
      );
      expect(yield* activeStoreRawQueryExecutionCount(targetQueryInterface)).toBe(0);

      yield* acquireRawQueryExecution(targetQueryInterface, targetCompiled);
      expect(yield* activeStoreRawQueryExecutionCount(targetQueryInterface)).toBe(1);
      const existingEntryExit = yield* Effect.exit(
        acquireRawQueryExecution(targetQueryInterface, incompatibleCompiled),
      );
      const existingEntryError = Exit.match(existingEntryExit, {
        onFailure: (cause) =>
          Result.match(Cause.findDefect(cause), {
            onFailure: () => "missing projection binding error",
            onSuccess: (defect) =>
              defect instanceof Error ? defect.message : "unexpected non-error defect",
          }),
        onSuccess: () => "unexpected projection binding success",
      });
      expect(existingEntryError).toBe(
        "Topic Storage projection schema does not match its compiled proof.",
      );
      expect(yield* activeStoreRawQueryExecutionCount(targetQueryInterface)).toBe(1);

      yield* releaseRawQueryExecution(targetQueryInterface, targetCompiled);
      expect(yield* activeStoreRawQueryExecutionCount(targetQueryInterface)).toBe(0);
    }),
  );

  it.effect("keeps materialized execution caches local to the active query registry", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "materialized-registry-isolation",
        Schema.Struct({
          id: Schema.String,
        }),
        "id",
        () => {},
      );
      const queryInterface = activeQueryTestInterface(store);
      const isolatedQueryInterface = {
        ...queryInterface,
        activeQueries: createActiveQueryRegistry(),
      };

      const emptyEvaluation = (version: number) => ({
        rows: [],
        keys: [],
        window: [],
        totalRows: 0,
        version,
      });

      const emptyDiagnostics = () => ({
        fullEvaluationCount: 0,
        patchedEvaluationCount: 0,
      });

      const firstExecution = yield* acquireMaterializedQueryExecution(
        queryInterface,
        "grouped",
        emptyResultSemantics,
        () => ({
          diagnostics: emptyDiagnostics,
          incremental: false,
          latest: () => emptyEvaluation(queryInterface.version()),
        }),
      );
      const secondExecution = yield* acquireMaterializedQueryExecution(
        isolatedQueryInterface,
        "grouped",
        emptyResultSemantics,
        () => ({
          diagnostics: emptyDiagnostics,
          incremental: false,
          latest: () => emptyEvaluation(isolatedQueryInterface.version()),
        }),
      );

      expect(Object.isFrozen(firstExecution)).toBe(true);
      expect(() => Object.assign(firstExecution, secondExecution)).toThrowError(TypeError);
      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(1);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedQueryInterface)).toBe(1);

      yield* releaseMaterializedQueryExecution(queryInterface, "grouped");
      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(0);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedQueryInterface)).toBe(1);

      yield* releaseMaterializedQueryExecution(isolatedQueryInterface, "grouped");
      expect(yield* activeStoreRawQueryExecutionCount(isolatedQueryInterface)).toBe(0);
    }),
  );
});
