import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import {
  acquireRawQueryExecution,
  activeQueryTestInterface,
  activeQueryTestMetadata,
  releaseRawQueryExecution,
} from "../test-harness/active-query-interface";
import { prepareRuntimeRawQuery } from "./raw-query-compiler";
import {
  deleteTopicStoreRow,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  TopicStore,
} from "./topic-store";

const invalidRow = (_topic: string, message: string): Error => new Error(message);
describe("column-live-view-engine Active Query raw", () => {
  it.effect("updates raw active windows from retained insert-only changes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-insert-incremental",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-insert-incremental",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-insert-incremental",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "b",
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
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
      expect(queryInterface.changesSince(3)).toBeUndefined();
    }),
  );

  it.effect("updates total rows for lower-ranked insert-only raw changes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-insert-incremental-count-only",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-insert-incremental-count-only",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 0 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-insert-incremental-count-only",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [],
        totalRows: 4,
      });
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("updates retained match-to-match raw rows that move up without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-match-update-move-up",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-match-update-move-up",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 5 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-match-update-move-up",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "move",
            key: "b",
            fromIndex: 1,
            toIndex: 0,
          },
          {
            type: "update",
            key: "b",
            row: {
              id: "b",
              score: 5,
            },
            index: 0,
          },
        ],
        totalRows: 3,
      });
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("falls back to a raw window scan when retained match-to-match raw rows move down", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-match-update-move-down-fallback",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-match-update-move-down-fallback",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 0 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-match-update-move-down-fallback",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "b",
          },
          {
            type: "insert",
            key: "a",
            row: {
              id: "a",
              score: 1,
            },
            index: 1,
          },
        ],
        totalRows: 3,
      });
      expect(scanLimits).toStrictEqual([3, 3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("repositions unlimited retained match-to-match raw rows without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-match-update-unlimited-move-down",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-match-update-unlimited-move-down",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b", "a"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 0 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-match-update-unlimited-move-down",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "move",
            key: "c",
            fromIndex: 0,
            toIndex: 2,
          },
          {
            type: "update",
            key: "c",
            row: {
              id: "c",
              score: 0,
            },
            index: 2,
          },
        ],
        totalRows: 3,
      });
      expect(scanLimits).toStrictEqual([undefined]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect(
    "keeps retained replacement batches sorted when multiple rows update before polling",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-match-update-batched-replacements",
          Schema.Struct({
            id: Schema.String,
            status: Schema.String,
            score: Schema.Number,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 3 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 1 }, invalidRow);

        const scanLimits: Array<number | undefined> = [];
        const queryInterface = activeQueryTestInterface(store);
        const observedQueryInterface = {
          ...queryInterface,
          scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
            scanLimits.push(plan.limit);
            return queryInterface.scanRawWindow(plan);
          },
        };

        const compiled = yield* prepareRuntimeRawQuery(
          "raw-match-update-batched-replacements",
          activeQueryTestMetadata(store),
          {
            select: ["id", "score"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            orderBy: [{ field: "score", direction: "desc" }],
          },
        );

        const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
        expect(execution.initial("query").keys).toStrictEqual(["a", "b", "c"]);
        const cursor = execution.createCursor();

        yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 4 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 5 }, invalidRow);

        const delta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(delta)).toStrictEqual({
          type: "delta",
          topic: "raw-match-update-batched-replacements",
          queryId: "query",
          fromVersion: 3,
          toVersion: 5,
          operations: [
            {
              type: "move",
              key: "c",
              fromIndex: 2,
              toIndex: 0,
            },
            {
              type: "update",
              key: "c",
              row: {
                id: "c",
                score: 5,
              },
              index: 0,
            },
            {
              type: "move",
              key: "b",
              fromIndex: 2,
              toIndex: 1,
            },
            {
              type: "update",
              key: "b",
              row: {
                id: "b",
                score: 4,
              },
              index: 1,
            },
          ],
          totalRows: 3,
        });
        expect(scanLimits).toStrictEqual([undefined]);

        yield* releaseRawQueryExecution(observedQueryInterface, compiled);
      }),
  );

  it.effect("uses shifted retained key indexes after removals before replacements", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-match-update-remove-before-replace",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-match-update-remove-before-replace",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 3,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["d", "c", "b"]);
      const cursor = execution.createCursor();

      yield* deleteTopicStoreRow(store, "d");
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 5 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-match-update-remove-before-replace",
        queryId: "query",
        fromVersion: 4,
        toVersion: 6,
        operations: [
          {
            type: "remove",
            key: "d",
          },
          {
            type: "move",
            key: "b",
            fromIndex: 1,
            toIndex: 0,
          },
          {
            type: "update",
            key: "b",
            row: {
              id: "b",
              score: 5,
            },
            index: 0,
          },
          {
            type: "insert",
            key: "a",
            row: {
              id: "a",
              score: 1,
            },
            index: 2,
          },
        ],
        totalRows: 3,
      });
      expect(scanLimits).toStrictEqual([4]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("falls back when retained match-to-match raw updates touch outside lookahead", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-match-update-outside-lookahead-fallback",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-match-update-outside-lookahead-fallback",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["d", "c"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 5 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-match-update-outside-lookahead-fallback",
        queryId: "query",
        fromVersion: 4,
        toVersion: 5,
        operations: [
          {
            type: "remove",
            key: "c",
          },
          {
            type: "insert",
            key: "a",
            row: {
              id: "a",
              score: 5,
            },
            index: 0,
          },
        ],
        totalRows: 4,
      });
      expect(scanLimits).toStrictEqual([3, 3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("falls back when retained match-to-match tail rows worsen", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-match-update-tail-worsens-fallback",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 10 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 9 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 8 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 7 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-match-update-tail-worsens-fallback",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["a", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 6 }, invalidRow);
      const updateDelta = yield* execution.next("query", cursor);
      expect(Option.isNone(updateDelta)).toBe(true);

      yield* deleteTopicStoreRow(store, "b");
      const deleteDelta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(deleteDelta)).toStrictEqual({
        type: "delta",
        topic: "raw-match-update-tail-worsens-fallback",
        queryId: "query",
        fromVersion: 4,
        toVersion: 6,
        operations: [
          {
            type: "remove",
            key: "b",
          },
          {
            type: "insert",
            key: "d",
            row: {
              id: "d",
              score: 7,
            },
            index: 1,
          },
        ],
        totalRows: 3,
      });
      expect(scanLimits).toStrictEqual([3, 3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("updates pending retained raw insert rows before merging without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-pending-insert-match-update",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-pending-insert-match-update",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 5 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-pending-insert-match-update",
        queryId: "query",
        fromVersion: 3,
        toVersion: 5,
        operations: [
          {
            type: "remove",
            key: "b",
          },
          {
            type: "insert",
            key: "d",
            row: {
              id: "d",
              score: 5,
            },
            index: 0,
          },
        ],
        totalRows: 4,
      });
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("falls back to a raw window scan when retained raw changes are unavailable", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-unavailable-changes-fallback",
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
        changesSince: () => undefined,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-unavailable-changes-fallback",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta).operations).toStrictEqual([
        {
          type: "remove",
          key: "a",
        },
        {
          type: "insert",
          key: "c",
          row: {
            id: "c",
            score: 3,
          },
          index: 0,
        },
      ]);
      expect(scanLimits).toStrictEqual([3, 3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("ignores non-matching insert-only raw changes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-mixed-insert-incremental",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-mixed-insert-incremental",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRows(
        store,
        [
          { id: "closed", status: "closed", score: 10 },
          { id: "c", status: "open", score: 3 },
        ],
        invalidRow,
      );

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta).operations).toStrictEqual([
        {
          type: "remove",
          key: "a",
        },
        {
          type: "insert",
          key: "c",
          row: {
            id: "c",
            score: 3,
          },
          index: 0,
        },
      ]);
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect(
    "updates zero-limit raw active queries from insert-only changes without rescanning",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-zero-limit-incremental",
          Schema.Struct({
            id: Schema.String,
            status: Schema.String,
            score: Schema.Number,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);

        const scanLimits: Array<number | undefined> = [];
        const queryInterface = activeQueryTestInterface(store);
        const observedQueryInterface = {
          ...queryInterface,
          scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
            scanLimits.push(plan.limit);
            return queryInterface.scanRawWindow(plan);
          },
        };

        const compiled = yield* prepareRuntimeRawQuery(
          "raw-zero-limit-incremental",
          activeQueryTestMetadata(store),
          {
            select: ["id"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            limit: 0,
          },
        );

        const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
        expect(execution.initial("query").totalRows).toBe(1);
        const cursor = execution.createCursor();

        yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);

        const delta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(delta)).toStrictEqual({
          type: "delta",
          topic: "raw-zero-limit-incremental",
          queryId: "query",
          fromVersion: 1,
          toVersion: 2,
          operations: [],
          totalRows: 2,
        });
        expect(scanLimits).toStrictEqual([0]);

        yield* releaseRawQueryExecution(observedQueryInterface, compiled);
      }),
  );

  it.effect("ignores non-matching retained raw updates and deletes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-non-matching-change-incremental",
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
      yield* publishTopicStoreRow(
        store,
        { id: "closed-update", status: "closed", score: 3 },
        invalidRow,
      );
      yield* publishTopicStoreRow(
        store,
        { id: "closed-delete", status: "closed", score: 4 },
        invalidRow,
      );

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-non-matching-change-incremental",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );
      let compareCount = 0;
      const observedCompiled = {
        ...compiled,
        plan: {
          ...compiled.plan,
          compare: (
            left: Parameters<typeof compiled.plan.compare>[0],
            right: Parameters<typeof compiled.plan.compare>[1],
          ) => {
            compareCount += 1;
            return compiled.plan.compare(left, right);
          },
        },
      };

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, observedCompiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();
      compareCount = 0;

      yield* patchTopicStoreRow(
        store,
        "closed-update",
        {
          score: 30,
        },
        invalidRow,
      );
      yield* deleteTopicStoreRow(store, "closed-delete");

      const delta = yield* execution.next("query", cursor);
      expect(Option.isNone(delta)).toBe(true);
      expect(scanLimits).toStrictEqual([3]);
      expect(compareCount).toBe(0);

      yield* releaseRawQueryExecution(observedQueryInterface, observedCompiled);
    }),
  );

  it.effect("sorts retained raw inserts with the storage slot comparator", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-retained-insert-slot-sort",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-retained-insert-slot-sort",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );
      let compareCount = 0;
      const observedCompiled = {
        ...compiled,
        plan: {
          ...compiled.plan,
          compare: (
            left: Parameters<typeof compiled.plan.compare>[0],
            right: Parameters<typeof compiled.plan.compare>[1],
          ) => {
            compareCount += 1;
            return compiled.plan.compare(left, right);
          },
        },
      };

      const execution = yield* acquireRawQueryExecution(
        activeQueryTestInterface(store),
        observedCompiled,
      );
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();
      compareCount = 0;

      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.isSome(delta)).toBe(true);
      expect(cursor.evaluation.keys).toStrictEqual(["c", "b"]);
      expect(compareCount).toBe(0);

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), observedCompiled);
    }),
  );

  it.effect("falls back to the row comparator when retained raw slot sorting is unavailable", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-retained-insert-row-sort-fallback",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-retained-insert-row-sort-fallback",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );
      let compareCount = 0;
      const observedCompiled = {
        ...compiled,
        plan: {
          ...compiled.plan,
          compare: (
            left: Parameters<typeof compiled.plan.compare>[0],
            right: Parameters<typeof compiled.plan.compare>[1],
          ) => {
            compareCount += 1;
            return compiled.plan.compare(left, right);
          },
        },
      };
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        compareRawSlots: () => undefined,
      };

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, observedCompiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();
      compareCount = 0;

      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.isSome(delta)).toBe(true);
      expect(cursor.evaluation.keys).toStrictEqual(["c", "b"]);
      expect(compareCount).toBeGreaterThan(0);

      yield* releaseRawQueryExecution(observedQueryInterface, observedCompiled);
    }),
  );

  it.effect("falls back to the row comparator when retained raw inserted slots disappear", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-retained-insert-missing-slot-fallback",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-retained-insert-missing-slot-fallback",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );
      let compareCount = 0;
      const observedCompiled = {
        ...compiled,
        plan: {
          ...compiled.plan,
          compare: (
            left: Parameters<typeof compiled.plan.compare>[0],
            right: Parameters<typeof compiled.plan.compare>[1],
          ) => {
            compareCount += 1;
            return compiled.plan.compare(left, right);
          },
        },
      };
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        slotForKey: (key: string) => (key === "c" ? undefined : queryInterface.slotForKey?.(key)),
      };

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, observedCompiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();
      compareCount = 0;

      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.isSome(delta)).toBe(true);
      expect(cursor.evaluation.keys).toStrictEqual(["c", "b"]);
      expect(compareCount).toBeGreaterThan(0);

      yield* releaseRawQueryExecution(observedQueryInterface, observedCompiled);
    }),
  );

  it.effect(
    "emits the next visible raw delta from the last delivered version after no-op changes",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-noop-then-visible-version",
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
        yield* publishTopicStoreRow(
          store,
          { id: "closed", status: "closed", score: 3 },
          invalidRow,
        );

        const queryInterface = activeQueryTestInterface(store);
        const compiled = yield* prepareRuntimeRawQuery(
          "raw-noop-then-visible-version",
          activeQueryTestMetadata(store),
          {
            select: ["id", "score"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            orderBy: [{ field: "score", direction: "desc" }],
            limit: 2,
          },
        );

        const execution = yield* acquireRawQueryExecution(queryInterface, compiled);
        expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
        const cursor = execution.createCursor();

        yield* patchTopicStoreRow(
          store,
          "closed",
          {
            score: 30,
          },
          invalidRow,
        );

        const noOpDelta = yield* execution.next("query", cursor);
        expect(Option.isNone(noOpDelta)).toBe(true);

        yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 4 }, invalidRow);

        const visibleDelta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(visibleDelta)).toStrictEqual({
          type: "delta",
          topic: "raw-noop-then-visible-version",
          queryId: "query",
          fromVersion: 3,
          toVersion: 5,
          operations: [
            {
              type: "remove",
              key: "a",
            },
            {
              type: "insert",
              key: "c",
              row: {
                id: "c",
                score: 4,
              },
              index: 0,
            },
          ],
          totalRows: 3,
        });

        yield* releaseRawQueryExecution(queryInterface, compiled);
      }),
  );

  it.effect("adds predicate-entering retained raw updates without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-retained-update-enters-predicate",
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
      yield* publishTopicStoreRow(store, { id: "d", status: "closed", score: 4 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-retained-update-enters-predicate",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-retained-update-enters-predicate",
        queryId: "query",
        fromVersion: 4,
        toVersion: 5,
        operations: [
          {
            type: "remove",
            key: "b",
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
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("removes same-key pending raw insert candidates before merging retained changes", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-same-key-pending-insert-removed",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-same-key-pending-insert-removed",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);
      yield* deleteTopicStoreRow(store, "d");

      const delta = yield* execution.next("query", cursor);
      expect(Option.isNone(delta)).toBe(true);
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("falls back after a retained removal consumes lookahead before a later delete", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-retained-removal-consumes-lookahead",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 100 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 90 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 80 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 70 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-retained-removal-consumes-lookahead",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["a", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "a", status: "closed", score: 100 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "low", status: "open", score: 0 }, invalidRow);

      const firstDelta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(firstDelta)).toStrictEqual({
        type: "delta",
        topic: "raw-retained-removal-consumes-lookahead",
        queryId: "query",
        fromVersion: 4,
        toVersion: 6,
        operations: [
          {
            type: "remove",
            key: "a",
          },
          {
            type: "insert",
            key: "c",
            row: {
              id: "c",
              score: 80,
            },
            index: 1,
          },
        ],
        totalRows: 4,
      });
      expect(scanLimits).toStrictEqual([3]);

      yield* deleteTopicStoreRow(store, "b");

      const secondDelta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(secondDelta)).toStrictEqual({
        type: "delta",
        topic: "raw-retained-removal-consumes-lookahead",
        queryId: "query",
        fromVersion: 6,
        toVersion: 7,
        operations: [
          {
            type: "remove",
            key: "b",
          },
          {
            type: "insert",
            key: "d",
            row: {
              id: "d",
              score: 70,
            },
            index: 1,
          },
        ],
        totalRows: 3,
      });
      expect(scanLimits).toStrictEqual([3, 3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect(
    "rescans before accepting later inserts after a retained delete exhausts lookahead",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-retained-exhausted-lookahead-rejects-later-insert",
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

        const compiled = yield* prepareRuntimeRawQuery(
          "raw-retained-exhausted-lookahead-rejects-later-insert",
          activeQueryTestMetadata(store),
          {
            select: ["id", "score"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            orderBy: [{ field: "score", direction: "desc" }],
            limit: 2,
          },
        );

        const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
        expect(execution.initial("query").keys).toStrictEqual(["d", "c"]);
        const cursor = execution.createCursor();

        yield* deleteTopicStoreRow(store, "d");

        const deleteTopDelta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(deleteTopDelta)).toStrictEqual({
          type: "delta",
          topic: "raw-retained-exhausted-lookahead-rejects-later-insert",
          queryId: "query",
          fromVersion: 4,
          toVersion: 5,
          operations: [
            {
              type: "remove",
              key: "d",
            },
            {
              type: "insert",
              key: "b",
              row: {
                id: "b",
                score: 2,
              },
              index: 1,
            },
          ],
          totalRows: 3,
        });
        expect(scanLimits).toStrictEqual([3]);

        yield* publishTopicStoreRow(store, { id: "x", status: "open", score: 0 }, invalidRow);

        const lowInsertDelta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(lowInsertDelta)).toStrictEqual({
          type: "delta",
          topic: "raw-retained-exhausted-lookahead-rejects-later-insert",
          queryId: "query",
          fromVersion: 5,
          toVersion: 6,
          operations: [],
          totalRows: 4,
        });
        expect(scanLimits).toStrictEqual([3, 3]);

        yield* deleteTopicStoreRow(store, "c");

        const deleteSecondDelta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(deleteSecondDelta)).toStrictEqual({
          type: "delta",
          topic: "raw-retained-exhausted-lookahead-rejects-later-insert",
          queryId: "query",
          fromVersion: 6,
          toVersion: 7,
          operations: [
            {
              type: "remove",
              key: "c",
            },
            {
              type: "insert",
              key: "a",
              row: {
                id: "a",
                score: 1,
              },
              index: 1,
            },
          ],
          totalRows: 3,
        });
        expect(scanLimits).toStrictEqual([3, 3]);

        yield* releaseRawQueryExecution(observedQueryInterface, compiled);
      }),
  );

  it.effect(
    "updates total rows for matching deletes outside retained raw windows without rescanning",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-delete-outside-retained-window",
          Schema.Struct({
            id: Schema.String,
            status: Schema.String,
            score: Schema.Number,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(store, { id: "low", status: "open", score: 0 }, invalidRow);
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

        const compiled = yield* prepareRuntimeRawQuery(
          "raw-delete-outside-retained-window",
          activeQueryTestMetadata(store),
          {
            select: ["id", "score"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            orderBy: [{ field: "score", direction: "desc" }],
            limit: 2,
          },
        );

        const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
        expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
        const cursor = execution.createCursor();

        yield* deleteTopicStoreRow(store, "low");

        const delta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(delta)).toStrictEqual({
          type: "delta",
          topic: "raw-delete-outside-retained-window",
          queryId: "query",
          fromVersion: 4,
          toVersion: 5,
          operations: [],
          totalRows: 3,
        });
        expect(scanLimits).toStrictEqual([3]);

        yield* releaseRawQueryExecution(observedQueryInterface, compiled);
      }),
  );

  it.effect(
    "updates zero-limit raw active counts from retained updates and deletes without rescanning",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-zero-limit-mixed-incremental",
          Schema.Struct({
            id: Schema.String,
            status: Schema.String,
            score: Schema.Number,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "b", status: "closed", score: 2 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "d", status: "closed", score: 4 }, invalidRow);

        const scanLimits: Array<number | undefined> = [];
        const queryInterface = activeQueryTestInterface(store);
        const observedQueryInterface = {
          ...queryInterface,
          scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
            scanLimits.push(plan.limit);
            return queryInterface.scanRawWindow(plan);
          },
        };

        const compiled = yield* prepareRuntimeRawQuery(
          "raw-zero-limit-mixed-incremental",
          activeQueryTestMetadata(store),
          {
            select: ["id"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            limit: 0,
          },
        );

        const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
        expect(execution.initial("query").totalRows).toBe(2);
        const cursor = execution.createCursor();

        yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "c", status: "closed", score: 3 }, invalidRow);
        yield* patchTopicStoreRow(
          store,
          "d",
          {
            score: 40,
          },
          invalidRow,
        );
        yield* deleteTopicStoreRow(store, "a");

        const delta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(delta)).toStrictEqual({
          type: "delta",
          topic: "raw-zero-limit-mixed-incremental",
          queryId: "query",
          fromVersion: 4,
          toVersion: 8,
          operations: [],
          totalRows: 1,
        });
        expect(scanLimits).toStrictEqual([0]);

        yield* releaseRawQueryExecution(observedQueryInterface, compiled);
      }),
  );

  it.effect("refills retained visible raw deletes from lookahead without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-visible-delete-fallback",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-visible-delete-fallback",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* deleteTopicStoreRow(store, "c");

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-visible-delete-fallback",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "c",
          },
          {
            type: "insert",
            key: "a",
            row: {
              id: "a",
              score: 1,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      expect(scanLimits).toStrictEqual([3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("falls back to a raw window scan when retained deletes exhaust lookahead", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-visible-delete-exhausted-lookahead",
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

      const compiled = yield* prepareRuntimeRawQuery(
        "raw-visible-delete-exhausted-lookahead",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedQueryInterface, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["d", "c"]);
      const cursor = execution.createCursor();

      yield* deleteTopicStoreRow(store, "d");
      yield* deleteTopicStoreRow(store, "c");

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-visible-delete-exhausted-lookahead",
        queryId: "query",
        fromVersion: 4,
        toVersion: 6,
        operations: [
          {
            type: "remove",
            key: "d",
          },
          {
            type: "remove",
            key: "c",
          },
          {
            type: "insert",
            key: "b",
            row: {
              id: "b",
              score: 2,
            },
            index: 0,
          },
          {
            type: "insert",
            key: "a",
            row: {
              id: "a",
              score: 1,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      expect(scanLimits).toStrictEqual([3, 3]);

      yield* releaseRawQueryExecution(observedQueryInterface, compiled);
    }),
  );

  it.effect("filters batched retained removals once when a wider shared window has lookahead", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-visible-delete-shared-wide-lookahead",
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
      yield* publishTopicStoreRow(store, { id: "e", status: "open", score: 5 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "f", status: "open", score: 6 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const queryInterface = activeQueryTestInterface(store);
      const observedQueryInterface = {
        ...queryInterface,
        scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return queryInterface.scanRawWindow(plan);
        },
      };

      const wideCompiled = yield* prepareRuntimeRawQuery(
        "raw-visible-delete-shared-wide-lookahead",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 128,
        },
      );
      const narrowCompiled = yield* prepareRuntimeRawQuery(
        "raw-visible-delete-shared-wide-lookahead",
        activeQueryTestMetadata(store),
        {
          select: ["id", "score"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const wideExecution = yield* acquireRawQueryExecution(observedQueryInterface, wideCompiled);
      const narrowExecution = yield* acquireRawQueryExecution(
        observedQueryInterface,
        narrowCompiled,
      );
      expect(wideExecution.initial("wide-query").keys).toStrictEqual([
        "f",
        "e",
        "d",
        "c",
        "b",
        "a",
      ]);
      expect(narrowExecution.initial("query").keys).toStrictEqual(["f", "e"]);
      const cursor = narrowExecution.createCursor();

      yield* deleteTopicStoreRow(store, "f");
      yield* deleteTopicStoreRow(store, "e");

      const delta = yield* narrowExecution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-visible-delete-shared-wide-lookahead",
        queryId: "query",
        fromVersion: 6,
        toVersion: 8,
        operations: [
          {
            type: "remove",
            key: "f",
          },
          {
            type: "remove",
            key: "e",
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
          {
            type: "insert",
            key: "c",
            row: {
              id: "c",
              score: 3,
            },
            index: 1,
          },
        ],
        totalRows: 4,
      });
      expect(scanLimits).toStrictEqual([192]);

      yield* releaseRawQueryExecution(observedQueryInterface, narrowCompiled);
      yield* releaseRawQueryExecution(observedQueryInterface, wideCompiled);
    }),
  );

  it.effect(
    "preserves large retained lookahead when matching inserts safely refill a removal batch",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-visible-delete-insert-preserves-lookahead",
          Schema.Struct({
            id: Schema.String,
            status: Schema.String,
            score: Schema.Number,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRows(
          store,
          Array.from({ length: 220 }, (_value, index) => ({
            id: `row-${index}`,
            status: "open",
            score: index,
          })),
          invalidRow,
        );

        const scanLimits: Array<number | undefined> = [];
        const queryInterface = activeQueryTestInterface(store);
        const observedQueryInterface = {
          ...queryInterface,
          scanRawWindow: (plan: Parameters<typeof queryInterface.scanRawWindow>[0]) => {
            scanLimits.push(plan.limit);
            return queryInterface.scanRawWindow(plan);
          },
        };

        const wideCompiled = yield* prepareRuntimeRawQuery(
          "raw-visible-delete-insert-preserves-lookahead",
          activeQueryTestMetadata(store),
          {
            select: ["id", "score"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            orderBy: [{ field: "score", direction: "desc" }],
            limit: 128,
          },
        );
        const narrowCompiled = yield* prepareRuntimeRawQuery(
          "raw-visible-delete-insert-preserves-lookahead",
          activeQueryTestMetadata(store),
          {
            select: ["id", "score"],
            where: [{ field: "status", type: "equals", filter: "open" }],
            orderBy: [{ field: "score", direction: "desc" }],
            limit: 2,
          },
        );

        const wideExecution = yield* acquireRawQueryExecution(observedQueryInterface, wideCompiled);
        const narrowExecution = yield* acquireRawQueryExecution(
          observedQueryInterface,
          narrowCompiled,
        );
        expect(wideExecution.initial("wide-query").keys.slice(0, 3)).toStrictEqual([
          "row-219",
          "row-218",
          "row-217",
        ]);
        expect(narrowExecution.initial("query").keys).toStrictEqual(["row-219", "row-218"]);
        const cursor = narrowExecution.createCursor();

        yield* publishTopicStoreRows(
          store,
          [
            ...Array.from({ length: 16 }, (_value, offset) => ({
              id: `row-${219 - offset}`,
              status: "closed",
              score: 219 - offset,
            })),
            ...Array.from({ length: 16 }, (_value, offset) => ({
              id: `new-${offset}`,
              status: "open",
              score: 1_000 + offset,
            })),
          ],
          invalidRow,
        );
        const firstDelta = yield* narrowExecution.next("query", cursor);
        expect(Option.getOrThrow(firstDelta)).toStrictEqual({
          type: "delta",
          topic: "raw-visible-delete-insert-preserves-lookahead",
          queryId: "query",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "remove",
              key: "row-219",
            },
            {
              type: "remove",
              key: "row-218",
            },
            {
              type: "insert",
              key: "new-15",
              row: {
                id: "new-15",
                score: 1015,
              },
              index: 0,
            },
            {
              type: "insert",
              key: "new-14",
              row: {
                id: "new-14",
                score: 1014,
              },
              index: 1,
            },
          ],
          totalRows: 220,
        });

        yield* publishTopicStoreRows(
          store,
          Array.from({ length: 16 }, (_value, offset) => ({
            id: `new-${offset}`,
            status: "closed",
            score: 1_000 + offset,
          })),
          invalidRow,
        );
        const secondDelta = yield* narrowExecution.next("query", cursor);
        expect(Option.getOrThrow(secondDelta)).toStrictEqual({
          type: "delta",
          topic: "raw-visible-delete-insert-preserves-lookahead",
          queryId: "query",
          fromVersion: 2,
          toVersion: 3,
          operations: [
            {
              type: "remove",
              key: "new-15",
            },
            {
              type: "remove",
              key: "new-14",
            },
            {
              type: "insert",
              key: "row-203",
              row: {
                id: "row-203",
                score: 203,
              },
              index: 0,
            },
            {
              type: "insert",
              key: "row-202",
              row: {
                id: "row-202",
                score: 202,
              },
              index: 1,
            },
          ],
          totalRows: 204,
        });
        expect(scanLimits).toStrictEqual([192]);

        yield* releaseRawQueryExecution(observedQueryInterface, narrowCompiled);
        yield* releaseRawQueryExecution(observedQueryInterface, wideCompiled);
      }),
  );
});
