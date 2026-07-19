import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  applyDelta,
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";
import { createColumnLiveViewEngine } from "./index";

const NumberRow = Schema.Struct({
  id: Schema.String,
  value: Schema.Number,
});

const numberViewServer = defineViewServerConfig({
  topics: {
    numberRows: {
      schema: NumberRow,
      key: "id",
    },
  },
});

const orderedIdQuery = {
  select: ["id"],
  orderBy: [{ field: "id", direction: "asc" }],
} satisfies {
  readonly select: readonly ["id"];
  readonly orderBy: readonly [{ readonly field: "id"; readonly direction: "asc" }];
};

const makeNumberEngine = () =>
  createColumnLiveViewEngine({
    topics: numberViewServer.topics,
  });

describe("ColumnLiveViewEngine Schema.Number zero semantics", () => {
  it.effect("canonicalizes selected negative zero in one-shot and subscription snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeNumberEngine();
      yield* engine.publish("numberRows", { id: "negative-zero", value: -0 });

      const query = {
        select: ["id", "value"],
      } as const;
      const oneShot = yield* engine.snapshot("numberRows", query);
      const subscription = yield* engine.subscribe("numberRows", query);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);

      expect(oneShot.rows).toStrictEqual([{ id: "negative-zero", value: 0 }]);
      expect(initial.rows).toStrictEqual([{ id: "negative-zero", value: 0 }]);
      expect(Object.is(oneShot.rows[0]?.value, -0)).toBe(false);
      expect(Object.is(initial.rows[0]?.value, -0)).toBe(false);

      yield* subscription.close();
      yield* engine.close();
    }),
  );

  it.effect("normalizes positive and negative zero across indexed eq, neq, and in snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeNumberEngine();
      yield* engine.publishMany("numberRows", [
        { id: "negative-zero", value: -0 },
        { id: "non-zero", value: 1 },
        { id: "positive-zero", value: 0 },
      ]);

      const positiveEq = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "equals", filter: 0 }],
      });
      const negativeEq = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "equals", filter: -0 }],
      });
      const positiveNeq = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "notEqual", filter: 0 }],
      });
      const negativeNeq = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "notEqual", filter: -0 }],
      });
      const positiveIn = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "in", filter: [0] }],
      });
      const negativeIn = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "in", filter: [-0] }],
      });

      const zeroSnapshot = {
        rows: [{ id: "negative-zero" }, { id: "positive-zero" }],
        totalRows: 2,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      };
      const nonZeroSnapshot = {
        rows: [{ id: "non-zero" }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      };

      expect({
        positiveEq,
        negativeEq,
        positiveNeq,
        negativeNeq,
        positiveIn,
        negativeIn,
      }).toStrictEqual({
        positiveEq: zeroSnapshot,
        negativeEq: zeroSnapshot,
        positiveNeq: nonZeroSnapshot,
        negativeNeq: nonZeroSnapshot,
        positiveIn: zeroSnapshot,
        negativeIn: zeroSnapshot,
      });

      yield* engine.close();
    }),
  );

  it.effect("treats sign-only zero replacements as no-ops and keeps live views converged", () =>
    Effect.gen(function* () {
      const engine = yield* makeNumberEngine();
      yield* engine.publishMany("numberRows", [
        { id: "negative-zero", value: -0 },
        { id: "non-zero", value: 1 },
        { id: "positive-zero", value: 0 },
      ]);

      const inSubscription = yield* engine.subscribe("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "in", filter: [-0] }],
      });
      const readIn = yield* makeEventReader(inSubscription);
      const inInitial = firstEvent(yield* readIn(1));
      expectSnapshotEvent(inInitial);
      let inState = stateFromSnapshot(inInitial);

      expect(inInitial).toStrictEqual({
        type: "snapshot",
        topic: "numberRows",
        queryId: inInitial.queryId,
        version: 1,
        keys: ["negative-zero", "positive-zero"],
        rows: [{ id: "negative-zero" }, { id: "positive-zero" }],
        totalRows: 2,
      });

      yield* engine.publishMany("numberRows", [
        { id: "negative-zero", value: 0 },
        { id: "positive-zero", value: -0 },
      ]);

      const afterSignOnlyReplacement = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "in", filter: [0] }],
      });
      const noOpHealth = yield* engine.health();
      expect(afterSignOnlyReplacement).toStrictEqual({
        rows: [{ id: "negative-zero" }, { id: "positive-zero" }],
        totalRows: 2,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect({
        version: noOpHealth.version,
        queuedEvents: noOpHealth.queuedEvents,
      }).toStrictEqual({
        version: 1,
        queuedEvents: 0,
      });

      yield* engine.publish("numberRows", { id: "positive-zero", value: 2 });
      const inRemoved = firstEvent(yield* readIn(1));
      expectDeltaEvent(inRemoved);
      expect(inRemoved).toStrictEqual({
        type: "delta",
        topic: "numberRows",
        queryId: inInitial.queryId,
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "remove", key: "positive-zero" }],
        totalRows: 1,
      });
      inState = applyDelta(inState, inRemoved);

      const equalAfterChange = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "equals", filter: 0 }],
      });
      const notEqualAfterChange = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "notEqual", filter: -0 }],
      });
      const inAfterChange = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "in", filter: [0] }],
      });
      expect(inState).toStrictEqual({
        keys: ["negative-zero"],
        rows: inAfterChange.rows,
      });
      expect({ equalAfterChange, notEqualAfterChange, inAfterChange }).toStrictEqual({
        equalAfterChange: {
          rows: [{ id: "negative-zero" }],
          totalRows: 1,
          version: 2,
          status: "ready",
          statusCode: "Ready",
        },
        notEqualAfterChange: {
          rows: [{ id: "non-zero" }, { id: "positive-zero" }],
          totalRows: 2,
          version: 2,
          status: "ready",
          statusCode: "Ready",
        },
        inAfterChange: {
          rows: [{ id: "negative-zero" }],
          totalRows: 1,
          version: 2,
          status: "ready",
          statusCode: "Ready",
        },
      });

      yield* engine.publish("numberRows", { id: "positive-zero", value: -0 });
      const inInserted = firstEvent(yield* readIn(1));
      expectDeltaEvent(inInserted);
      expect(inInserted).toStrictEqual({
        type: "delta",
        topic: "numberRows",
        queryId: inInitial.queryId,
        fromVersion: 2,
        toVersion: 3,
        operations: [
          {
            type: "insert",
            key: "positive-zero",
            row: { id: "positive-zero" },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      inState = applyDelta(inState, inInserted);

      const restoredPositiveEq = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "equals", filter: 0 }],
      });
      const restoredNegativeEq = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "equals", filter: -0 }],
      });
      expect({ inState, restoredPositiveEq, restoredNegativeEq }).toStrictEqual({
        inState: {
          keys: ["negative-zero", "positive-zero"],
          rows: restoredPositiveEq.rows,
        },
        restoredPositiveEq: {
          rows: [{ id: "negative-zero" }, { id: "positive-zero" }],
          totalRows: 2,
          version: 3,
          status: "ready",
          statusCode: "Ready",
        },
        restoredNegativeEq: {
          rows: [{ id: "negative-zero" }, { id: "positive-zero" }],
          totalRows: 2,
          version: 3,
          status: "ready",
          statusCode: "Ready",
        },
      });

      yield* engine.publish("numberRows", { id: "positive-zero", value: 0 });
      const finalEqual = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "equals", filter: -0 }],
      });
      const finalNotEqual = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "notEqual", filter: 0 }],
      });
      const finalIn = yield* engine.snapshot("numberRows", {
        ...orderedIdQuery,
        where: [{ field: "value", type: "in", filter: [-0] }],
      });
      const finalHealth = yield* engine.health();
      expect(inState).toStrictEqual({
        keys: ["negative-zero", "positive-zero"],
        rows: finalIn.rows,
      });
      expect({ finalEqual, finalNotEqual, finalIn }).toStrictEqual({
        finalEqual: {
          rows: [{ id: "negative-zero" }, { id: "positive-zero" }],
          totalRows: 2,
          version: 3,
          status: "ready",
          statusCode: "Ready",
        },
        finalNotEqual: {
          rows: [{ id: "non-zero" }],
          totalRows: 1,
          version: 3,
          status: "ready",
          statusCode: "Ready",
        },
        finalIn: {
          rows: [{ id: "negative-zero" }, { id: "positive-zero" }],
          totalRows: 2,
          version: 3,
          status: "ready",
          statusCode: "Ready",
        },
      });
      expect({
        version: finalHealth.version,
        queuedEvents: finalHealth.queuedEvents,
      }).toStrictEqual({
        version: 3,
        queuedEvents: 0,
      });

      yield* inSubscription.close();
      yield* engine.close();
    }),
  );
});
