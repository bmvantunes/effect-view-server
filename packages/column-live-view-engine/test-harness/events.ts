import { expect } from "@effect/vitest";
import { type DeltaEvent, type SnapshotEvent, type StatusEvent } from "@effect-view-server/config";
import { Cause, Effect, Scope, Stream } from "effect";
import type { ColumnLiveViewEngineEvent, ColumnLiveViewSubscription } from "../src/index";

export const takeEvents = <Row>(
  subscription: ColumnLiveViewSubscription<Row>,
  count: number,
): Effect.Effect<ReadonlyArray<ColumnLiveViewEngineEvent<Row>>> =>
  subscription.events.pipe(Stream.take(count), Stream.runCollect);

export const makeEventReader = <Row>(
  subscription: ColumnLiveViewSubscription<Row>,
): Effect.Effect<
  (count: number) => Effect.Effect<ReadonlyArray<ColumnLiveViewEngineEvent<Row>>, Cause.Done>,
  never,
  Scope.Scope
> =>
  Effect.gen(function* () {
    const pull = yield* Stream.toPull(subscription.events);
    return (count) =>
      Effect.gen(function* () {
        const events: Array<ColumnLiveViewEngineEvent<Row>> = [];
        while (events.length < count) {
          const chunk = yield* pull;
          events.push(...chunk);
        }
        return events.slice(0, count);
      });
  });

export const collectEvents = <Row>(
  subscription: ColumnLiveViewSubscription<Row>,
): Effect.Effect<ReadonlyArray<ColumnLiveViewEngineEvent<Row>>> =>
  subscription.events.pipe(Stream.runCollect);

export const firstEvent = <Row>(
  events: ReadonlyArray<ColumnLiveViewEngineEvent<Row>>,
): ColumnLiveViewEngineEvent<Row> => {
  expect(events).not.toStrictEqual([]);
  return events[0]!;
};

export const expectSnapshotEvent: <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
) => asserts event is SnapshotEvent<Row> = (event) => {
  expect(event).toMatchObject({ type: "snapshot" });
};

export const expectDeltaEvent: <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
) => asserts event is DeltaEvent<Row> = (event) => {
  expect(event).toMatchObject({ type: "delta" });
};

export const expectStatusEvent: <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
) => asserts event is StatusEvent = (event) => {
  expect(event).toMatchObject({ type: "status" });
};

export const expectSnapshotRows = <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
  rows: ReadonlyArray<Row>,
) => {
  expectSnapshotEvent(event);
  expect(event.rows).toStrictEqual(rows);
};

export const expectDefined = <Value>(value: Value | undefined): Value => {
  expect(value).not.toBeUndefined();
  return value!;
};

export type ClientState<Row> = {
  readonly keys: ReadonlyArray<string>;
  readonly rows: ReadonlyArray<Row>;
};

export const applyDelta = <Row>(
  state: ClientState<Row>,
  event: DeltaEvent<Row>,
): ClientState<Row> => {
  const keys = [...state.keys];
  const rows = [...state.rows];

  for (const operation of event.operations) {
    if (operation.type === "remove") {
      const index = keys.indexOf(operation.key);
      expect(index).toBeGreaterThanOrEqual(0);
      keys.splice(index, 1);
      rows.splice(index, 1);
    }
    if (operation.type === "insert") {
      keys.splice(operation.index, 0, operation.key);
      rows.splice(operation.index, 0, operation.row);
    }
    if (operation.type === "update") {
      const index = keys.indexOf(operation.key);
      expect(index).toBeGreaterThanOrEqual(0);
      rows[index] = operation.row;
    }
    if (operation.type === "move") {
      const index = keys.indexOf(operation.key);
      expect(index).toBeGreaterThanOrEqual(0);
      const row = expectDefined(rows[index]);
      keys.splice(index, 1);
      rows.splice(index, 1);
      keys.splice(operation.toIndex, 0, operation.key);
      rows.splice(operation.toIndex, 0, row);
    }
  }

  return { keys, rows };
};

export const stateFromSnapshot = <Row>(event: ColumnLiveViewEngineEvent<Row>): ClientState<Row> => {
  expectSnapshotEvent(event);
  return {
    keys: event.keys,
    rows: event.rows,
  };
};

export const expectDeltaConverges = <Row>(
  state: ClientState<Row>,
  event: ColumnLiveViewEngineEvent<Row>,
  freshRows: ReadonlyArray<Row>,
): ClientState<Row> => {
  expectDeltaEvent(event);
  const nextState = applyDelta(state, event);
  expect(nextState.rows).toStrictEqual(freshRows);
  return nextState;
};
