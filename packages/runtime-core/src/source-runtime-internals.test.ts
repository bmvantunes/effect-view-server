import { describe, expect, it } from "@effect/vitest";
import type { ViewServerLiveEvent, ViewServerLiveSubscription } from "@effect-view-server/client";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Deferred, Effect, Result, Schema, SchemaGetter, Stream } from "effect";
import { make as makeBigDecimal } from "effect/BigDecimal";
import {
  sourceLeaseTerminalObserver,
  sourceRuntimeInternals,
  type SourceRuntimeRouteEntry,
} from "./source-runtime";
import { healthFromEngine } from "./health";
import { engineHealth } from "./test-support/runtime-test-fixtures";

const Failure = Schema.TaggedStruct("RouteTestFailure", {
  message: Schema.String,
});
const Declaration = {
  metrics: Schema.Struct({ observed: Schema.BigInt }),
  rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
  definitionOptions: SourceAdapter.definitionOptions<void>(),
};
const adapter = SourceAdapter.make({
  identity: { name: "route-test" },
  failure: Failure,
  materialized: undefined,
  leased: Declaration,
});
const Row = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  amount: Schema.BigDecimal,
});
const definition = adapter.leasedSource(["region", "amount"], undefined);
const entry: SourceRuntimeRouteEntry = {
  topic: "rows",
  schema: Row,
  definition,
};

describe("Source Runtime internal contracts", () => {
  it("compares exact route scalars including BigDecimal storage identity", () => {
    expect(
      sourceRuntimeInternals.equalRouteValue(makeBigDecimal(100n, 2), makeBigDecimal(100n, 2)),
    ).toBe(true);
    expect(
      sourceRuntimeInternals.equalRouteValue(makeBigDecimal(100n, 2), makeBigDecimal(10n, 1)),
    ).toBe(false);
    expect(sourceRuntimeInternals.equalRouteValue(makeBigDecimal(100n, 2), "1.00")).toBe(false);
    expect(sourceRuntimeInternals.equalRouteValue("eu", "eu")).toBe(true);
    expect(sourceRuntimeInternals.equalRouteValue(0, -0)).toBe(false);
  });

  it("accepts only exact own enumerable schema-valid route fields", () => {
    const valid = sourceRuntimeInternals.exactRoute(entry, {
      region: "eu",
      amount: makeBigDecimal(123n, 2),
    });
    expect(Result.isSuccess(valid)).toBe(true);
    const route = Result.getOrThrow(valid);
    expect({
      route,
      frozen: Object.isFrozen(route),
    }).toStrictEqual({
      route: {
        region: "eu",
        amount: makeBigDecimal(123n, 2),
      },
      frozen: true,
    });

    const throwingKeys = new Proxy(
      {
        region: "eu",
        amount: makeBigDecimal(123n, 2),
      },
      {
        ownKeys: () => {
          throw new Error("ownKeys failed");
        },
      },
    );
    const throwingDescriptor = new Proxy(
      {
        region: "eu",
        amount: makeBigDecimal(123n, 2),
      },
      {
        getOwnPropertyDescriptor: (target, property) => {
          if (property === "region") {
            throw new Error("descriptor failed");
          }
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      },
    );
    const nonEnumerable: Record<string, unknown> = {
      amount: makeBigDecimal(123n, 2),
    };
    Object.defineProperty(nonEnumerable, "region", {
      value: "eu",
    });
    const invalid = [
      null,
      [],
      "eu",
      {},
      {
        region: "eu",
        amount: makeBigDecimal(123n, 2),
        extra: true,
      },
      {
        region: 1,
        amount: makeBigDecimal(123n, 2),
      },
      throwingKeys,
      throwingDescriptor,
      nonEnumerable,
    ];
    expect(
      invalid.every((candidate) =>
        Result.isFailure(sourceRuntimeInternals.exactRoute(entry, candidate)),
      ),
    ).toBe(true);
  });

  it("builds stable route identities and reports unsupported encodings", () => {
    const route = {
      region: "eu",
      amount: makeBigDecimal(123n, 2),
    };
    const identity = sourceRuntimeInternals.feedKeyFor(entry, route);
    expect(Result.isSuccess(identity)).toBe(true);

    const missingFields = new Proxy(Row, {
      get: (target, property, receiver) =>
        property === "fields"
          ? { id: Schema.String, region: Schema.String }
          : Reflect.get(target, property, receiver),
    });
    expect(
      Result.isFailure(
        sourceRuntimeInternals.feedKeyFor({ ...entry, schema: missingFields }, route),
      ),
    ).toBe(true);

    const unsupportedField = new Proxy(Schema.String, {
      get: (target, property, receiver) => {
        if (property === "ast") {
          throw new Error("unsupported schema");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const unsupportedSchema = new Proxy(Row, {
      get: (target, property, receiver) =>
        property === "fields"
          ? {
              id: Schema.String,
              region: unsupportedField,
              amount: Schema.BigDecimal,
            }
          : Reflect.get(target, property, receiver),
    });
    expect(
      Result.isFailure(
        sourceRuntimeInternals.feedKeyFor({ ...entry, schema: unsupportedSchema }, route),
      ),
    ).toBe(true);

    const Unsafe = Schema.String.pipe(
      Schema.encodeTo(Schema.Any, {
        decode: SchemaGetter.transform((value) => (typeof value === "string" ? value : "decoded")),
        encode: SchemaGetter.transform(() => Symbol("not-json")),
      }),
    );
    const unsafeSchema = new Proxy(Row, {
      get: (target, property, receiver) =>
        property === "fields"
          ? {
              id: Schema.String,
              region: Unsafe,
              amount: Schema.BigDecimal,
            }
          : Reflect.get(target, property, receiver),
    });
    expect(
      Result.isFailure(
        sourceRuntimeInternals.feedKeyFor({ ...entry, schema: unsafeSchema }, route),
      ),
    ).toBe(true);
  });

  it.effect("translates raw snapshot and every delta operation back to public ids", () =>
    Effect.gen(function* () {
      type Row = {
        readonly id: string;
        readonly value: string;
      };
      const stored = (id: string) => sourceRuntimeInternals.internalStorageKey("rows", "feed", id);
      const throwingRow: Row = {
        id: "initial",
        value: "throwing",
      };
      Object.defineProperty(throwingRow, "id", {
        enumerable: true,
        get: () => {
          throw new Error("id failed");
        },
      });
      const events: ReadonlyArray<ViewServerLiveEvent<Row>> = [
        {
          type: "snapshot",
          topic: "rows",
          queryId: "query",
          version: 1,
          keys: [stored("public-a"), "storage:b", "storage:missing"],
          rows: [
            { id: "row-a", value: "a" },
            { id: "row-b", value: "b" },
          ],
          totalRows: 2,
        },
        {
          type: "delta",
          topic: "rows",
          queryId: "query",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "insert",
              key: stored("public-a"),
              row: { id: "row-a", value: "inserted" },
              index: 0,
            },
            {
              type: "update",
              key: "storage:b",
              row: { id: "row-b", value: "updated" },
              index: 1,
            },
            {
              type: "move",
              key: stored("public-c"),
              fromIndex: 2,
              toIndex: 0,
            },
            {
              type: "remove",
              key: stored("public-d"),
            },
            {
              type: "insert",
              key: "storage:throwing",
              row: throwingRow,
              index: 2,
            },
          ],
          totalRows: 2,
        },
        {
          type: "status",
          topic: "rows",
          queryId: "query",
          status: "ready",
          code: "Ready",
          message: "ready",
        },
      ];
      const subscription: ViewServerLiveSubscription<Row> = {
        events: Stream.fromIterable(events),
        close: () => Effect.void,
      };
      const translated = sourceRuntimeInternals.translateSubscription(subscription, {
        select: ["id"],
      });
      const translatedEvents = yield* translated.events.pipe(Stream.runCollect);
      expect(Array.from(translatedEvents)).toStrictEqual([
        {
          ...events[0],
          keys: ["public-a", "row-b", "storage:missing"],
        },
        {
          ...events[1],
          operations: [
            {
              type: "insert",
              key: "public-a",
              row: { id: "row-a", value: "inserted" },
              index: 0,
            },
            {
              type: "update",
              key: "row-b",
              row: { id: "row-b", value: "updated" },
              index: 1,
            },
            {
              type: "move",
              key: "public-c",
              fromIndex: 2,
              toIndex: 0,
            },
            {
              type: "remove",
              key: "public-d",
            },
            {
              type: "insert",
              key: "storage:throwing",
              row: throwingRow,
              index: 2,
            },
          ],
        },
        events[2],
      ]);
      expect(
        sourceRuntimeInternals.translateSubscription(subscription, { groupBy: ["value"] }),
      ).toBe(subscription);
      expect(sourceRuntimeInternals.internalPublicId("ordinary")).toBeUndefined();
      expect(sourceRuntimeInternals.internalPublicId("source/x")).toBeUndefined();
      expect(sourceRuntimeInternals.internalPublicId("source/rows/feed/%")).toBeUndefined();
    }),
  );

  it("maps every Source status branch to live-query availability", () => {
    const termination = {
      _tag: "UnexpectedCompletion",
    } as const;
    const statuses = [
      {
        _tag: "Starting",
        attempt: 1n,
        startedAtNanos: 1n,
      },
      {
        _tag: "Ready",
        attempt: 1n,
        readyAtNanos: 1n,
      },
      {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 1n,
        latestRejection: {
          failure: {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidSourceDelivery",
              message: "rejected",
            },
          },
          location: { offset: 1n },
          rejectedAtNanos: 1n,
        },
      },
      {
        _tag: "WaitingToRetry",
        nextAttempt: 2n,
        termination,
        retryAtNanos: 2n,
      },
      {
        _tag: "Reacquiring",
        previousTermination: termination,
        attempt: 2n,
        startedAtNanos: 2n,
      },
      {
        _tag: "Exhausted",
        exhaustion: {
          _tag: "RetryExhausted",
          lastTermination: termination,
        },
        exhaustedAtNanos: 3n,
      },
      {
        _tag: "Stopping",
        reason: "runtime-shutdown",
        stoppingAtNanos: 4n,
      },
    ] as const;
    expect(
      statuses.map((status) => {
        const event = sourceRuntimeInternals.sourceAvailabilityEvent("rows", "query", status);
        return `${event.status}:${event.code}:${event.message}`;
      }),
    ).toStrictEqual([
      "stale:SnapshotStale:Source is starting; retained rows may be incomplete.",
      "ready:Ready:Source is ready.",
      "ready:Ready:Source delivery continues with one or more settled item rejections.",
      "stale:SnapshotStale:Source is retrying; retained rows may be stale.",
      "stale:SnapshotStale:Source is retrying; retained rows may be stale.",
      "error:RuntimeUnavailable:Source retries are exhausted; retained rows are preserved.",
      "error:RuntimeUnavailable:Source is stopping.",
    ]);
  });

  it("combines repeated Source, engine, and runtime health states by severity", () => {
    const ready = {
      _tag: "Ready",
      attempt: 1n,
      readyAtNanos: 1n,
    } as const;
    const starting = {
      _tag: "Starting",
      attempt: 1n,
      startedAtNanos: 1n,
    } as const;
    const reacquiring = {
      _tag: "Reacquiring",
      previousTermination: {
        _tag: "UnexpectedCompletion",
      },
      attempt: 2n,
      startedAtNanos: 2n,
    } as const;
    const stopping = {
      _tag: "Stopping",
      reason: "runtime-shutdown",
      stoppingAtNanos: 3n,
    } as const;
    const base = healthFromEngine(engineHealth("ready", 0));
    const engineStarting = {
      ...base,
      engine: {
        topics: {
          orders: {
            ...base.engine.topics.orders,
            status: "starting" as const,
          },
        },
      },
    };
    const runtimeStarting = {
      ...base,
      status: "starting" as const,
    };
    const runtimeDegraded = {
      ...base,
      status: "degraded" as const,
    };
    const runtimeStopping = {
      ...base,
      status: "stopping" as const,
    };
    const engineDegraded = {
      ...healthFromEngine(engineHealth("stopping", 0)),
      status: "ready" as const,
    };
    const repeated = [
      { topic: "orders", status: ready },
      { topic: "orders", status: starting },
      { topic: "orders", status: ready },
      { topic: "orders", status: stopping },
      { topic: "orders", status: reacquiring },
      { topic: "missing", status: ready },
    ];

    expect([
      sourceRuntimeInternals.overlaySourceHealth(base, []).status,
      sourceRuntimeInternals.overlaySourceHealth(base, [{ topic: "orders", status: ready }]).engine
        .topics.orders.status,
      sourceRuntimeInternals.overlaySourceHealth(engineStarting, [
        { topic: "orders", status: ready },
      ]).engine.topics.orders.status,
      sourceRuntimeInternals.overlaySourceHealth(engineDegraded, [
        { topic: "orders", status: ready },
      ]).engine.topics.orders.status,
      sourceRuntimeInternals.overlaySourceHealth(base, [{ topic: "orders", status: starting }])
        .status,
      sourceRuntimeInternals.overlaySourceHealth(base, [{ topic: "orders", status: stopping }])
        .status,
      sourceRuntimeInternals.overlaySourceHealth(runtimeStarting, []).status,
      sourceRuntimeInternals.overlaySourceHealth(runtimeDegraded, []).status,
      sourceRuntimeInternals.overlaySourceHealth(runtimeStopping, []).status,
      sourceRuntimeInternals.overlaySourceHealth(base, repeated).status,
    ]).toStrictEqual([
      "ready",
      "ready",
      "starting",
      "degraded",
      "starting",
      "degraded",
      "starting",
      "degraded",
      "stopping",
      "degraded",
    ]);
  });

  it.effect("atomically hands an invalid metrics sample to the next attempt", () =>
    Effect.gen(function* () {
      const observation = sourceRuntimeInternals.makeMetricFailureObservation();
      const signal =
        yield* Deferred.make<
          import("@effect-view-server/source-adapter").SourceExecutionFailure<unknown>
        >();
      const failure = {
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceMetrics",
          message: "invalid sample during attempt handoff",
        },
      } as const;

      yield* observation.record(Result.fail(failure));
      const registration = yield* observation.register(signal);

      expect(registration).toStrictEqual({
        _tag: "Failed",
        failure,
      });

      const activeObservation = sourceRuntimeInternals.makeMetricFailureObservation();
      const activeSignal =
        yield* Deferred.make<
          import("@effect-view-server/source-adapter").SourceExecutionFailure<unknown>
        >();
      const unrelatedSignal =
        yield* Deferred.make<
          import("@effect-view-server/source-adapter").SourceExecutionFailure<unknown>
        >();
      expect(yield* activeObservation.register(activeSignal)).toStrictEqual({
        _tag: "Registered",
      });
      yield* activeObservation.unregister(unrelatedSignal);
      yield* activeObservation.record(Result.fail(failure));
      expect(yield* Deferred.await(activeSignal)).toStrictEqual(failure);
      yield* activeObservation.unregister(activeSignal);
    }),
  );

  it.effect("provides infallible terminal observer no-ops", () =>
    Effect.all(
      [
        sourceLeaseTerminalObserver.onQueryRegistered("query"),
        sourceLeaseTerminalObserver.onTerminalOccurrence({
          type: "status",
          topic: "rows",
          queryId: "query",
          status: "error",
          code: "RuntimeUnavailable",
        }),
        sourceLeaseTerminalObserver.onTerminalReady({
          type: "status",
          topic: "rows",
          queryId: "query",
          status: "error",
          code: "RuntimeUnavailable",
        }),
      ],
      { discard: true },
    ),
  );
});
