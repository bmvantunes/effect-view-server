import { describe, expect, it } from "@effect/vitest";
import { Clock, Effect, Fiber, Option, Stream, Tracer } from "effect";
import { TestClock } from "effect/testing";
import { makeViewServerRuntimeCore } from "./index";
import { order, viewServer } from "./runtime-core-test-fixtures";

type RecordedSpan = {
  readonly attributes: ReadonlyArray<readonly [string, unknown]>;
  readonly name: string;
  readonly parentName: string | null;
  readonly parentSpanId: string | null;
  readonly spanId: string;
  readonly traceId: string;
};

const stableAttributes = (
  attributes: ReadonlyMap<string, unknown>,
): ReadonlyArray<readonly [string, unknown]> =>
  Array.from(attributes.entries()).sort(([left], [right]) => left.localeCompare(right));

const spanName = (span: Tracer.AnySpan): string => (span._tag === "Span" ? span.name : span.spanId);

const makeRecordingTracer = (): {
  readonly spans: Array<RecordedSpan>;
  readonly tracer: Tracer.Tracer;
} => {
  const spans: Array<RecordedSpan> = [];
  let nextSpanId = 0;
  const nextId = (): string => {
    nextSpanId += 1;
    return String(nextSpanId);
  };
  const tracer = Tracer.make({
    span: (options): Tracer.Span => {
      const id = nextId();
      const attributes = new Map<string, unknown>();
      const links = Array.from(options.links);
      let status: Tracer.SpanStatus = {
        _tag: "Started",
        startTime: options.startTime,
      };
      const span: Tracer.Span = {
        _tag: "Span",
        annotations: options.annotations,
        attribute: (key, value) => {
          attributes.set(key, value);
        },
        attributes,
        end: (endTime, exit) => {
          const parent = Option.getOrNull(options.parent);
          status = {
            _tag: "Ended",
            endTime,
            exit,
            startTime: status.startTime,
          };
          spans.push({
            attributes: stableAttributes(attributes),
            name: options.name,
            parentName: parent === null ? null : spanName(parent),
            parentSpanId: parent === null ? null : parent.spanId,
            spanId: span.spanId,
            traceId: span.traceId,
          });
        },
        event: () => {},
        addLinks: (newLinks) => {
          links.push(...newLinks);
        },
        get status() {
          return status;
        },
        kind: options.kind,
        links,
        name: options.name,
        parent: options.parent,
        sampled: options.sampled,
        spanId: `span-${id}`,
        traceId: Option.match(options.parent, {
          onNone: () => `trace-${id}`,
          onSome: (parent) => parent.traceId,
        }),
      };
      return span;
    },
  });
  return { spans, tracer };
};

describe("@effect-view-server/runtime-core", () => {
  it.effect("reports elapsed runtime uptime and passes refresh time to health overlays", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(10_000);
      const overlayTimes: Array<number> = [];
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health, nowMillis) => {
          overlayTimes.push(nowMillis);
          return health;
        },
      });

      expect(runtimeCore.liveClient.health.value.uptimeMs).toBe(0);
      expect(overlayTimes).toStrictEqual([10_000]);

      yield* TestClock.adjust("2500 millis");
      const refreshedHealth = yield* runtimeCore.refreshHealth;

      expect(refreshedHealth.uptimeMs).toBe(2_500);
      expect(overlayTimes).toStrictEqual([10_000, 12_500]);
      yield* runtimeCore.close;
    }),
  );

  it.effect("clamps runtime uptime when the clock moves before the runtime start", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(10_000);
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});

      yield* TestClock.setTime(9_000);
      const refreshedHealth = yield* runtimeCore.refreshHealth;

      expect(refreshedHealth.uptimeMs).toBe(0);
      yield* runtimeCore.close;
    }),
  );

  it.effect("uses monotonic time for uptime when wall time moves backward", () =>
    Effect.gen(function* () {
      let wallMillis = 10_000;
      let monotonicNanos = 5_000_000_000n;
      const clock: Clock.Clock = {
        currentTimeMillisUnsafe: () => wallMillis,
        currentTimeMillis: Effect.sync(() => wallMillis),
        currentTimeNanosUnsafe: () => monotonicNanos,
        currentTimeNanos: Effect.sync(() => monotonicNanos),
        sleep: () => Effect.void,
      };
      const overlayTimes: Array<number> = [];
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health, nowMillis) => {
          overlayTimes.push(nowMillis);
          return health;
        },
      }).pipe(Effect.provideService(Clock.Clock, clock));

      wallMillis = 9_000;
      monotonicNanos = 7_500_000_000n;
      const refreshedHealth = yield* runtimeCore.refreshHealth.pipe(
        Effect.provideService(Clock.Clock, clock),
      );

      expect(refreshedHealth.uptimeMs).toBe(2_500);
      expect(overlayTimes).toStrictEqual([10_000, 9_000]);
      yield* runtimeCore.close;
    }),
  );

  it.effect("records runtime core publish, engine mutation, and subscription fanout spans", () =>
    Effect.gen(function* () {
      const recording = makeRecordingTracer();
      const observedSpans = yield* Effect.scoped(
        Effect.gen(function* () {
          const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
          yield* Effect.addFinalizer(() => runtimeCore.close);
          const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
            select: ["id", "price"],
            orderBy: [{ field: "price", direction: "asc" }],
            limit: 10,
          });
          yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.orDie));

          const eventsFiber = yield* subscription.events.pipe(
            Stream.take(2),
            Stream.runCollect,
            Effect.forkChild,
          );
          yield* runtimeCore.client.publish("orders", order("a", 10));
          const events = yield* Fiber.join(eventsFiber);
          expect(events).toStrictEqual([
            {
              type: "snapshot",
              topic: "orders",
              queryId: "query-0",
              version: 0,
              keys: [],
              rows: [],
              totalRows: 0,
            },
            {
              type: "delta",
              topic: "orders",
              queryId: "query-0",
              fromVersion: 0,
              toVersion: 1,
              operations: [
                {
                  type: "insert",
                  key: "a",
                  row: {
                    id: "a",
                    price: 10,
                  },
                  index: 0,
                },
              ],
              totalRows: 1,
            },
          ]);

          return recording.spans;
        }),
      ).pipe(Effect.provideService(Tracer.Tracer, recording.tracer));

      const spansByName = new Map(observedSpans.map((span) => [span.name, span]));
      const clientPublish = spansByName.get("ViewServerRuntimeCore.client.publish");
      const sourceMutationApply = spansByName.get("ViewServerRuntimeCore.sourceMutation.apply");
      const publish = spansByName.get("ColumnLiveViewEngine.publish");
      const topicStorePublish = spansByName.get("ColumnLiveViewEngine.topicStore.publish");
      const mutationTransaction = spansByName.get(
        "ColumnLiveViewEngine.topicStore.mutationTransaction",
      );
      const mutationBatch = spansByName.get("ColumnLiveViewEngine.topicStore.mutationBatch");
      const notify = spansByName.get("ColumnLiveViewEngine.topicStore.notify");
      const liveSubscriptionNotify = spansByName.get(
        "ColumnLiveViewEngine.liveSubscription.notify",
      );

      expect({
        clientPublish: {
          name: clientPublish?.name,
          parentSpanId: clientPublish?.parentSpanId,
          traceId: clientPublish?.traceId,
        },
        liveSubscriptionNotify: {
          attributes: liveSubscriptionNotify?.attributes,
          name: liveSubscriptionNotify?.name,
          parentName: liveSubscriptionNotify?.parentName,
          parentSpanId: liveSubscriptionNotify?.parentSpanId,
          traceId: liveSubscriptionNotify?.traceId,
        },
        mutationBatch: {
          name: mutationBatch?.name,
          parentName: mutationBatch?.parentName,
          parentSpanId: mutationBatch?.parentSpanId,
          traceId: mutationBatch?.traceId,
        },
        mutationTransaction: {
          name: mutationTransaction?.name,
          parentName: mutationTransaction?.parentName,
          parentSpanId: mutationTransaction?.parentSpanId,
          traceId: mutationTransaction?.traceId,
        },
        notify: {
          name: notify?.name,
          parentName: notify?.parentName,
          parentSpanId: notify?.parentSpanId,
          traceId: notify?.traceId,
        },
        publish: {
          name: publish?.name,
          parentName: publish?.parentName,
          parentSpanId: publish?.parentSpanId,
          traceId: publish?.traceId,
        },
        sourceMutationApply: {
          name: sourceMutationApply?.name,
          parentName: sourceMutationApply?.parentName,
          parentSpanId: sourceMutationApply?.parentSpanId,
          traceId: sourceMutationApply?.traceId,
        },
        topicStorePublish: {
          name: topicStorePublish?.name,
          parentName: topicStorePublish?.parentName,
          parentSpanId: topicStorePublish?.parentSpanId,
          traceId: topicStorePublish?.traceId,
        },
      }).toStrictEqual({
        clientPublish: {
          name: "ViewServerRuntimeCore.client.publish",
          parentSpanId: null,
          traceId: clientPublish?.traceId,
        },
        liveSubscriptionNotify: {
          attributes: [
            ["queryId", "query-0"],
            ["topic", "orders"],
          ],
          name: "ColumnLiveViewEngine.liveSubscription.notify",
          parentName: "ColumnLiveViewEngine.topicStore.notify",
          parentSpanId: notify?.spanId,
          traceId: clientPublish?.traceId,
        },
        mutationBatch: {
          name: "ColumnLiveViewEngine.topicStore.mutationBatch",
          parentName: "ColumnLiveViewEngine.topicStore.mutationTransaction",
          parentSpanId: mutationTransaction?.spanId,
          traceId: clientPublish?.traceId,
        },
        mutationTransaction: {
          name: "ColumnLiveViewEngine.topicStore.mutationTransaction",
          parentName: "ColumnLiveViewEngine.topicStore.publish",
          parentSpanId: topicStorePublish?.spanId,
          traceId: clientPublish?.traceId,
        },
        notify: {
          name: "ColumnLiveViewEngine.topicStore.notify",
          parentName: "ColumnLiveViewEngine.topicStore.mutationBatch",
          parentSpanId: mutationBatch?.spanId,
          traceId: clientPublish?.traceId,
        },
        publish: {
          name: "ColumnLiveViewEngine.publish",
          parentName: "ViewServerRuntimeCore.sourceMutation.apply",
          parentSpanId: sourceMutationApply?.spanId,
          traceId: clientPublish?.traceId,
        },
        sourceMutationApply: {
          name: "ViewServerRuntimeCore.sourceMutation.apply",
          parentName: "ViewServerRuntimeCore.client.publish",
          parentSpanId: clientPublish?.spanId,
          traceId: clientPublish?.traceId,
        },
        topicStorePublish: {
          name: "ColumnLiveViewEngine.topicStore.publish",
          parentName: "ColumnLiveViewEngine.publish",
          parentSpanId: publish?.spanId,
          traceId: clientPublish?.traceId,
        },
      });
    }),
  );
});
