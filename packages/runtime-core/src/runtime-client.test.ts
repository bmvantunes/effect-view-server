import { describe, expect, it } from "@effect/vitest";
import { createColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import { Deferred, Effect, Fiber, Stream, Tracer } from "effect";
import { healthFromEngine } from "./health";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { createViewServerRuntimeCore, makeViewServerRuntimeCore } from "./index";
import { makeRuntimeCoreClient } from "./runtime-client";
import { makeRecordingTracer, order, viewServer } from "./test-support/runtime-test-fixtures";

describe("Runtime Core client", () => {
  it.effect("starts a new fresh health read after a completed mutation", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      let readCount = 0;
      const readFreshHealth = Effect.gen(function* () {
        const readNumber = readCount + 1;
        readCount = readNumber;
        const health = healthFromEngine(yield* engine.health());
        if (readNumber === 1) {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
        }
        return health;
      });
      const runtimeClient = yield* makeRuntimeCoreClient(
        viewServer,
        engine,
        readFreshHealth,
        Effect.void,
      );
      const firstHealthFiber = yield* runtimeClient.client
        .health()
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(firstReadStarted);

      yield* runtimeClient.client.publish("orders", order("mutation", 10));
      const secondHealthFiber = yield* runtimeClient.client
        .health()
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(readCount).toBe(2);
      yield* Deferred.succeed(releaseFirstRead, undefined);
      const [firstHealth, secondHealth] = yield* Effect.all(
        [Fiber.join(firstHealthFiber), Fiber.join(secondHealthFiber)],
        { concurrency: 2 },
      );
      expect({
        firstRowCount: firstHealth.engine.topics.orders.rowCount,
        secondRowCount: secondHealth.engine.topics.orders.rowCount,
      }).toStrictEqual({
        firstRowCount: 0,
        secondRowCount: 1,
      });
      yield* engine.close();
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

  it.effect("supports the synchronous runtime core constructor", () =>
    Effect.gen(function* () {
      const runtimeCore = createViewServerRuntimeCore(viewServer, { subscriptionQueueCapacity: 1 });
      expect("set" in runtimeCore.liveClient.health).toBe(false);
      expect(runtimeCore.liveClient.health.value.status).toBe("ready");

      yield* runtimeCore.client.publish("orders", order("a", 10));
      const health = yield* runtimeCore.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      expect(health.transport).toStrictEqual({
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.effect("supports the synchronous runtime core constructor defaults", () =>
    Effect.gen(function* () {
      const runtimeCore = createViewServerRuntimeCore(viewServer);

      yield* runtimeCore.client.publish("orders", order("a", 10));
      const health = yield* runtimeCore.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      yield* runtimeCore.close;
    }),
  );

  it.effect("forwards grouped admission limits to the engine", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
      });
      yield* runtimeCore.client.publishMany("orders", [order("a", 10), order("b", 20)]);
      const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
        groupBy: ["price"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      const health = yield* runtimeCore.client.health();
      expect(health.engine.topics.orders.activeFallbackGroupedViews).toBe(1);
      expect(health.engine.topics.orders.activeIncrementalGroupedViews).toBe(0);

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("subscribes through the runtime live-client entrypoint", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.internalClient.publish("orders", order("a", 10));

      const subscription = yield* runtimeCore.liveClient.subscribeRuntime("orders", {
        select: ["id", "price"],
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", price: 10 }],
        totalRows: 1,
      });

      yield* subscription.close();
      const health = yield* runtimeCore.internalClient.health();
      expect(health.engine.topics.orders.activeSubscriptions).toBe(0);
      yield* runtimeCore.close;
    }),
  );

  it.effect("forwards producer terminal observation through the internal live client", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {
        subscriptionQueueCapacity: 1,
      });
      const phases: Array<string> = [];
      const subscription = yield* runtimeCore.internalLiveClient.subscribeObservedInternal(
        "orders",
        {
          select: ["id"],
          limit: 10,
        },
        {
          onQueryRegistered: (queryId) =>
            Effect.sync(() => {
              phases.push(`registered:${queryId}`);
            }),
          onTerminalOccurrence: () =>
            Effect.sync(() => {
              phases.push("occurrence");
            }),
          onTerminalReady: () =>
            Effect.sync(() => {
              phases.push("ready");
            }),
        },
      );

      yield* runtimeCore.internalClient.publish("orders", order("observed", 10));
      const events = yield* subscription.events.pipe(Stream.runCollect);
      expect(phases).toStrictEqual(["registered:query-0", "occurrence", "ready"]);
      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "query-0",
          status: "closed",
          code: "BackpressureExceeded",
          message: "Subscription closed because its event queue exceeded capacity.",
        },
      ]);

      const runtimeQueryIds: Array<string> = [];
      const runtimeSubscription =
        yield* runtimeCore.internalLiveClient.subscribeRuntimeObservedInternal(
          "orders",
          { select: ["id"] },
          {
            onQueryRegistered: (queryId) =>
              Effect.sync(() => {
                runtimeQueryIds.push(queryId);
              }),
            onTerminalOccurrence: () => Effect.void,
            onTerminalReady: () => Effect.void,
          },
        );
      expect(runtimeQueryIds).toStrictEqual(["query-1"]);
      yield* runtimeSubscription.close();
      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("maps engine errors into runtime errors", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* runtimeCore.client.publish("orders", order("a", 10));

      const invalidTopic = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown topics.
        runtimeCore.client.publish("missing", order("b", 20)),
      );
      const invalidRow = yield* Effect.flip(
        runtimeCore.client.publish("orders", {
          id: "bad",
          customerId: "customer-bad",
          // @ts-expect-error hostile runtime callers can still send malformed rows.
          status: "unknown",
          price: 20,
          region: "usa",
          updatedAt: 20,
        }),
      );
      const groupedQuery = yield* runtimeCore.client.snapshot("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
      });
      const invalidQuery = yield* Effect.flip(
        runtimeCore.client.snapshot("orders", {
          // @ts-expect-error hostile runtime callers can still send unknown projected fields.
          select: ["prcie"],
        }),
      );

      yield* runtimeCore.close;
      const runtimeUnavailable = yield* Effect.flip(
        runtimeCore.client.publish("orders", order("closed", 30)),
      );

      expect(invalidTopic.code).toBe("InvalidTopic");
      expect(invalidRow.code).toBe("InvalidRow");
      expect(groupedQuery.rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(invalidQuery.code).toBe("InvalidQuery");
      expect(runtimeUnavailable.code).toBe("RuntimeUnavailable");
    }),
  );
});
