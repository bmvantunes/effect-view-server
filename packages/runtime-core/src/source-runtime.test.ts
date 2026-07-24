import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import {
  SourceFixture,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import { Deferred, Effect, Exit, Fiber, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeViewServerRuntimeCore } from "./index";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { sourceLeaseTerminalObserver } from "./source-runtime";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
  region: Schema.String,
});

const materializedTarget: SourceFixtureTarget = {
  _tag: "Materialized",
};

const leasedTarget = (region: string): SourceFixtureTarget => ({
  _tag: "Leased",
  route: { region },
});

describe("Runtime Core Source Adapter vertical slice", () => {
  it.effect("publishes deeply frozen decoded adapter metrics", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const accessorPayload = {
        get nested() {
          return ["payload"];
        },
      };
      yield* fixture.controls.setMetrics({
        observed: 1n,
        details: {
          samples: [1, { nested: ["sample"] }],
          payload: accessorPayload,
        },
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: fixture.materializedSource({
              label: "deeply-frozen-metrics",
            }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      const details = Option.getOrThrow(Option.fromUndefinedOr(health.metrics.adapter.details));
      const nestedSample = Option.getOrThrow(Option.fromUndefinedOr(details.samples[1]));

      expect({
        adapter: Object.isFrozen(health.metrics.adapter),
        details: Object.isFrozen(details),
        samples: Object.isFrozen(details.samples),
        nestedSample: Object.isFrozen(nestedSample),
        payload: Object.isFrozen(details.payload),
      }).toStrictEqual({
        adapter: true,
        details: true,
        samples: true,
        nestedSample: true,
        payload: true,
      });

      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("routes observed server subscriptions through the shared Leased Source path", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: fixture.leasedSource(["region"], {
              label: "observed-leased-orders",
            }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCoreInternal(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      const subscription = yield* runtime.internalLiveClient.subscribeObservedInternal(
        "orders",
        {
          routeBy: { region: "eu" },
          select: ["id", "price", "region"],
        },
        sourceLeaseTerminalObserver,
      );
      const eventsFiber = yield* subscription.events.pipe(
        Stream.filter((event) => event.type !== "status"),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* fixture.controls.awaitActive(leasedTarget("eu"));
      yield* fixture.controls.upsert(leasedTarget("eu"), {
        id: "observed",
        price: 10,
        region: "eu",
      });
      const events = yield* Fiber.join(eventsFiber);

      expect(events.map((event) => event.type)).toStrictEqual(["snapshot", "delta"]);
      yield* subscription.close();
      yield* runtime.close;
    }),
  );

  it.effect(
    "applies ordered materialized deliveries, continues after rejection, and reports sticky degraded health",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Order);
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: fixture.materializedSource({
                label: "materialized-orders",
              }),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(fixture.layer),
        );
        yield* Effect.yieldNow;
        const startupDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
        const startupHealth = yield* startupDiagnostics.events.pipe(Stream.take(1), Stream.runHead);
        expect(Option.getOrThrow(startupHealth).status._tag).toBe("Ready");
        yield* startupDiagnostics.close();
        const startupCounts = fixture.controls.counts(materializedTarget);
        expect(startupCounts.acquisitions).toBe(1n);
        expect(startupCounts.finalizations).toBe(0n);
        const subscription = yield* runtime.liveClient.subscribe("orders", {
          select: ["id", "price", "region"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const eventsFiber = yield* subscription.events.pipe(
          Stream.filter((event) => event.type !== "status"),
          Stream.take(4),
          Stream.runCollect,
          Effect.forkChild,
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
        const degradedFiber = yield* diagnostics.events.pipe(
          Stream.filter((result) => result.status._tag === "Degraded"),
          Stream.take(1),
          Stream.runHead,
          Effect.forkChild,
        );
        const settlementResults: Array<boolean> = [];

        yield* fixture.controls.awaitActive(materializedTarget).pipe(Effect.timeout("500 millis"));
        yield* fixture.controls.upsert(
          materializedTarget,
          { id: "a", price: 10, region: "eu" },
          (exit) =>
            Effect.sync(() => {
              settlementResults.push(Exit.isSuccess(exit));
            }),
        );
        yield* fixture.controls.reject(
          materializedTarget,
          SourceFixture.failure("bad source item", "stream"),
          { lane: "fixture", offset: 1n },
        );
        yield* fixture.controls.upsert(
          materializedTarget,
          { id: "b", price: 20, region: "us" },
          (exit) =>
            Effect.sync(() => {
              settlementResults.push(Exit.isSuccess(exit));
            }),
        );
        yield* fixture.controls.delete(materializedTarget, "a", (exit) =>
          Effect.sync(() => {
            settlementResults.push(Exit.isSuccess(exit));
          }),
        );

        const events = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));
        const degraded = yield* Fiber.join(degradedFiber).pipe(Effect.timeout("1 second"));
        expect(events.map((event) => event.type)).toStrictEqual([
          "snapshot",
          "delta",
          "delta",
          "delta",
        ]);
        expect(settlementResults).toStrictEqual([true, true, true]);
        expect(Option.getOrThrow(degraded).status._tag).toBe("Degraded");
        yield* TestClock.adjust("1 second");
        const currentHealth = yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead);
        const current = Option.getOrThrow(currentHealth);
        expect(current.status._tag).toBe("Degraded");
        expect(current.metrics.runtime.currentAttempt).toBe(1n);
        expect(current.metrics.runtime.retryCount).toBe(0n);
        expect(current.metrics.runtime.receivedDeliveryCount).toBe(3n);
        expect(current.metrics.runtime.rejectedItemCount).toBe(1n);
        expect(current.metrics.runtime.attemptedMutationCount).toBe(3n);
        expect(current.metrics.runtime.appliedUpsertCount).toBe(2n);
        expect(current.metrics.runtime.appliedDeleteCount).toBe(1n);
        expect(current.metrics.runtime.failedMutationCount).toBe(0n);
        expect(current.metrics.runtime.completedSettlementCount).toBe(4n);
        expect(current.metrics.runtime.failedSettlementCount).toBe(0n);
        expect(current.metrics.runtime.retainedRowCount).toBe(1);

        const finalSubscription = yield* runtime.liveClient.subscribe("orders", {
          select: ["id", "price", "region"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const finalSnapshot = yield* finalSubscription.events.pipe(
          Stream.filter((event) => event.type === "snapshot"),
          Stream.take(1),
          Stream.runHead,
        );
        const finalSnapshotEvent = Option.getOrThrow(finalSnapshot);
        expect(finalSnapshotEvent.type).toBe("snapshot");
        expect(finalSnapshotEvent.keys).toStrictEqual(["b"]);
        expect(finalSnapshotEvent.rows).toStrictEqual([{ id: "b", price: 20, region: "us" }]);
        expect(finalSnapshotEvent.totalRows).toBe(1);
        const aggregateHealth = yield* runtime.refreshHealth;
        expect(aggregateHealth.status).toBe("degraded");
        expect(aggregateHealth.engine.topics.orders.status).toBe("degraded");

        yield* finalSubscription.close();
        yield* diagnostics.close();
        yield* subscription.close();
        yield* runtime.close;
        expect(fixture.controls.counts(materializedTarget).finalizations).toBe(1n);
      }),
  );

  it.effect(
    "retries the whole attempt after rejection settlement failure and unexpected completion",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Order);
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: fixture.materializedSource({
                label: "retrying-orders",
              }),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(fixture.layer),
        );
        yield* fixture.controls.awaitActive(materializedTarget);
        const subscription = yield* runtime.liveClient.subscribe("orders", {
          select: ["id", "price", "region"],
        });
        const availabilityFiber = yield* subscription.events.pipe(
          Stream.filter((event) => event.type === "status"),
          Stream.takeUntil((event) => event.status === "error"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;

        yield* fixture.controls.reject(
          materializedTarget,
          SourceFixture.failure("rejected", "stream"),
          { lane: "fixture", offset: 2n },
          () => Effect.fail(SourceFixture.failure("rejection settlement failed", "settlement")),
        );
        yield* fixture.controls.awaitCounts(materializedTarget, {
          acquisitions: 2n,
          finalizations: 1n,
        });

        yield* fixture.controls.fail(
          materializedTarget,
          SourceFixture.failure("stream failed", "stream"),
        );
        yield* fixture.controls.awaitCounts(materializedTarget, {
          acquisitions: 3n,
          finalizations: 2n,
        });

        yield* fixture.controls.complete(materializedTarget);
        yield* fixture.controls.awaitCounts(materializedTarget, {
          acquisitions: 4n,
          finalizations: 3n,
        });

        yield* fixture.controls.fail(
          materializedTarget,
          SourceFixture.failure("terminal stream failure", "stream"),
        );
        const availability = yield* Fiber.join(availabilityFiber).pipe(Effect.timeout("1 second"));
        expect(availability.map((event) => `${event.status}:${event.code}`)).toStrictEqual([
          "ready:Ready",
          "stale:SnapshotStale",
          "ready:Ready",
          "stale:SnapshotStale",
          "ready:Ready",
          "stale:SnapshotStale",
          "ready:Ready",
          "error:RuntimeUnavailable",
        ]);
        expect(new Set(availability.map((event) => event.queryId)).size).toBe(1);

        yield* subscription.close();
        yield* runtime.close;
        expect(fixture.controls.counts(materializedTarget).finalizations).toBe(4n);
      }),
  );

  it.effect("retries acquisition failures and rejects controls for inactive targets", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const inactiveFailure = yield* Effect.flip(
        fixture.controls.upsert(materializedTarget, {
          id: "inactive",
        }),
      );
      expect(inactiveFailure).toStrictEqual(
        SourceFixture.failure("Fixture target is not active.", "stream"),
      );
      const defaultMaterialized = fixture.materializedSource();
      const defaultLeased = fixture.leasedSource(["region"]);
      expect(defaultMaterialized.options).toStrictEqual({
        label: "materialized",
        row: Order,
      });
      expect(defaultLeased.options).toStrictEqual({
        label: "leased",
        row: Order,
      });

      yield* fixture.controls.failNextAcquisition(
        materializedTarget,
        SourceFixture.failure("acquisition failed", "acquire"),
      );
      yield* fixture.controls.setMetrics({ observed: 1n });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: defaultMaterialized,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: 1n,
        finalizations: 0n,
      });
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders");
      const sampled = yield* diagnostics.events.pipe(
        Stream.filter((result) => result.metrics.adapter.observed === 1n),
        Stream.take(1),
        Stream.runHead,
        Effect.timeout("2 seconds"),
      );
      expect(Option.isSome(sampled)).toBe(true);
      expect(fixture.controls.metricReads()).toBe(1n);
      yield* fixture.controls.setMetrics({ observed: 2n });
      yield* fixture.controls.delete(materializedTarget, "missing");
      expect(fixture.controls.metricReads()).toBe(1n);
      const resampledFiber = yield* diagnostics.events.pipe(
        Stream.filter((result) => result.metrics.adapter.observed === 2n),
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* TestClock.adjust("999 millis");
      expect(fixture.controls.metricReads()).toBe(1n);
      yield* TestClock.adjust("1 millis");
      const resampled = yield* Fiber.join(resampledFiber);
      expect(fixture.controls.metricReads()).toBe(2n);
      expect(Option.getOrThrow(resampled).metrics.adapter).toStrictEqual({
        observed: 2n,
      });

      yield* diagnostics.close();
      yield* runtime.close;
      expect(fixture.controls.counts(materializedTarget)).toStrictEqual({
        acquisitions: 1n,
        finalizations: 1n,
      });
    }),
  );

  it.effect(
    "shares leased routes, isolates distinct routes, keeps diagnostics non-owning, and cleans rows on final release",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Order);
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: fixture.leasedSource(["region"], {
                label: "leased-orders",
              }),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(fixture.layer),
        );
        const euTarget = leasedTarget("eu");
        const usTarget = leasedTarget("us");
        const euDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("orders", {
          region: "eu",
        });
        const initialDiagnostic = yield* euDiagnostics.events.pipe(Stream.take(1), Stream.runHead);
        expect(Option.getOrThrow(initialDiagnostic)).toStrictEqual({
          _tag: "Inactive",
          route: { region: "eu" },
        });
        expect(fixture.controls.counts(euTarget).acquisitions).toBe(0n);

        const euFirst = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          select: ["id", "region"],
        });
        const euSecond = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          select: ["id", "region"],
        });
        const us = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "us" },
          select: ["id", "region"],
        });
        yield* fixture.controls.awaitActive(euTarget);
        yield* fixture.controls.awaitActive(usTarget);
        expect(fixture.controls.counts(euTarget).acquisitions).toBe(1n);
        expect(fixture.controls.counts(usTarget).acquisitions).toBe(1n);

        const euApplied = yield* Deferred.make<void>();
        const usApplied = yield* Deferred.make<void>();
        yield* fixture.controls.upsert(
          euTarget,
          {
            id: "eu-1",
            price: 10,
            region: "eu",
          },
          () => Deferred.succeed(euApplied, undefined).pipe(Effect.asVoid),
        );
        yield* fixture.controls.upsert(
          usTarget,
          {
            id: "us-1",
            price: 20,
            region: "us",
          },
          () => Deferred.succeed(usApplied, undefined).pipe(Effect.asVoid),
        );
        yield* Effect.all([Deferred.await(euApplied), Deferred.await(usApplied)]);
        const euSnapshotSubscription = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          select: ["id", "region"],
        });
        const euSnapshot = yield* euSnapshotSubscription.events.pipe(
          Stream.filter((event) => event.type === "snapshot"),
          Stream.take(1),
          Stream.runHead,
        );
        const euSnapshotEvent = Option.getOrThrow(euSnapshot);
        expect(euSnapshotEvent.type).toBe("snapshot");
        expect(euSnapshotEvent.keys).toStrictEqual(["eu-1"]);
        expect(euSnapshotEvent.rows).toStrictEqual([{ id: "eu-1", region: "eu" }]);

        yield* euFirst.close();
        expect(fixture.controls.counts(euTarget).finalizations).toBe(0n);
        yield* euSecond.close();
        expect(fixture.controls.counts(euTarget).finalizations).toBe(1n);
        yield* euSnapshotSubscription.close();
        const inactiveAgain = yield* euDiagnostics.events.pipe(
          Stream.filter((result) => result._tag === "Inactive"),
          Stream.take(1),
          Stream.runHead,
        );
        expect(Option.getOrThrow(inactiveAgain)).toStrictEqual({
          _tag: "Inactive",
          route: { region: "eu" },
        });

        const euAfterRelease = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          select: ["id", "region"],
        });
        const emptySnapshot = yield* euAfterRelease.events.pipe(
          Stream.filter((event) => event.type === "snapshot"),
          Stream.take(1),
          Stream.runHead,
        );
        const emptySnapshotEvent = Option.getOrThrow(emptySnapshot);
        expect(emptySnapshotEvent.type).toBe("snapshot");
        expect(emptySnapshotEvent.keys).toStrictEqual([]);
        expect(emptySnapshotEvent.rows).toStrictEqual([]);
        expect(emptySnapshotEvent.totalRows).toBe(0);
        expect(fixture.controls.counts(euTarget).acquisitions).toBe(2n);

        yield* euAfterRelease.close();
        yield* us.close();
        yield* euDiagnostics.close();
        yield* runtime.close;
        expect(fixture.controls.counts(usTarget).finalizations).toBe(1n);
      }),
  );
});
