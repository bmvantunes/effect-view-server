import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Context, Effect, Layer, Schedule, Schema } from "effect";
import { createInMemoryViewServer, makeInMemoryViewServer } from "./index";
import { createInMemoryViewServerTesting, makeInMemoryViewServerTesting } from "./testing";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

const SourceFailure = Schema.TaggedStruct("InMemorySourceFailure", {
  message: Schema.String,
});
const sourceAdapter = SourceAdapter.make({
  identity: { name: "in-memory-type-source" },
  failure: SourceFailure,
  materialized: {
    metrics: Schema.Struct({ observed: Schema.BigInt }),
    rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: {
    metrics: Schema.Struct({ observed: Schema.BigInt }),
    rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
});

class RetryDependency extends Context.Service<
  RetryDependency,
  { readonly delayEnabled: boolean }
>()("@effect-view-server/in-memory/type-test/RetryDependency") {}

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const materializedSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.materializedSource(undefined),
    },
  },
});

const leasedSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["id"], undefined),
    },
  },
});

const retrySourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.materializedSource(
        undefined,
        Schedule.recurs(1).pipe(Schedule.tap(() => RetryDependency.pipe(Effect.asVoid))),
      ),
    },
  },
});

const inMemory = createInMemoryViewServer(viewServer);
const materializedSourceInMemoryEffect = makeInMemoryViewServer(materializedSourceViewServer, {});
const materializedSourceTestingEffect = makeInMemoryViewServerTesting(
  materializedSourceViewServer,
  {},
);
const leasedSourceInMemoryEffect = makeInMemoryViewServer(leasedSourceViewServer, {});
const leasedSourceTestingEffect = makeInMemoryViewServerTesting(leasedSourceViewServer, {});
const retrySourceInMemoryEffect = makeInMemoryViewServer(retrySourceViewServer, {});
const retrySourceTestingEffect = makeInMemoryViewServerTesting(retrySourceViewServer, {});

declare const materializedSourceInMemory: Effect.Success<typeof materializedSourceInMemoryEffect>;
declare const materializedSourceTesting: Effect.Success<typeof materializedSourceTestingEffect>;
declare const leasedSourceInMemory: Effect.Success<typeof leasedSourceInMemoryEffect>;
declare const leasedSourceTesting: Effect.Success<typeof leasedSourceTestingEffect>;
declare const sourceAdapterLayer: Layer.Layer<
  Context.Service.Identifier<typeof sourceAdapter.runtimeService>
>;
const retryDependencyLayer = Layer.succeed(RetryDependency)({
  delayEnabled: true,
});
const retryAppLayer = Layer.merge(sourceAdapterLayer, retryDependencyLayer);
const retrySourceInMemoryReady = retrySourceInMemoryEffect.pipe(Effect.provide(retryAppLayer));
const retrySourceTestingReady = retrySourceTestingEffect.pipe(Effect.provide(retryAppLayer));

// @effect-diagnostics missingEffectContext:off
// @ts-expect-error the in-memory runtime still requires the retry Schedule service.
const invalidRetrySourceInMemoryReady: Effect.Effect<
  Effect.Success<typeof retrySourceInMemoryEffect>,
  Effect.Error<typeof retrySourceInMemoryEffect>
> = retrySourceInMemoryEffect.pipe(Effect.provide(sourceAdapterLayer));
// @effect-diagnostics missingEffectContext:on

// @ts-expect-error synchronous in-memory construction cannot provide a Source Adapter service.
const invalidSynchronousSourceInMemory = createInMemoryViewServer(materializedSourceViewServer);
const invalidSynchronousSourceTesting = createInMemoryViewServerTesting(
  // @ts-expect-error synchronous testing construction cannot provide a Source Adapter service.
  materializedSourceViewServer,
);

const inMemoryWithGroupedAdmissionLimits = createInMemoryViewServer(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
  },
});
const invalidTransportHealthOption = createInMemoryViewServer(viewServer, {
  // @ts-expect-error in-memory does not expose Runtime Core transport adapter hooks.
  transportHealth: () => ({
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
  }),
});
const invalidGroupedAdmissionLimitKey = createInMemoryViewServer(viewServer, {
  groupedIncrementalAdmissionLimits: {
    // @ts-expect-error grouped admission limit keys are exact.
    maxGroupz: 1,
  },
});
const invalidGroupedAdmissionLimitValue = createInMemoryViewServer(viewServer, {
  groupedIncrementalAdmissionLimits: {
    // @ts-expect-error grouped admission limits must be numeric.
    maxGroups: "1",
  },
});

describe("in-memory type contracts", () => {
  it("preserves runtime, source requirements, and public client topic types", () => {
    const publish = inMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const subscription = inMemory.liveClient.subscribe("orders", {
      select: ["id"],
    });
    const materializedSnapshot = materializedSourceInMemory.client.snapshot("orders", {
      select: ["id"],
    });
    const leasedSubscribe = leasedSourceInMemory.liveClient.subscribe("orders", {
      routeBy: { id: "order-1" },
      select: ["id"],
      where: [{ field: "id", type: "equals", filter: "order-1" }],
    });
    const invalidPatch = inMemory.client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Services<typeof materializedSourceInMemoryEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof materializedSourceTestingEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof leasedSourceInMemoryEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof leasedSourceTestingEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof retrySourceInMemoryEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService> | RetryDependency
    >();
    expectTypeOf<Effect.Services<typeof retrySourceTestingEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService> | RetryDependency
    >();
    expectTypeOf<Effect.Services<typeof retrySourceInMemoryReady>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Services<typeof retrySourceTestingReady>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf<Effect.Success<typeof leasedSubscribe>>().toEqualTypeOf<
      Effect.Success<typeof subscription>
    >();
    expectTypeOf(materializedSnapshot).not.toBeAny();
    expectTypeOf(inMemory.liveClient).not.toHaveProperty("subscribeRuntime");
    expectTypeOf(leasedSourceTesting.serverLiveClient).toHaveProperty("subscribeProtocolQuery");
    expectTypeOf(inMemoryWithGroupedAdmissionLimits.client).toEqualTypeOf<typeof inMemory.client>();
    expectTypeOf(invalidPatch).not.toBeAny();
    expectTypeOf(invalidTransportHealthOption).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitKey).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitValue).not.toBeAny();
    expectTypeOf(invalidSynchronousSourceInMemory).not.toBeAny();
    expectTypeOf(invalidSynchronousSourceTesting).not.toBeAny();
    expectTypeOf(invalidRetrySourceInMemoryReady).not.toBeAny();
  });

  it("rejects public source-owned mutations and leased snapshots", () => {
    const leasedQuery = {
      routeBy: { id: "order-1" },
      select: ["id"],
      where: [{ field: "id", type: "equals", filter: "order-1" }],
    } satisfies {
      readonly routeBy: { readonly id: "order-1" };
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "id";
          readonly type: "equals";
          readonly filter: "order-1";
        },
      ];
    };

    // @ts-expect-error public in-memory clients reject leased Source snapshots.
    const invalidLeasedSnapshot = leasedSourceInMemory.client.snapshot("orders", leasedQuery);
    // @ts-expect-error source-owned topics reject direct in-memory publishes.
    const invalidLeasedPublish = leasedSourceInMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error source-owned topics reject direct in-memory batch publishes.
    const invalidLeasedPublishMany = leasedSourceInMemory.client.publishMany("orders", [
      { id: "order-1", price: 42 },
    ]);
    // @ts-expect-error source-owned topics reject direct in-memory patches.
    const invalidLeasedPatch = leasedSourceInMemory.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error source-owned topics reject direct in-memory deletes.
    const invalidLeasedDelete = leasedSourceInMemory.client.delete("orders", "order-1");
    // @ts-expect-error source-owned in-memory clients reject direct reset.
    const invalidLeasedReset = leasedSourceInMemory.client.reset();
    // @ts-expect-error source-owned topics reject direct in-memory publishes.
    const invalidMaterializedPublish = materializedSourceInMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error source-owned topics reject direct in-memory batch publishes.
    const invalidMaterializedPublishMany = materializedSourceInMemory.client.publishMany("orders", [
      { id: "order-1", price: 42 },
    ]);
    // @ts-expect-error source-owned topics reject direct in-memory patches.
    const invalidMaterializedPatch = materializedSourceInMemory.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error source-owned topics reject direct in-memory deletes.
    const invalidMaterializedDelete = materializedSourceInMemory.client.delete("orders", "order-1");
    // @ts-expect-error source-owned in-memory clients reject direct reset.
    const invalidMaterializedReset = materializedSourceInMemory.client.reset();

    expectTypeOf(invalidLeasedSnapshot).not.toBeAny();
    expectTypeOf(invalidLeasedPublish).not.toBeAny();
    expectTypeOf(invalidLeasedPublishMany).not.toBeAny();
    expectTypeOf(invalidLeasedPatch).not.toBeAny();
    expectTypeOf(invalidLeasedDelete).not.toBeAny();
    expectTypeOf(invalidLeasedReset).not.toBeAny();
    expectTypeOf(invalidMaterializedPublish).not.toBeAny();
    expectTypeOf(invalidMaterializedPublishMany).not.toBeAny();
    expectTypeOf(invalidMaterializedPatch).not.toBeAny();
    expectTypeOf(invalidMaterializedDelete).not.toBeAny();
    expectTypeOf(invalidMaterializedReset).not.toBeAny();
  });

  it("allows testing instances to drive Source-owned topics", () => {
    const leasedSubscribe = leasedSourceTesting.liveClient.subscribe("orders", {
      routeBy: { id: "order-1" },
      select: ["id"],
      where: [{ field: "id", type: "equals", filter: "order-1" }],
    });
    const leasedPublish = leasedSourceTesting.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const materializedPublish = materializedSourceTesting.client.publish("orders", {
      id: "order-1",
      price: 42,
    });

    expectTypeOf<Effect.Success<typeof leasedSubscribe>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf<Effect.Success<typeof leasedPublish>>().toEqualTypeOf<void>();
    expectTypeOf<Effect.Success<typeof materializedPublish>>().toEqualTypeOf<void>();
  });
});
