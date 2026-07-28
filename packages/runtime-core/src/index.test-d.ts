import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import {
  ViewServerId,
  defineViewServerConfig,
  type FilterExpression,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Context, Effect, Layer, Schedule, Schema } from "effect";
import { createViewServerRuntimeCore, makeViewServerRuntimeCore } from "./index";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

type ValidRuntimeCoreIdCondition = {
  readonly field: "id";
  readonly type: "equals";
  readonly filter: "order-1";
};

type QueryUnionWithInvalidWhere =
  | { readonly select: readonly ["id"] }
  | {
      readonly select: readonly ["id"];
      readonly where: readonly [ValidRuntimeCoreIdCondition & { readonly unexpected: true }];
    };

const SourceFailure = Schema.TaggedStruct("RuntimeCoreSourceFailure", {
  message: Schema.String,
});
const sourceAdapter = SourceAdapter.make({
  identity: { name: "runtime-core-type-source" },
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
>()("@effect-view-server/runtime-core/type-test/RetryDependency") {}

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

const runtimeCore = createViewServerRuntimeCore(viewServer);
const runtimeCoreEffect = makeViewServerRuntimeCore(viewServer, {});
const materializedSourceRuntimeCoreEffect = makeViewServerRuntimeCore(
  materializedSourceViewServer,
  {},
);
const leasedSourceRuntimeCoreEffect = makeViewServerRuntimeCore(leasedSourceViewServer, {});
const retrySourceRuntimeCoreEffect = makeViewServerRuntimeCore(retrySourceViewServer, {});

declare const materializedSourceRuntimeCore: Effect.Success<
  typeof materializedSourceRuntimeCoreEffect
>;
declare const leasedSourceRuntimeCore: Effect.Success<typeof leasedSourceRuntimeCoreEffect>;
declare const sourceAdapterLayer: Layer.Layer<
  Context.Service.Identifier<typeof sourceAdapter.runtimeService>
>;
const retryDependencyLayer = Layer.succeed(RetryDependency)({
  delayEnabled: true,
});
const retryAppLayer = Layer.merge(sourceAdapterLayer, retryDependencyLayer);
const retrySourceRuntimeCoreReady = retrySourceRuntimeCoreEffect.pipe(
  Effect.provide(retryAppLayer),
);

// @effect-diagnostics missingEffectContext:off
// @ts-expect-error the retry Schedule service remains required when the app Layer omits it.
const invalidRetrySourceRuntimeCoreReady: Effect.Effect<
  Effect.Success<typeof retrySourceRuntimeCoreEffect>,
  Effect.Error<typeof retrySourceRuntimeCoreEffect>
> = retrySourceRuntimeCoreEffect.pipe(Effect.provide(sourceAdapterLayer));
// @effect-diagnostics missingEffectContext:on

const invalidSynchronousSourceRuntimeCore = createViewServerRuntimeCore(
  // @ts-expect-error synchronous Runtime Core construction cannot provide a Source Adapter service.
  materializedSourceViewServer,
);

describe("runtime-core type contracts", () => {
  it("preserves runtime, source requirements, and public client topic types", () => {
    const canonicalExpression: FilterExpression<typeof Order.Type> = {
      field: "id",
      type: "equals",
      filter: "order-1",
    };
    const publish = runtimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const subscription = runtimeCore.liveClient.subscribe("orders", {
      select: ["id"],
      where: [canonicalExpression],
    });
    const materializedSnapshot = materializedSourceRuntimeCore.client.snapshot("orders", {
      select: ["id"],
    });
    const materializedSubscribe = materializedSourceRuntimeCore.liveClient.subscribe("orders", {
      select: ["id"],
    });
    const leasedSubscribe = leasedSourceRuntimeCore.liveClient.subscribe("orders", {
      routeBy: { id: "order-1" },
      select: ["id"],
      where: [canonicalExpression],
    });
    const rejectQueryUnion = (query: QueryUnionWithInvalidWhere) => {
      // @ts-expect-error every whole-query union member must be exact.
      return runtimeCore.client.snapshot("orders", query);
    };
    const invalidValidatedSubscription = runtimeCore.serverLiveClient.subscribeProtocolQuery(
      "orders",
      // @ts-expect-error only the protocol decoder can construct a validated runtime query.
      { select: ["id"] },
    );
    const invalidPatch = runtimeCore.client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });
    const runtimeCoreWithTransportHealth = createViewServerRuntimeCore(viewServer, {
      transportHealth: (health) => {
        expectTypeOf(health.topics.orders.rowCount).toEqualTypeOf<number>();
        return {
          activeClients: 1,
          activeStreams: health.activeSubscriptions,
          activeSubscriptions: health.activeSubscriptions,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          queuedMessages: health.queuedEvents,
          queuedBytes: 0,
          droppedClients: 0,
          backpressureEvents: health.backpressureEvents,
          reconnects: 0,
          lastError: null,
        };
      },
    });
    const runtimeCoreWithGroupedAdmissionLimits = createViewServerRuntimeCore(viewServer, {
      groupedIncrementalAdmissionLimits: {
        maxGroups: 1,
      },
    });
    const invalidGroupedAdmissionLimitKey = createViewServerRuntimeCore(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limit keys are exact.
        maxGroupz: 1,
      },
    });
    const invalidGroupedAdmissionLimitValue = createViewServerRuntimeCore(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits must be numeric.
        maxGroups: "1",
      },
    });
    // @ts-expect-error public runtime-core instances must not expose route-bypassing internals.
    const _internalLiveClient = runtimeCore.internalLiveClient;
    type _InternalLiveClientFromMake = Effect.Success<
      typeof runtimeCoreEffect
      // @ts-expect-error public runtime-core factory success must not expose route-bypassing internals.
    >["internalLiveClient"];

    expectTypeOf(rejectQueryUnion).toBeFunction();
    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Success<typeof runtimeCoreEffect>>().toEqualTypeOf<typeof runtimeCore>();
    expectTypeOf<Effect.Services<typeof materializedSourceRuntimeCoreEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof leasedSourceRuntimeCoreEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof retrySourceRuntimeCoreEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService> | RetryDependency
    >();
    expectTypeOf<Effect.Services<typeof retrySourceRuntimeCoreReady>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf<Effect.Success<typeof materializedSubscribe>>().toEqualTypeOf<
      Effect.Success<typeof subscription>
    >();
    expectTypeOf<Effect.Success<typeof leasedSubscribe>>().toEqualTypeOf<
      Effect.Success<typeof subscription>
    >();
    expectTypeOf(materializedSnapshot).not.toBeAny();
    expectTypeOf(invalidValidatedSubscription).not.toBeAny();
    expectTypeOf(invalidPatch).not.toBeAny();
    expectTypeOf(runtimeCoreWithTransportHealth.client).toEqualTypeOf<typeof runtimeCore.client>();
    expectTypeOf(runtimeCoreWithGroupedAdmissionLimits.client).toEqualTypeOf<
      typeof runtimeCore.client
    >();
    expectTypeOf(invalidGroupedAdmissionLimitKey).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitValue).not.toBeAny();
    expectTypeOf(invalidRetrySourceRuntimeCoreReady).not.toBeAny();
    expectTypeOf(invalidSynchronousSourceRuntimeCore).not.toBeAny();
  });

  it("rejects source-owned mutations and leased snapshots", () => {
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

    // @ts-expect-error public runtime-core clients reject leased Source snapshots.
    const invalidLeasedSnapshot = leasedSourceRuntimeCore.client.snapshot("orders", leasedQuery);
    // @ts-expect-error source-owned topics reject direct runtime-core publishes.
    const invalidLeasedPublish = leasedSourceRuntimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error source-owned topics reject direct runtime-core batch publishes.
    const invalidLeasedPublishMany = leasedSourceRuntimeCore.client.publishMany("orders", [
      {
        id: "order-1",
        price: 42,
      },
    ]);
    // @ts-expect-error source-owned topics reject direct runtime-core patches.
    const invalidLeasedPatch = leasedSourceRuntimeCore.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error source-owned topics reject direct runtime-core deletes.
    const invalidLeasedDelete = leasedSourceRuntimeCore.client.delete("orders", "order-1");
    // @ts-expect-error source-owned runtime-core clients reject direct reset.
    const invalidLeasedReset = leasedSourceRuntimeCore.client.reset();
    // @ts-expect-error source-owned topics reject direct runtime-core publishes.
    const invalidMaterializedPublish = materializedSourceRuntimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const invalidMaterializedPublishMany = materializedSourceRuntimeCore.client.publishMany(
      // @ts-expect-error source-owned topics reject direct runtime-core batch publishes.
      "orders",
      [{ id: "order-1", price: 42 }],
    );
    const invalidMaterializedPatch = materializedSourceRuntimeCore.client.patch(
      // @ts-expect-error source-owned topics reject direct runtime-core patches.
      "orders",
      "order-1",
      { price: 10 },
    );
    const invalidMaterializedDelete = materializedSourceRuntimeCore.client.delete(
      // @ts-expect-error source-owned topics reject direct runtime-core deletes.
      "orders",
      "order-1",
    );
    // @ts-expect-error source-owned runtime-core clients reject direct reset.
    const invalidMaterializedReset = materializedSourceRuntimeCore.client.reset();
    // @ts-expect-error server transport clients never expose public query subscriptions.
    const _invalidServerSubscribe = leasedSourceRuntimeCore.serverLiveClient.subscribe(
      "orders",
      leasedQuery,
    );
    const _invalidServerRuntimeSubscribe =
      // @ts-expect-error server transport clients never expose public runtime subscriptions.
      leasedSourceRuntimeCore.serverLiveClient.subscribeRuntime("orders", leasedQuery);

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
});
