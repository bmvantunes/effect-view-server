import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Effect, Schema } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeViewServerRuntime,
  type ViewServerRuntime,
  type ViewServerTcpPublishIngressError,
} from "./index";

import {
  runEffect,
  runtime,
  runtimeEffect,
  runtimeWithAuth,
  runtimeWithGroupedAdmissionLimits,
  viewServer,
} from "../test-harness/runtime-type-contracts";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});
const Trade = Schema.Struct({
  id: ViewServerId,
  symbol: Schema.String,
});
const SourceFailure = Schema.TaggedStruct("RuntimeTypeSourceFailure", {
  message: Schema.String,
});
const sourceAdapter = SourceAdapter.make({
  identity: { name: "runtime-type-source" },
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
const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["id"], undefined),
    },
  },
});
const materializedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.materializedSource(undefined),
    },
  },
});
const multiSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.materializedSource(undefined),
    },
    trades: {
      schema: Trade,
      source: sourceAdapter.materializedSource(undefined),
    },
  },
});
const leasedRuntimeEffect = makeViewServerRuntime(leasedViewServer);
const materializedRuntimeEffect = makeViewServerRuntime(materializedViewServer);
declare const leasedRuntime: Effect.Success<typeof leasedRuntimeEffect>;
declare const materializedRuntime: Effect.Success<typeof materializedRuntimeEffect>;
declare const sourceOwnedRuntime: typeof materializedRuntime;

type MultiSourceVisible = typeof multiSourceViewServer.topics.orders extends {
  readonly source: object;
}
  ? true
  : false;

describe("Runtime client and source ownership contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    expectTypeOf<MultiSourceVisible>().toEqualTypeOf<true>();
    expectTypeOf(runtime.url).toEqualTypeOf<ViewServerRuntime<typeof viewServer.topics>["url"]>();
    expectTypeOf(runtime.healthUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["healthUrl"]
    >();
    expectTypeOf(runtime.metricsUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["metricsUrl"]
    >();
    expectTypeOf(runtime.tcpPublishUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["tcpPublishUrl"]
    >();
    expectTypeOf(runtime.health).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["health"]
    >();
    expectTypeOf(runtime.close).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["close"]
    >();
    expectTypeOf<Effect.Services<typeof runtimeEffect>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Services<typeof materializedRuntimeEffect>>().not.toEqualTypeOf<never>();
    expectTypeOf<Effect.Success<typeof runEffect>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Services<typeof runEffect>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Error<typeof runEffect>>().toEqualTypeOf<
      HttpServerError.ServeError | ViewServerRuntimeError | ViewServerTcpPublishIngressError
    >();
    expectTypeOf<Effect.Success<typeof runtimeWithGroupedAdmissionLimits>>().toExtend<
      ViewServerRuntime<typeof viewServer.topics>
    >();
    expectTypeOf<Effect.Success<typeof runtimeWithAuth>>().toExtend<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    const publish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    const subscribe = runtime.liveClient.subscribe("orders", {
      select: ["id", "price"],
    });
    const leasedSubscribe = leasedRuntime.liveClient.subscribe("orders", {
      routeBy: { id: "order-1" },
      where: [{ field: "id", type: "equals", filter: "order-1" }],
      select: ["id"],
    });
    const materializedSnapshot = materializedRuntime.client.snapshot("orders", {
      select: ["id", "price"],
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf(subscribe).not.toBeAny();
    expectTypeOf(leasedSubscribe).not.toBeAny();
    expectTypeOf(materializedSnapshot).not.toBeAny();

    const missingRouteQuery = {
      select: ["id"],
    } satisfies {
      readonly select: readonly ["id"];
    };

    // @ts-expect-error leased Source snapshots are live-subscription-only.
    const invalidLeasedSnapshot = leasedRuntime.client.snapshot("orders", missingRouteQuery);
    // @ts-expect-error leased Source topics reject direct runtime publishes.
    const invalidLeasedPublish = leasedRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    // @ts-expect-error leased Source topics reject direct runtime batch publishes.
    const invalidLeasedPublishMany = leasedRuntime.client.publishMany("orders", [
      {
        id: "order-1",
        price: 10,
      },
    ]);
    // @ts-expect-error leased Source topics reject direct runtime patches.
    const invalidLeasedPatch = leasedRuntime.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error leased Source topics reject direct runtime deletes.
    const invalidLeasedDelete = leasedRuntime.client.delete("orders", "order-1");
    // @ts-expect-error source-owned runtimes reject direct runtime reset.
    const invalidLeasedReset = leasedRuntime.client.reset();
    // @ts-expect-error materialized Source-owned topics reject direct runtime publishes.
    const invalidSourceOwnedPublish = sourceOwnedRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    // @ts-expect-error materialized Source-owned topics reject direct runtime patches.
    const invalidSourceOwnedPatch = sourceOwnedRuntime.client.patch("orders", "order-1", {
      price: 11,
    });
    // @ts-expect-error materialized Source-owned topics reject direct runtime deletes.
    const invalidSourceOwnedDelete = sourceOwnedRuntime.client.delete("orders", "order-1");
    // @ts-expect-error source-owned runtimes reject direct runtime reset.
    const invalidSourceOwnedReset = sourceOwnedRuntime.client.reset();
    // @ts-expect-error leased Source topics require routeBy.
    const invalidLeasedSubscribe = leasedRuntime.liveClient.subscribe("orders", missingRouteQuery);

    expectTypeOf(invalidLeasedSnapshot).not.toBeAny();
    expectTypeOf(invalidLeasedPublish).not.toBeAny();
    expectTypeOf(invalidLeasedPublishMany).not.toBeAny();
    expectTypeOf(invalidLeasedPatch).not.toBeAny();
    expectTypeOf(invalidLeasedDelete).not.toBeAny();
    expectTypeOf(invalidLeasedReset).not.toBeAny();
    expectTypeOf(invalidSourceOwnedPublish).not.toBeAny();
    expectTypeOf(invalidSourceOwnedPatch).not.toBeAny();
    expectTypeOf(invalidSourceOwnedDelete).not.toBeAny();
    expectTypeOf(invalidSourceOwnedReset).not.toBeAny();
    expectTypeOf(invalidLeasedSubscribe).not.toBeAny();
    expectTypeOf(runtime.client.reset).not.toBeAny();

    const invalidPublish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
      // @ts-expect-error runtime mutation client rejects fields outside the topic row.
      prcie: 10,
    });
    // @ts-expect-error runtime live client rejects fields outside the topic row.
    const invalidSubscribe = runtime.liveClient.subscribe("orders", {
      select: ["prcie"],
    });
    const invalidTopicPublish = runtime.client.publish(
      // @ts-expect-error runtime mutation client rejects unknown topics.
      "missing",
      {
        id: "order-1",
        price: 10,
      },
    );
    // @ts-expect-error filters reject fields outside the Topic Row.
    const invalidSnapshot = runtime.client.snapshot("orders", {
      select: ["id"],
      where: [{ field: "prcie", type: "greaterThanOrEqual", filter: 10 }],
    });

    expectTypeOf(invalidPublish).not.toBeAny();
    expectTypeOf(invalidSubscribe).not.toBeAny();
    expectTypeOf(invalidTopicPublish).not.toBeAny();
    expectTypeOf(invalidSnapshot).not.toBeAny();
  });
});
