import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { makeViewServerRuntimeCore } from "./index";
import {
  Order,
  order,
  publicLeasedRuntimeAccessError,
  publicSourceOwnedRuntimeMutationError,
  publicSourceOwnedRuntimeResetError,
} from "./runtime-core-test-fixtures";

const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      grpcSource: grpcSourceMarkers.leased({ routeBy: ["region", "status"] }),
    },
  },
});

describe("@effect-view-server/runtime-core", () => {
  it.effect("rejects direct leased topic runtime mutations through the public runtime core", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(leasedViewServer, {});
      const publishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.publish,
        runtimeCore.client,
        ["orders", order("a", 10)],
      );
      const publishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.publishMany,
        runtimeCore.client,
        ["orders", [order("b", 20)]],
      );
      const patchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.patch,
        runtimeCore.client,
        ["orders", "a", { price: 20 }],
      );
      const deleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.delete,
        runtimeCore.client,
        ["orders", "a"],
      );
      const resetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.reset,
        runtimeCore.client,
        [],
      );
      expect(yield* Effect.flip(publishEffect)).toStrictEqual(
        publicSourceOwnedRuntimeMutationError,
      );
      expect(yield* Effect.flip(publishManyEffect)).toStrictEqual(
        publicSourceOwnedRuntimeMutationError,
      );
      expect(yield* Effect.flip(patchEffect)).toStrictEqual(publicSourceOwnedRuntimeMutationError);
      expect(yield* Effect.flip(deleteEffect)).toStrictEqual(publicSourceOwnedRuntimeMutationError);
      expect(yield* Effect.flip(resetEffect)).toStrictEqual(publicSourceOwnedRuntimeResetError);

      yield* runtimeCore.close;
    }),
  );

  it.effect("rejects public leased queries before route validation", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(leasedViewServer, {});
      const missingRouteQuery = {
        where: [{ field: "region", type: "equals", filter: "usa" }],
        select: ["id"],
        limit: 1,
      };
      const snapshotEffect: Effect.Effect<unknown, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.snapshot,
        runtimeCore.client,
        ["orders", missingRouteQuery],
      );
      const subscribeEffect: Effect.Effect<unknown, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.liveClient.subscribe,
        runtimeCore.liveClient,
        ["orders", missingRouteQuery],
      );

      expect(yield* Effect.flip(snapshotEffect)).toStrictEqual(publicLeasedRuntimeAccessError);
      expect(yield* Effect.flip(subscribeEffect)).toStrictEqual(publicLeasedRuntimeAccessError);

      yield* runtimeCore.close;
    }),
  );

  it.effect("rejects captured leased subscription effects when they execute", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedViewServer, {});
      const query = {
        where: [
          { field: "region", type: "equals", filter: "usa" },
          { field: "status", type: "equals", filter: "open" },
        ],
        routeBy: { region: "usa", status: "open" },
        select: ["id"],
        limit: 1,
      } satisfies {
        where: [
          { field: "region"; type: "equals"; filter: string },
          { field: "status"; type: "equals"; filter: "open" | "closed" | "cancelled" },
        ];
        routeBy: {
          region: string;
          status: "open" | "closed" | "cancelled";
        };
        select: ["id"];
        limit: number;
      };
      const subscribeEffect = runtimeCore.liveClient.subscribe("orders", query);
      const subscribeRuntimeEffect = runtimeCore.liveClient.subscribeRuntime("orders", query);
      expect(Reflect.set(query.routeBy, "status", "closed")).toBe(true);

      expect(yield* Effect.flip(subscribeEffect)).toStrictEqual(publicLeasedRuntimeAccessError);
      expect(yield* Effect.flip(subscribeRuntimeEffect)).toStrictEqual(
        publicLeasedRuntimeAccessError,
      );

      yield* runtimeCore.close;
    }),
  );
});
