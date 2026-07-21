import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type RawQuery } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Schema, Stream } from "effect";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { grpcClients, grpcOrderValue, grpcTopicSources } from "../test-harness/grpc-config";
import { makeLeasedGrpcHealth } from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";

const RouteOwnershipOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

describe("gRPC lease manager route ownership", () => {
  it.live("snapshots routeBy at subscribe and preserves its exact string", () =>
    Effect.gen(function* () {
      const observedRoutes: Array<string> = [];
      const mappedRoutes: Array<{ readonly region: string }> = [];
      const localViewServer = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          orders: grpcTopicSources.leased({
            schema: RouteOwnershipOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["region"],
            request: ({ region }) => {
              observedRoutes.push(region);
              return { orderId: region };
            },
            acquire: ({ route }) => {
              observedRoutes.push(route.region);
              return Stream.make(grpcOrderValue("order-1", 10), grpcOrderValue("order-2", 20)).pipe(
                Stream.concat(Stream.never),
              );
            },
            release: ({ route }) =>
              Effect.sync(() => {
                observedRoutes.push(route.region);
              }),
            map: ({ value, route }) => {
              observedRoutes.push(route.region);
              mappedRoutes.push(route);
              return {
                id: `${route.region}:${value.customerId}`,
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region: route.region,
                updatedAt: value.updatedAt,
              };
            },
          }),
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(localViewServer);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        makeLeasedGrpcHealth(grpcOptions),
      );
      const query = {
        routeBy: { region: "ÁbCDEfgh" },
        select: ["id", "region"],
        where: [{ field: "region", type: "equals", filter: "abcdefgh" }],
      } satisfies RawQuery<typeof RouteOwnershipOrder.Type> & {
        readonly routeBy: { readonly region: string };
        readonly select: readonly ["id", "region"];
        readonly where: readonly [
          { readonly field: "region"; readonly type: "equals"; readonly filter: string },
        ];
      };

      const subscriptionEffect = manager.liveClient.subscribe("orders", query);
      Reflect.set(query.routeBy, "region", "caller-mutation");
      const subscription = yield* subscriptionEffect;
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

      expect(events.length).toBe(2);
      expect(mappedRoutes.length).toBe(2);
      expect(mappedRoutes[0]).toBe(mappedRoutes[1]);
      yield* subscription.close();
      expect(observedRoutes).toStrictEqual([
        "ÁbCDEfgh",
        "ÁbCDEfgh",
        "ÁbCDEfgh",
        "ÁbCDEfgh",
        "ÁbCDEfgh",
      ]);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
