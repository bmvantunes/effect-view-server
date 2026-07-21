import { describe, expect, it } from "@effect/vitest";
import type { ViewServerLiveEvent } from "@effect-view-server/client";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@effect-view-server/runtime-core/internal";
import { Effect, Stream } from "effect";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";

import {
  grpcLeasedViewServer,
  leasedGrpcViewServer,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";

const presentGroupedFieldKey = (field: string, canonicalKey: string): string =>
  JSON.stringify([[field, JSON.stringify(["present", canonicalKey])]]);

describe("gRPC lease manager large grouped snapshots", () => {
  it.live("externalizes a 200,000-row grouped snapshot without caller-side spread limits", () =>
    Effect.gen(function* () {
      const cardinality = 200_000;
      const midpoint = Math.floor(cardinality / 2);
      const internalKeys = Array.from(
        { length: cardinality },
        (_value, index) => "customer-internal-" + index,
      );
      const rows = Array.from({ length: cardinality }, (_value, index) => ({
        customerId: "customer-" + index,
        rowCount: 1n,
      }));
      const internalSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "large-customer-groups",
        version: 0,
        keys: internalKeys,
        rows,
        totalRows: cardinality,
      } as const;
      const publicGroupedCustomerKey = (customerId: string): string =>
        presentGroupedFieldKey("customerId", JSON.stringify(customerId));
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {}),
        (runtimeCore) => {
          const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
            typeof leasedGrpcViewServer.topics
          > = {
            ...runtimeCore.internalLiveClient,
            subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
              observer.onQueryRegistered(internalSnapshot.queryId).pipe(
                Effect.as({
                  events: Stream.make(internalSnapshot).pipe(Stream.concat(Stream.never)),
                  close: () => Effect.void,
                }),
              ),
          };
          const health = makeLeasedGrpcHealth(grpcOptions);

          return Effect.acquireUseRelease(
            makeViewServerGrpcLeaseManager(
              grpcOptions.sourceConfig,
              runtimeCore.internalClient,
              runtimeCore.liveClient,
              fakeInternalLiveClient,
              Effect.void,
              grpcOptions,
              health,
            ),
            (manager) =>
              Effect.acquireUseRelease(
                manager.liveClient.subscribeRuntime("orders", {
                  routeBy: { region: "usa" },
                  groupBy: ["customerId"],
                  aggregates: {
                    rowCount: { aggFunc: "count" },
                  },
                  where: [{ field: "region", type: "equals", filter: "usa" }],
                  limit: cardinality,
                }),
                (subscription) =>
                  Effect.gen(function* () {
                    const firstEventOption = yield* Stream.runHead(subscription.events);
                    const firstEvent: ViewServerLiveEvent<object> =
                      yield* Effect.fromOption(firstEventOption);
                    const snapshot = yield* Effect.succeed(firstEvent).pipe(
                      Effect.filterOrFail(
                        (
                          event,
                        ): event is Extract<typeof firstEvent, { readonly type: "snapshot" }> =>
                          event.type === "snapshot",
                      ),
                    );

                    expect(snapshot.keys.length).toBe(cardinality);
                    expect(snapshot.keys[0]).toBe(publicGroupedCustomerKey("customer-0"));
                    expect(snapshot.keys[cardinality - 1]).toBe(
                      publicGroupedCustomerKey("customer-" + (cardinality - 1)),
                    );
                    expect(snapshot.rows.length).toBe(cardinality);
                    expect([
                      snapshot.rows[0],
                      snapshot.rows[midpoint],
                      snapshot.rows[cardinality - 1],
                    ]).toStrictEqual([
                      { customerId: "customer-0", rowCount: 1n },
                      { customerId: "customer-" + midpoint, rowCount: 1n },
                      { customerId: "customer-" + (cardinality - 1), rowCount: 1n },
                    ]);
                    expect(snapshot.totalRows).toBe(cardinality);
                  }),
                (subscription) => subscription.close(),
              ),
            (manager) => manager.close,
          );
        },
        (runtimeCore) => runtimeCore.close,
      );
    }),
  );
});
