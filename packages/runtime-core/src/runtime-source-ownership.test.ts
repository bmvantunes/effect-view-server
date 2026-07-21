import { describe, expect, it } from "@effect/vitest";
import type { ViewServerRuntimeError } from "@effect-view-server/config";
import {
  trustDecodedRuntimeQuery,
  viewServerRuntimeDecodedMutationTrust,
} from "@effect-view-server/config/internal";
import { Effect, Stream } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { makeViewServerRuntimeCore } from "./index";
import {
  kafkaOwnedViewServer,
  leasedViewServer,
  materializedGrpcSourceViewServer,
  order,
  publicLeasedRuntimeAccessError,
  publicSourceOwnedRuntimeMutationError,
  publicSourceOwnedRuntimeResetError,
  viewServer,
} from "./test-support/runtime-test-fixtures";

const expectRuntimeRejection = Effect.fn("ViewServerRuntimeCore.test.expectRuntimeRejection")(
  function* <A>(
    effect: Effect.Effect<A, ViewServerRuntimeError>,
    expected: ViewServerRuntimeError,
  ) {
    expect(yield* Effect.flip(effect)).toStrictEqual(expected);
  },
);

describe("Runtime Core source ownership", () => {
  it.effect("exposes route-bypassing internals only through the internal factory", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const subscription = yield* runtimeCore.internalLiveClient.subscribeInternal("orders", {
        select: ["id"],
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("rejects public leased topic queries before route validation", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(leasedViewServer, {});
      const missingRouteQuery = {
        where: [{ field: "region", type: "equals", filter: "usa" }],
        select: ["id"],
        limit: 1,
      } as const;

      const missingRouteEffect: Effect.Effect<unknown, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.snapshot,
        runtimeCore.client,
        ["orders", missingRouteQuery],
      );
      yield* expectRuntimeRejection(missingRouteEffect, publicLeasedRuntimeAccessError);

      const missingSubscribeRouteEffect: Effect.Effect<unknown, ViewServerRuntimeError> =
        Reflect.apply(runtimeCore.liveClient.subscribe, runtimeCore.liveClient, [
          "orders",
          missingRouteQuery,
        ]);
      yield* expectRuntimeRejection(missingSubscribeRouteEffect, publicLeasedRuntimeAccessError);

      const incompleteRouteEffect: Effect.Effect<unknown, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.liveClient.subscribe,
        runtimeCore.liveClient,
        [
          "orders",
          {
            where: [{ field: "status", type: "in", filter: ["open"] }],
            routeBy: { region: "usa" },
            select: ["id"],
            limit: 1,
          },
        ],
      );
      yield* expectRuntimeRejection(incompleteRouteEffect, publicLeasedRuntimeAccessError);

      yield* runtimeCore.close;
    }),
  );

  it.effect("rejects direct leased topic runtime access through the public runtime core", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(leasedViewServer, {});
      const leasedQuery = {
        where: [
          { field: "region", type: "equals", filter: "usa" },
          { field: "status", type: "equals", filter: "open" },
        ],
        routeBy: { region: "usa", status: "open" },
        select: ["id", "region", "status"],
        limit: 1,
      } as const;
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
      const snapshotEffect: Effect.Effect<unknown, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.snapshot,
        runtimeCore.client,
        ["orders", leasedQuery],
      );
      const subscribeEffect: Effect.Effect<unknown, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.liveClient.subscribe,
        runtimeCore.liveClient,
        ["orders", leasedQuery],
      );
      const resetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.client.reset,
        runtimeCore.client,
        [],
      );
      yield* expectRuntimeRejection(publishEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(publishManyEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(patchEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(deleteEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(snapshotEffect, publicLeasedRuntimeAccessError);
      yield* expectRuntimeRejection(subscribeEffect, publicLeasedRuntimeAccessError);
      yield* expectRuntimeRejection(resetEffect, publicSourceOwnedRuntimeResetError);

      yield* runtimeCore.close;
    }),
  );

  it.effect("keeps source-owned materialized topics readable but blocks public mutations", () =>
    Effect.gen(function* () {
      const kafkaRuntimeCore = yield* makeViewServerRuntimeCoreInternal(kafkaOwnedViewServer, {});
      const grpcRuntimeCore = yield* makeViewServerRuntimeCoreInternal(
        materializedGrpcSourceViewServer,
        {},
      );

      yield* kafkaRuntimeCore.internalClient.publish("orders", order("kafka", 10));
      yield* grpcRuntimeCore.internalClient.publish("orders", order("grpc", 20));

      const kafkaSnapshot = yield* kafkaRuntimeCore.publicClient.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const grpcSnapshot = yield* grpcRuntimeCore.publicClient.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const kafkaPublishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaRuntimeCore.publicClient.publish,
        kafkaRuntimeCore.publicClient,
        ["orders", order("blocked-kafka", 30)],
      );
      const kafkaPublishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaRuntimeCore.publicClient.publishMany,
        kafkaRuntimeCore.publicClient,
        ["orders", [order("blocked-kafka-many", 35)]],
      );
      const kafkaPatchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaRuntimeCore.publicClient.patch,
        kafkaRuntimeCore.publicClient,
        ["orders", "kafka", { price: 35 }],
      );
      const kafkaDeleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaRuntimeCore.publicClient.delete,
        kafkaRuntimeCore.publicClient,
        ["orders", "kafka"],
      );
      const grpcPublishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcRuntimeCore.publicClient.publish,
        grpcRuntimeCore.publicClient,
        ["orders", order("blocked-grpc", 40)],
      );
      const grpcPublishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcRuntimeCore.publicClient.publishMany,
        grpcRuntimeCore.publicClient,
        ["orders", [order("blocked-grpc-many", 45)]],
      );
      const grpcPatchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcRuntimeCore.publicClient.patch,
        grpcRuntimeCore.publicClient,
        ["orders", "grpc", { price: 45 }],
      );
      const grpcDeleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcRuntimeCore.publicClient.delete,
        grpcRuntimeCore.publicClient,
        ["orders", "grpc"],
      );
      const kafkaResetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaRuntimeCore.publicClient.reset,
        kafkaRuntimeCore.publicClient,
        [],
      );
      const grpcResetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcRuntimeCore.publicClient.reset,
        grpcRuntimeCore.publicClient,
        [],
      );

      expect(kafkaSnapshot).toStrictEqual({
        rows: [{ id: "kafka", price: 10 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(grpcSnapshot).toStrictEqual({
        rows: [{ id: "grpc", price: 20 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* expectRuntimeRejection(kafkaPublishEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(kafkaPublishManyEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(kafkaPatchEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(kafkaDeleteEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(grpcPublishEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(grpcPublishManyEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(grpcPatchEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(grpcDeleteEffect, publicSourceOwnedRuntimeMutationError);
      yield* expectRuntimeRejection(kafkaResetEffect, publicSourceOwnedRuntimeResetError);
      yield* expectRuntimeRejection(grpcResetEffect, publicSourceOwnedRuntimeResetError);

      yield* kafkaRuntimeCore.close;
      yield* grpcRuntimeCore.close;
    }),
  );

  it.effect("applies Source Ownership Policy to the neutral decoded mutation client", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(kafkaOwnedViewServer, {});
        yield* Effect.addFinalizer(() => runtimeCore.close);
        const checkError = yield* runtimeCore.decodedMutationClient
          .execute({
            _tag: "CheckMutationAllowed",
            topic: "orders",
          })
          .pipe(Effect.flip);
        const publishError = yield* runtimeCore.decodedMutationClient
          .execute({
            _tag: "PublishDecodedRows",
            topic: "orders",
            rows: [order("blocked", 10)],
          })
          .pipe(Effect.flip);
        const trustedPublishError = yield* runtimeCore.decodedMutationClient
          .execute(
            {
              _tag: "PublishDecodedRows",
              topic: "orders",
              rows: [order("blocked-trusted", 11)],
            },
            viewServerRuntimeDecodedMutationTrust,
          )
          .pipe(Effect.flip);
        const patchError = yield* runtimeCore.decodedMutationClient
          .execute({
            _tag: "PatchDecodedFields",
            topic: "orders",
            key: "blocked",
            patch: { price: 20 },
          })
          .pipe(Effect.flip);
        const trustedPatchError = yield* runtimeCore.decodedMutationClient
          .execute(
            {
              _tag: "PatchDecodedFields",
              topic: "orders",
              key: "blocked",
              patch: { price: 21 },
            },
            viewServerRuntimeDecodedMutationTrust,
          )
          .pipe(Effect.flip);
        const deleteError = yield* runtimeCore.decodedMutationClient
          .execute({
            _tag: "DeleteDecodedRow",
            topic: "orders",
            key: "blocked",
          })
          .pipe(Effect.flip);
        const resetError = yield* runtimeCore.client.reset().pipe(Effect.flip);

        expect([
          checkError,
          publishError,
          trustedPublishError,
          patchError,
          trustedPatchError,
          deleteError,
        ]).toStrictEqual([
          publicSourceOwnedRuntimeMutationError,
          publicSourceOwnedRuntimeMutationError,
          publicSourceOwnedRuntimeMutationError,
          publicSourceOwnedRuntimeMutationError,
          publicSourceOwnedRuntimeMutationError,
          publicSourceOwnedRuntimeMutationError,
        ]);
        expect(resetError).toStrictEqual(publicSourceOwnedRuntimeResetError);
      }),
    ),
  );

  it.effect("allows internal runtime core access for leased gRPC manager internals", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedViewServer, {});
      yield* runtimeCore.internalClient.publish("orders", order("a", 10));
      yield* runtimeCore.internalClient.publishManyWithStorageKeys("orders", [
        {
          storageKey: "orders/lease/row/public-b",
          row: order("public-b", 20),
        },
      ]);

      const snapshot = yield* runtimeCore.internalClient.snapshot("orders", {
        where: [
          { field: "customerId", type: "equals", filter: "customer-a" },
          { field: "region", type: "equals", filter: "usa" },
          { field: "status", type: "equals", filter: "open" },
        ],
        routeBy: { region: "usa", status: "open" },
        select: ["id", "region", "status"],
        limit: 1,
      });
      const storageKeySnapshot = yield* runtimeCore.internalClient.snapshot("orders", {
        where: [
          { field: "customerId", type: "equals", filter: "customer-public-b" },
          { field: "region", type: "equals", filter: "usa" },
          { field: "status", type: "equals", filter: "open" },
        ],
        routeBy: { region: "usa", status: "open" },
        select: ["id", "region", "status"],
        limit: 1,
      });
      const invalidRouteSnapshotEffect: Effect.Effect<unknown, ViewServerRuntimeError> =
        Reflect.apply(runtimeCore.internalClient.snapshot, runtimeCore.internalClient, [
          "orders",
          {
            where: [{ field: "region", type: "equals", filter: "usa" }],
            select: ["id"],
            limit: 1,
          },
        ]);
      const invalidRouteSnapshot = yield* Effect.flip(invalidRouteSnapshotEffect);
      const publicRuntimeSubscribe = yield* Effect.flip(
        runtimeCore.liveClient.subscribeRuntime("orders", {
          where: [
            { field: "customerId", type: "equals", filter: "customer-a" },
            { field: "region", type: "equals", filter: "usa" },
            { field: "status", type: "equals", filter: "open" },
          ],
          routeBy: { region: "usa", status: "open" },
          select: ["id"],
          limit: 1,
        }),
      );
      const subscription = yield* runtimeCore.internalLiveClient.subscribeInternal("orders", {
        where: [
          { field: "customerId", type: "equals", filter: "customer-a" },
          { field: "region", type: "equals", filter: "usa" },
          { field: "status", type: "equals", filter: "open" },
        ],
        routeBy: { region: "usa", status: "open" },
        select: ["id"],
        limit: 1,
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "a",
            region: "usa",
            status: "open",
          },
        ],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      expect(storageKeySnapshot).toStrictEqual({
        rows: [
          {
            id: "public-b",
            region: "usa",
            status: "open",
          },
        ],
        totalRows: 1,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      expect(invalidRouteSnapshot).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Leased topic orders requires routeBy fields: region, status.",
      });
      expect(publicRuntimeSubscribe).toStrictEqual(publicLeasedRuntimeAccessError);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 2,
        keys: ["a"],
        rows: [{ id: "a" }],
        totalRows: 1,
      });

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("rejects public leased subscriptions when effects execute", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedViewServer, {});
      const delayedSubscribeQuery = {
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
      const subscribeEffect: Effect.Effect<unknown, ViewServerRuntimeError> = Reflect.apply(
        runtimeCore.liveClient.subscribe,
        runtimeCore.liveClient,
        ["orders", delayedSubscribeQuery],
      );
      const subscribeRuntimeEffect = runtimeCore.liveClient.subscribeRuntime(
        "orders",
        delayedSubscribeQuery,
      );
      expect(Reflect.set(delayedSubscribeQuery.routeBy, "status", "closed")).toBe(true);

      const subscribeRouteError = yield* Effect.flip(subscribeEffect);
      expect(subscribeRouteError).toStrictEqual(publicLeasedRuntimeAccessError);
      const subscribeRuntimeRouteError = yield* Effect.flip(subscribeRuntimeEffect);
      expect(subscribeRuntimeRouteError).toStrictEqual(publicLeasedRuntimeAccessError);

      const delayedRuntimeQuery = {
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
      const subscribeProtocolQueryEffect = runtimeCore.serverLiveClient.subscribeProtocolQuery(
        "orders",
        trustDecodedRuntimeQuery(delayedRuntimeQuery),
      );
      expect(Reflect.set(delayedRuntimeQuery.routeBy, "status", "closed")).toBe(true);

      const subscribeProtocolQueryRouteError = yield* Effect.flip(subscribeProtocolQueryEffect);
      expect(subscribeProtocolQueryRouteError).toStrictEqual(publicLeasedRuntimeAccessError);

      yield* runtimeCore.close;
    }),
  );
});
