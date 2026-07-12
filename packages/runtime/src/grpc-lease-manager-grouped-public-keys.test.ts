import { describe, expect, it } from "@effect/vitest";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@effect-view-server/runtime-core/internal";
import { Deferred, Effect, Fiber, Queue, Stream } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import type { GrpcGroupedKeyRetentionView } from "./grpc-grouped-key-translations";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { makeDefaultGrpcClient } from "./grpc-source-lifecycle";

import { grpcOrderValue } from "../test-harness/grpc-config";
import { grpcGroupedKeyEncodingLeasedViewServer } from "../test-harness/grpc-grouped";
import {
  grpcLeasedViewServer,
  leasedGrpcViewServer,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
  routeEncodingValues,
} from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";

const presentGroupedFieldKey = (field: string, canonicalKey: string): string =>
  JSON.stringify([[field, JSON.stringify(["present", canonicalKey])]]);

const groupedPublicKey = (
  fields: ReadonlyArray<readonly [field: string, canonicalKey: string]>,
): string =>
  JSON.stringify(
    fields.map(([field, canonicalKey]) => [field, JSON.stringify(["present", canonicalKey])]),
  );

describe("gRPC lease manager grouped public-key translation and retention", () => {
  it.live("externalizes grouped leased gRPC keys that include the topic key field", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-order-1`, 10),
            grpcOrderValue(`${region}-order-2`, 20),
          ]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const first = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const firstEventQueue = yield* Queue.unbounded<unknown>();
      const firstEventsFiber = yield* first.events.pipe(
        Stream.runForEach((event) => Queue.offer(firstEventQueue, event)),
        Effect.forkChild,
      );
      const firstSnapshot = yield* Queue.take(firstEventQueue);
      const firstDelta = yield* Queue.take(firstEventQueue);
      const second = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const secondEvents = yield* second.events.pipe(Stream.take(1), Stream.runCollect);
      const firstPublicGroupKey = presentGroupedFieldKey("id", '"usa:usa-order-1"');
      const secondPublicGroupKey = presentGroupedFieldKey("id", '"usa:usa-order-2"');

      expect(firstSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(firstDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: firstPublicGroupKey,
            row: {
              id: "usa:usa-order-1",
              rowCount: 1n,
            },
            index: 0,
          },
          {
            type: "insert",
            key: secondPublicGroupKey,
            row: {
              id: "usa:usa-order-2",
              rowCount: 1n,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      expect(secondEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: [firstPublicGroupKey, secondPublicGroupKey],
        rows: [
          {
            id: "usa:usa-order-1",
            rowCount: 1n,
          },
          {
            id: "usa:usa-order-2",
            rowCount: 1n,
          },
        ],
        totalRows: 2,
      });

      yield* first.close();
      yield* second.close();
      yield* Fiber.interrupt(firstEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes grouped leased gRPC route values with public grouped keys", () =>
    Effect.gen(function* () {
      const feed = grpcGroupedKeyEncodingLeasedViewServer({
        acquire: () =>
          Stream.make(
            grpcOrderValue("route-encoding-1", 10),
            grpcOrderValue("route-encoding-2", 20),
            grpcOrderValue("route-encoding-3", 30),
          ),
        map: (value) => ({
          id: value.customerId,
          ...routeEncodingValues,
          meta: {
            desk: value.price === 30 ? "credit" : value.price === 20 ? "rates" : "equities",
          },
          tags:
            value.price === 30
              ? ["unsupported"]
              : value.price === 20
                ? ["slow", "shared"]
                : routeEncodingValues.tags,
          weird: null,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<typeof grpcOptions.sourceConfig.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        groupBy: [
          "amount",
          "count",
          "disabled",
          "flag",
          "none",
          "plainScore",
          "score",
          "text",
          "weird",
          "meta",
          "tags",
        ],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const deltaEvents = yield* subscription.events.pipe(
        Stream.filter((event) => event.type === "delta"),
        Stream.take(4),
        Stream.runCollect,
      );
      const commonGroupedKeyFields = [
        ["amount", '"123.45"'],
        ["count", '"9007199254740993"'],
        ["disabled", "false"],
        ["flag", "true"],
        ["none", "null"],
        ["plainScore", "42"],
        ["score", "0"],
        ["text", '"route"'],
        ["weird", "null"],
      ] satisfies ReadonlyArray<readonly [string, string]>;
      const publicGroupedKeyOne = groupedPublicKey([
        ...commonGroupedKeyFields,
        ["meta", '{"desk":"equities"}'],
        ["tags", '["fast","shared"]'],
      ]);
      const publicGroupedKeyTwo = groupedPublicKey([
        ...commonGroupedKeyFields,
        ["meta", '{"desk":"rates"}'],
        ["tags", '["slow","shared"]'],
      ]);
      const publicGroupedKeyThree = groupedPublicKey([
        ...commonGroupedKeyFields,
        ["meta", '{"desk":"credit"}'],
        ["tags", '["unsupported"]'],
      ]);

      expect(deltaEvents[0]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: publicGroupedKeyThree,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: 0,
              text: routeEncodingValues.text,
              weird: null,
              meta: {
                desk: "credit",
              },
              tags: ["unsupported"],
              rowCount: 1n,
            },
            index: 0,
          },
          {
            type: "insert",
            key: publicGroupedKeyOne,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: 0,
              text: routeEncodingValues.text,
              weird: null,
              meta: routeEncodingValues.meta,
              tags: routeEncodingValues.tags,
              rowCount: 1n,
            },
            index: 1,
          },
          {
            type: "insert",
            key: publicGroupedKeyTwo,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: 0,
              text: routeEncodingValues.text,
              weird: null,
              meta: {
                desk: "rates",
              },
              tags: ["slow", "shared"],
              rowCount: 1n,
            },
            index: 2,
          },
        ],
        totalRows: 3,
      });
      expect(deltaEvents[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyOne,
          },
        ],
        totalRows: 2,
      });
      expect(deltaEvents[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 2,
        toVersion: 3,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyTwo,
          },
        ],
        totalRows: 1,
      });
      expect(deltaEvents[3]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyThree,
          },
        ],
        totalRows: 0,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("bounds grouped-key translations to each leased subscription lifetime", () =>
    Effect.gen(function* () {
      const churnRows = Array.from({ length: 64 }, (_, index) => ({
        customerId: `customer-${index}`,
        rowCount: 1n,
      }));
      const churnInternalKeys = churnRows.map((_row, index) => `customer-internal-${index}`);
      const removalOperations: ReadonlyArray<{ readonly type: "remove"; readonly key: string }> =
        churnInternalKeys.slice(1).map((key) => ({ type: "remove", key }));
      const replacementRows = [
        { customerId: "replacement-a", rowCount: 1n },
        { customerId: "replacement-b", rowCount: 1n },
      ];
      const replacementInternalKeys = ["replacement-internal-a", "replacement-internal-b"];
      const customerSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "customer-groups",
        version: 0,
        keys: churnInternalKeys,
        rows: churnRows,
        totalRows: churnRows.length,
      } as const;
      const customerRemovalDelta = {
        type: "delta",
        topic: "orders",
        queryId: "customer-groups",
        fromVersion: 0,
        toVersion: 1,
        operations: removalOperations,
        totalRows: 1,
      } as const;
      const customerReplacementSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "customer-groups",
        version: 2,
        keys: replacementInternalKeys,
        rows: replacementRows,
        totalRows: replacementRows.length,
      } as const;
      const statusSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "status-groups",
        version: 0,
        keys: ["status-internal-open"],
        rows: [{ status: "open", rowCount: 64n }],
        totalRows: 1,
      } as const;
      const statusMoveDelta = {
        type: "delta",
        topic: "orders",
        queryId: "status-groups",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "update",
            key: "status-internal-open",
            row: { status: "open", rowCount: 65n },
            index: 0,
          },
          {
            type: "move",
            key: "status-internal-open",
            fromIndex: 0,
            toIndex: 0,
          },
        ],
        totalRows: 1,
      } as const;
      const publicGroupedStringKey = (field: string, value: string): string =>
        presentGroupedFieldKey(field, JSON.stringify(value));
      const releaseCustomerRemovals = yield* Deferred.make<void>();
      const releaseCustomerReplacement = yield* Deferred.make<void>();
      const releaseStatusMove = yield* Deferred.make<void>();
      const retentionViews: Array<GrpcGroupedKeyRetentionView> = [];
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {}),
        (runtimeCore) => {
          let subscriptionIndex = 0;
          const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
            typeof leasedGrpcViewServer.topics
          > = {
            ...runtimeCore.internalLiveClient,
            subscribeRuntimeObservedInternal: (_topic, _query, observer) => {
              if (subscriptionIndex === 0) {
                subscriptionIndex += 1;
                return observer.onQueryRegistered(customerSnapshot.queryId).pipe(
                  Effect.as({
                    events: Stream.make(customerSnapshot).pipe(
                      Stream.concat(
                        Stream.fromEffect(
                          Deferred.await(releaseCustomerRemovals).pipe(
                            Effect.as(customerRemovalDelta),
                          ),
                        ),
                      ),
                      Stream.concat(
                        Stream.fromEffect(
                          Deferred.await(releaseCustomerReplacement).pipe(
                            Effect.as(customerReplacementSnapshot),
                          ),
                        ),
                      ),
                      Stream.concat(Stream.never),
                    ),
                    close: () => Effect.void,
                  }),
                );
              }
              subscriptionIndex += 1;
              return observer.onQueryRegistered(statusSnapshot.queryId).pipe(
                Effect.as({
                  events: Stream.make(statusSnapshot).pipe(
                    Stream.concat(
                      Stream.fromEffect(
                        Deferred.await(releaseStatusMove).pipe(Effect.as(statusMoveDelta)),
                      ),
                    ),
                    Stream.concat(Stream.never),
                  ),
                  close: () => Effect.void,
                }),
              );
            },
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
              makeDefaultGrpcClient,
              (retention) => {
                retentionViews.push(retention);
              },
            ),
            (manager) =>
              Effect.acquireUseRelease(
                manager.liveClient.subscribeRuntime("orders", {
                  groupBy: ["customerId"],
                  aggregates: {
                    rowCount: { aggFunc: "count" },
                  },
                  where: {
                    region: { eq: "usa" },
                  },
                  limit: 100,
                }),
                (customerSubscription) =>
                  Effect.acquireUseRelease(
                    manager.liveClient.subscribeRuntime("orders", {
                      groupBy: ["status"],
                      aggregates: {
                        rowCount: { aggFunc: "count" },
                      },
                      where: {
                        region: { eq: "usa" },
                      },
                      limit: 10,
                    }),
                    (statusSubscription) =>
                      Effect.gen(function* () {
                        const customerRetention = yield* Effect.fromNullishOr(retentionViews[0]);
                        const statusRetention = yield* Effect.fromNullishOr(retentionViews[1]);
                        const customerEventQueue = yield* Queue.unbounded<unknown>();
                        const customerEventsFiber = yield* customerSubscription.events.pipe(
                          Stream.runForEach((event) => Queue.offer(customerEventQueue, event)),
                          Effect.forkChild({ startImmediately: true }),
                        );
                        const customerSnapshotEvent = yield* Queue.take(customerEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(customerRetention.retainedEntryCount()).toBe(64);

                        yield* Deferred.succeed(releaseCustomerRemovals, undefined);
                        const customerRemovalEvent = yield* Queue.take(customerEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(customerRetention.retainedEntryCount()).toBe(1);

                        yield* Deferred.succeed(releaseCustomerReplacement, undefined);
                        const customerReplacementEvent = yield* Queue.take(customerEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(customerRetention.retainedEntryCount()).toBe(2);
                        const customerEvents = [
                          customerSnapshotEvent,
                          customerRemovalEvent,
                          customerReplacementEvent,
                        ];
                        const statusEventQueue = yield* Queue.unbounded<unknown>();
                        const statusEventsFiber = yield* statusSubscription.events.pipe(
                          Stream.runForEach((event) => Queue.offer(statusEventQueue, event)),
                          Effect.forkChild({ startImmediately: true }),
                        );
                        const statusSnapshotEvent = yield* Queue.take(statusEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(statusRetention.retainedEntryCount()).toBe(1);

                        yield* customerSubscription.close();
                        yield* Fiber.interrupt(customerEventsFiber);
                        expect(customerRetention.retainedEntryCount()).toBe(0);
                        yield* Deferred.succeed(releaseStatusMove, undefined);
                        const statusMoveEvent = yield* Queue.take(statusEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        const statusEvents = [statusSnapshotEvent, statusMoveEvent];

                        expect(customerEvents).toStrictEqual([
                          {
                            ...customerSnapshot,
                            keys: churnRows.map((row) =>
                              publicGroupedStringKey("customerId", row.customerId),
                            ),
                          },
                          {
                            ...customerRemovalDelta,
                            operations: churnRows.slice(1).map((row) => ({
                              type: "remove",
                              key: publicGroupedStringKey("customerId", row.customerId),
                            })),
                          },
                          {
                            ...customerReplacementSnapshot,
                            keys: replacementRows.map((row) =>
                              publicGroupedStringKey("customerId", row.customerId),
                            ),
                          },
                        ]);
                        expect(statusEvents).toStrictEqual([
                          {
                            ...statusSnapshot,
                            keys: [publicGroupedStringKey("status", "open")],
                          },
                          {
                            ...statusMoveDelta,
                            operations: [
                              {
                                ...statusMoveDelta.operations[0],
                                key: publicGroupedStringKey("status", "open"),
                              },
                              {
                                ...statusMoveDelta.operations[1],
                                key: publicGroupedStringKey("status", "open"),
                              },
                            ],
                          },
                        ]);
                        yield* manager.close;
                        expect(statusRetention.retainedEntryCount()).toBe(0);
                        expect(customerRetention.retainedEntryCount()).toBe(0);
                        yield* manager.close;
                        expect(statusRetention.retainedEntryCount()).toBe(0);
                        yield* Fiber.interrupt(statusEventsFiber);
                      }).pipe(
                        Effect.ensuring(
                          Deferred.succeed(releaseCustomerRemovals, undefined).pipe(
                            Effect.andThen(Deferred.succeed(releaseCustomerReplacement, undefined)),
                            Effect.andThen(Deferred.succeed(releaseStatusMove, undefined)),
                          ),
                        ),
                      ),
                    (subscription) => subscription.close(),
                  ),
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
