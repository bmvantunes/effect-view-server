import { describe, expect, it } from "@effect/vitest";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";

import { grpcViewServer } from "../test-harness/grpc-config";
import { leasedGrpcViewServer } from "../test-harness/grpc-leased";

import type { GrpcTopics } from "../test-harness/grpc-config";

describe("gRPC runtime health", () => {
  it.live("tracks gRPC materialized feed health and same-window rate increments", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });

      const startingHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      yield* health.clientConnected("missing", 1_000);
      yield* health.clientDegraded("missing", "ignored");
      yield* health.feedReady("missing");
      yield* health.feedStopping("missing");
      yield* health.feedDegraded("missing", "ignored");
      yield* health.rowsPublished("missing", {
        messages: 1,
        rows: 1,
        nowMillis: 2_000,
      });
      yield* health.mappingFailed("missing", {
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* health.publishFailed("missing", {
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* health.clientConnected("orders", 1_000);
      yield* health.feedReady("ordersFeed");
      yield* health.rowsPublished("ordersFeed", {
        messages: 1,
        rows: 2,
        nowMillis: 2_000,
      });
      yield* health.rowsPublished("ordersFeed", {
        messages: 3,
        rows: 4,
        nowMillis: 2_000,
      });
      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        startingStatus: startingHealth.status,
        startingActiveFeeds: startingHealth.grpc?.clients["orders"]?.activeFeeds,
        readyStatus: readyHealth.status,
        readyActiveFeeds: readyHealth.grpc?.clients["orders"]?.activeFeeds,
        materialized: readyHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"],
      }).toStrictEqual({
        startingStatus: "starting",
        startingActiveFeeds: 0,
        readyStatus: "ready",
        readyActiveFeeds: 1,
        materialized: {
          status: "ready",
          lifecycle: "materialized",
          feedName: "ordersFeed",
          feedKey: "orders/ordersFeed/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 4,
          rowsPerSecond: 6,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: 2_000,
          lastError: null,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.live(
    "tracks active leased gRPC feed health in the ledger without pre-registering leases",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
        const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
          clients: {
            orders: "https://orders.example.test",
          },
          feeds: {},
        });
        const ordersLeaseKey = "orders/ordersLease/leased";

        const startingHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
        yield* health.clientConnected("orders", 1_000);
        yield* health.leasedFeedStarting({
          feedName: "ordersLease",
          feedKey: ordersLeaseKey,
          topic: "orders",
          clientName: "orders",
        });
        yield* health.feedReady(ordersLeaseKey);
        const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
        yield* health.subscriberAdded("missingLease");
        yield* health.subscriberRemoved("missingLease");
        yield* health.subscriberRemoved(ordersLeaseKey);
        yield* health.leasedFeedRemoved("missingLease");
        yield* health.leasedFeedStarting({
          feedName: "orphanLease",
          feedKey: "orders/orphanLease/leased/region=string%3A3%3Ausa",
          topic: "orders",
          clientName: "orphan",
        });
        yield* health.leasedFeedRemoved("orders/orphanLease/leased/region=string%3A3%3Ausa");
        yield* health.leasedFeedStarting({
          feedName: "degradedLease",
          feedKey: "orders/degradedLease/leased/region=string%3A3%3Ausa",
          topic: "orders",
          clientName: "orders",
        });
        yield* health.feedDegraded(
          "orders/degradedLease/leased/region=string%3A3%3Ausa",
          "leased route failed",
        );
        yield* health.leasedFeedStarting({
          feedName: "degradedLeaseTwo",
          feedKey: "orders/degradedLeaseTwo/leased/region=string%3A6%3Alondon",
          topic: "orders",
          clientName: "orders",
        });
        yield* health.feedDegraded(
          "orders/degradedLeaseTwo/leased/region=string%3A6%3Alondon",
          "second leased route failed",
        );
        yield* health.clientDegraded("orders", "leased route failed");
        const degradedMixedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_500);
        yield* health.leasedFeedRemoved("orders/degradedLease/leased/region=string%3A3%3Ausa");
        const stillDegradedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_750);
        yield* health.leasedFeedRemoved(
          "orders/degradedLeaseTwo/leased/region=string%3A6%3Alondon",
        );
        const afterDefensiveRemovalHealth = health.healthOverlay(
          yield* runtimeCore.client.health(),
          3_000,
        );

        expect({
          startingActiveFeeds: startingHealth.grpc?.clients["orders"]?.activeFeeds,
          startingFeedKeys: Object.keys(startingHealth.grpc?.feeds["orders"]?.leased ?? {}),
          readyActiveFeeds: readyHealth.grpc?.clients["orders"]?.activeFeeds,
          readyFeed: readyHealth.grpc?.feeds["orders"]?.leased[ordersLeaseKey],
          degradedMixedClientStatus: degradedMixedHealth.grpc?.clients["orders"]?.status,
          degradedMixedRuntimeStatus: degradedMixedHealth.status,
          stillDegradedClientStatus: stillDegradedHealth.grpc?.clients["orders"]?.status,
          stillDegradedClientError: stillDegradedHealth.grpc?.clients["orders"]?.lastError,
          subscriberCountAfterNoops:
            afterDefensiveRemovalHealth.grpc?.feeds["orders"]?.leased[ordersLeaseKey]
              ?.subscriberCount,
          clientStatusAfterRemovingDegradedLease:
            afterDefensiveRemovalHealth.grpc?.clients["orders"]?.status,
          clientErrorAfterRemovingDegradedLease:
            afterDefensiveRemovalHealth.grpc?.clients["orders"]?.lastError,
          defensiveRemovalFeedKeys: Object.keys(
            afterDefensiveRemovalHealth.grpc?.feeds["orders"]?.leased ?? {},
          ),
        }).toStrictEqual({
          startingActiveFeeds: 0,
          startingFeedKeys: [],
          readyActiveFeeds: 1,
          readyFeed: {
            status: "ready",
            lifecycle: "leased",
            feedName: "ordersLease",
            feedKey: "orders/ordersLease/leased",
            topic: "orders",
            subscriberCount: 0,
            rowCount: 0,
            messagesPerSecond: 0,
            rowsPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            reconnects: 0,
            lastMessageAt: null,
            lastError: null,
          },
          degradedMixedClientStatus: "degraded",
          degradedMixedRuntimeStatus: "degraded",
          stillDegradedClientStatus: "degraded",
          stillDegradedClientError: "second leased route failed",
          subscriberCountAfterNoops: 0,
          clientStatusAfterRemovingDegradedLease: "connected",
          clientErrorAfterRemovingDegradedLease: null,
          defensiveRemovalFeedKeys: [ordersLeaseKey],
        });

        yield* runtimeCore.close;
      }),
  );

  it.live(
    "keeps gRPC client starting when removing a leased feed leaves starting materialized work",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
        const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
          clients: {
            orders: "https://orders.example.test",
          },
          feeds: {
            ordersFeed: {
              client: "orders",
              lifecycle: "materialized",
              topic: "orders",
            },
            ordersLease: {
              client: "orders",
              lifecycle: "leased",
              topic: "orders",
            },
          },
        });

        yield* health.clientConnected("orders", 1_000);
        yield* health.leasedFeedRemoved("orders/ordersLease/leased");
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect({
          client: currentHealth.grpc?.clients["orders"],
          materializedStatus:
            currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status,
          leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
          runtimeStatus: currentHealth.status,
        }).toStrictEqual({
          client: {
            status: "starting",
            baseUrl: "https://orders.example.test",
            activeFeeds: 0,
            lastConnectedAt: 1_000,
            lastError: null,
          },
          materializedStatus: "starting",
          leasedFeedKeys: [],
          runtimeStatus: "starting",
        });

        yield* runtimeCore.close;
      }),
  );
});
