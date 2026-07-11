import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { viewServerDecodeHealth } from "./index";

import {
  grpcFeedHealth,
  kafkaStartFromHealth,
  topicHealth,
  viewServer,
  wireHealth,
} from "../test-harness/protocol";

describe("Invalid health payload wire inputs", () => {
  it.effect("rejects invalid runtime health and feed payloads", () =>
    Effect.gen(function* () {
      const missingHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { orders: topicHealth } },
        }),
      );

      expect(missingHealthTopic.message).toBe("Health payload is missing topic: badjson");

      const extraHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { ...wireHealth.engine.topics, missing: topicHealth } },
        }),
      );

      expect(extraHealthTopic.message).toBe("Health payload references unknown topic: missing");

      const reservedExtraHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { ...wireHealth.engine.topics, __view_server_health: topicHealth } },
        }),
      );

      expect(reservedExtraHealthTopic.message).toBe(
        "Health payload references unknown topic: __view_server_health",
      );

      const healthWithExtras = {
        ...wireHealth,
        extraRoot: "drop-me",
        engine: {
          topics: {
            orders: { ...topicHealth, extraTopic: "drop-me" },
            badjson: { ...topicHealth, rowCount: 0, liveRowCount: 0, extraTopic: "drop-me" },
          },
        },
        transport: { ...wireHealth.transport, extraTransport: "drop-me" },
      };

      const normalizedHealth = yield* viewServerDecodeHealth(viewServer, healthWithExtras);

      expect(Object.hasOwn(normalizedHealth, "extraRoot")).toBe(false);

      expect(Object.hasOwn(normalizedHealth.transport, "extraTransport")).toBe(false);

      expect(Object.hasOwn(normalizedHealth.engine.topics["orders"], "extraTopic")).toBe(false);

      const malformedHealthStatus = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          // @ts-expect-error hostile runtime adapters can return malformed health status.
          status: "broken",
        }),
      );

      expect(malformedHealthStatus.message).toMatch(/Invalid health payload/);

      const malformedHealthTransport = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          transport: {
            ...wireHealth.transport,
            // @ts-expect-error hostile runtime adapters can return malformed health counters.
            activeClients: "1",
          },
        }),
      );

      expect(malformedHealthTransport.message).toMatch(/Invalid health payload/);

      const validKafkaViewServerTopic = yield* viewServerDecodeHealth(viewServer, {
        ...wireHealth,
        kafka: {
          startFrom: kafkaStartFromHealth,
          regions: {},
          topics: {
            ordersSource: {
              status: "ready",
              sourceTopic: "orders-source",
              viewServerTopic: "orders",
              regions: {},
            },
          },
        },
      });

      expect(validKafkaViewServerTopic.kafka?.topics["ordersSource"]?.viewServerTopic).toBe(
        "orders",
      );

      const unknownKafkaViewServerTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          kafka: {
            startFrom: kafkaStartFromHealth,
            regions: {},
            topics: {
              ordersSource: {
                status: "ready",
                sourceTopic: "orders-source",
                viewServerTopic: "missing",
                regions: {},
              },
            },
          },
        }),
      );

      expect(unknownKafkaViewServerTopic.message).toBe(
        "Health payload references unknown topic: missing",
      );

      const validGrpcHealth = yield* viewServerDecodeHealth(viewServer, {
        ...wireHealth,
        grpc: {
          clients: {
            orders: {
              status: "connected",
              baseUrl: "https://orders.example.test",
              activeFeeds: 1,
              lastConnectedAt: 100,
              lastError: null,
            },
          },
          feeds: {
            orders: {
              materialized: {
                ordersFeed: grpcFeedHealth,
              },
              leased: {},
            },
          },
        },
      });

      expect(validGrpcHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]).toStrictEqual(
        grpcFeedHealth,
      );

      const leasedFeedInMaterializedBucket = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          grpc: {
            clients: {},
            feeds: {
              orders: {
                materialized: {
                  ordersFeed: { ...grpcFeedHealth, lifecycle: "leased" },
                },
                leased: {},
              },
            },
          },
        }),
      );

      expect(leasedFeedInMaterializedBucket.message).toBe(
        "Health payload materialized feed has leased lifecycle: ordersFeed",
      );

      const unknownGrpcFeedGroupTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          grpc: {
            clients: {},
            feeds: {
              missing: {
                materialized: {
                  ordersFeed: { ...grpcFeedHealth, topic: "missing" },
                },
                leased: {},
              },
            },
          },
        }),
      );

      expect(unknownGrpcFeedGroupTopic.message).toBe(
        "Health payload references unknown topic: missing",
      );

      const unknownGrpcFeedTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          grpc: {
            clients: {},
            feeds: {
              orders: {
                materialized: {
                  ordersFeed: { ...grpcFeedHealth, topic: "missing" },
                },
                leased: {},
              },
            },
          },
        }),
      );

      expect(unknownGrpcFeedTopic.message).toBe(
        "Health payload feed topic does not match feed group: missing != orders",
      );

      const mismatchedGrpcFeedTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          grpc: {
            clients: {},
            feeds: {
              orders: {
                materialized: {
                  ordersFeed: { ...grpcFeedHealth, topic: "badjson" },
                },
                leased: {},
              },
            },
          },
        }),
      );

      expect(mismatchedGrpcFeedTopic.message).toBe(
        "Health payload feed topic does not match feed group: badjson != orders",
      );

      const validLeasedGrpcHealth = yield* viewServerDecodeHealth(viewServer, {
        ...wireHealth,
        grpc: {
          clients: {},
          feeds: {
            orders: {
              materialized: {},
              leased: {
                ordersLease: {
                  ...grpcFeedHealth,
                  lifecycle: "leased",
                  feedName: "ordersLease",
                  feedKey: "orders/ordersLease/region=usa",
                  subscriberCount: 2,
                },
              },
            },
          },
        },
      });

      expect(validLeasedGrpcHealth.grpc?.feeds["orders"]?.leased["ordersLease"]).toStrictEqual({
        ...grpcFeedHealth,
        lifecycle: "leased",
        feedName: "ordersLease",
        feedKey: "orders/ordersLease/region=usa",
        subscriberCount: 2,
      });

      const materializedFeedInLeasedBucket = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          grpc: {
            clients: {},
            feeds: {
              orders: {
                materialized: {},
                leased: {
                  ordersLease: {
                    ...grpcFeedHealth,
                    feedName: "ordersLease",
                  },
                },
              },
            },
          },
        }),
      );

      expect(materializedFeedInLeasedBucket.message).toBe(
        "Health payload leased feed has materialized lifecycle: ordersLease",
      );

      const mismatchedLeasedGrpcFeedTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          grpc: {
            clients: {},
            feeds: {
              orders: {
                materialized: {},
                leased: {
                  ordersLease: {
                    ...grpcFeedHealth,
                    lifecycle: "leased",
                    feedName: "ordersLease",
                    topic: "badjson",
                  },
                },
              },
            },
          },
        }),
      );

      expect(mismatchedLeasedGrpcFeedTopic.message).toBe(
        "Health payload feed topic does not match feed group: badjson != orders",
      );
    }),
  );
});
