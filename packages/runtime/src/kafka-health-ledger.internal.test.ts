import { describe, expect, it } from "@effect/vitest";
import { Consumer } from "@platformatic/kafka";
import { makeViewServerRuntimeCoreInternal as makeViewServerRuntimeCore } from "@effect-view-server/runtime-core/internal";
import { Buffer } from "node:buffer";
import { Cause, Deferred, Effect, Exit, Fiber, Logger, References, Scope } from "effect";
import type { ViewServerKafkaHealthLedger } from "./kafka-health";
import {
  recordKafkaAssignments,
  recordKafkaLag,
  registerKafkaConsumerHealthListeners,
} from "./kafka-ingress";
import {
  kafkaOptions,
  makeCapturedLogs,
  makeViewServerKafkaHealthLedger,
  nullRecord,
  ordersSourceTopic,
  paymentsSourceTopic,
  regions,
  type Topics,
  unknownSourceTopic,
  viewServer,
} from "../test-harness/kafka-ingress";

describe("Kafka ingress health ledger internals", () => {
  it.effect("records Kafka health transitions and ignores unknown ledger keys", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* ledger.regionRecovered("local", 1_000);
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 2, 1_000);
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "1",
        nowMillis: 1_000,
      });
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 20,
        committedOffset: "2",
        nowMillis: 2_000,
      });
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "bad-json",
        nowMillis: 2_000,
      });
      yield* ledger.regionConnected("missing", 2_000);
      yield* ledger.regionDegraded("missing", "ignored");
      yield* ledger.regionDegraded("local", "lag monitor failed");
      yield* ledger.regionRecovered("local", 2_000);
      yield* ledger.regionDisconnected("local", "lost");
      yield* ledger.regionRecovered("local", 2_000);
      yield* ledger.regionDisconnected("missing", "ignored");
      yield* ledger.regionRecovered("missing", 2_000);
      yield* ledger.topicConnected("missing", "local", 1, 2_000);
      yield* ledger.messageDecoded("missing", "local", {
        bytes: 1,
        committedOffset: "3",
        nowMillis: 2_000,
      });
      yield* ledger.decodeFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* ledger.mappingFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* ledger.messagePublishFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* ledger.messageCommitFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* ledger.messageSkippedCommitted("missing", "local", {
        committedOffset: "4",
        nowMillis: 2_000,
      });

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        status: health.status,
        kafka: health.kafka,
      }).toStrictEqual({
        status: "degraded",
        kafka: {
          startFrom: kafkaOptions.consume,
          regions: nullRecord({
            local: {
              status: "disconnected",
              brokers: regions.local,
              lastConnectedAt: 1_000,
              lastError: "lost",
            },
          }),
          topics: nullRecord({
            [ordersSourceTopic]: {
              status: "degraded",
              sourceTopic: ordersSourceTopic,
              viewServerTopic: "orders",
              regions: nullRecord({
                local: {
                  connected: false,
                  assignedPartitions: 0,
                  messagesPerSecond: 3,
                  bytesPerSecond: 35,
                  decodedMessagesPerSecond: 2,
                  decodeFailuresPerSecond: 1,
                  mappingFailuresPerSecond: 0,
                  publishFailuresPerSecond: 0,
                  commitFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: 2_000,
                  lastCommitAt: 2_000,
                  consumerLagMessages: null,
                  lagSampledAt: null,
                  committedOffset: "2",
                  lastError: "lost",
                },
              }),
            },
          }),
        },
      });

      const splitRegionLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
          [unknownSourceTopic]: {
            regions: ["cold"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* splitRegionLedger.regionRecovered("local", 3_000);
      const splitRegionHealth = splitRegionLedger.healthOverlay(
        yield* runtimeCore.client.health(),
        3_000,
      );
      expect(splitRegionHealth.kafka).toStrictEqual({
        startFrom: kafkaOptions.consume,
        regions: nullRecord({
          cold: {
            status: "starting",
            brokers: regions.cold,
            lastConnectedAt: null,
            lastError: null,
          },
          local: {
            status: "connected",
            brokers: regions.local,
            lastConnectedAt: 3_000,
            lastError: null,
          },
        }),
        topics: nullRecord({
          [ordersSourceTopic]: {
            status: "starting",
            sourceTopic: ordersSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: false,
                assignedPartitions: 0,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            }),
          },
          [unknownSourceTopic]: {
            status: "starting",
            sourceTopic: unknownSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              cold: {
                connected: false,
                assignedPartitions: 0,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            }),
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records Kafka assignments and lag samples", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* Effect.gen(function* () {
        const ledger = makeViewServerKafkaHealthLedger<Topics>({
          regions: kafkaOptions.regions,
          topics: {
            [ordersSourceTopic]: {
              regions: ["local"],
              viewServerTopic: "orders",
            },
          },
        });
        let healthRefreshRequestCount = 0;
        const requestHealthRefresh = Effect.sync(() => {
          healthRefreshRequestCount += 1;
        });
        yield* ledger.regionConnected("local", 1_000);
        yield* recordKafkaAssignments(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          [{ topic: ordersSourceTopic, partitions: [0, 1] }],
          1_000,
        );
        yield* recordKafkaLag(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic, unknownSourceTopic],
          new Map([
            [ordersSourceTopic, [3n, -1n, 2n]],
            [unknownSourceTopic, [99n]],
          ]),
          2_000,
        );
        const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect(healthRefreshRequestCount).toBe(2);
        expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
          status: "ready",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 2,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: null,
              lastCommitAt: null,
              consumerLagMessages: 5n,
              lagSampledAt: 2_000,
              committedOffset: null,
              lastError: null,
            },
          }),
        });
        expect(health.kafka?.topics[unknownSourceTopic]).toBeUndefined();
      }).pipe(Effect.ensuring(runtimeCore.close));
    }),
  );

  it.effect("keeps Kafka all-negative lag sentinels unknown", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* Effect.gen(function* () {
        const ledger = makeViewServerKafkaHealthLedger<Topics>({
          regions: kafkaOptions.regions,
          topics: {
            [ordersSourceTopic]: {
              regions: ["local"],
              viewServerTopic: "orders",
            },
          },
        });
        let healthRefreshRequestCount = 0;
        const requestHealthRefresh = Effect.sync(() => {
          healthRefreshRequestCount += 1;
        });

        yield* recordKafkaAssignments(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          [{ topic: ordersSourceTopic, partitions: [0] }],
          1_000,
        );
        yield* recordKafkaLag(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          new Map([[ordersSourceTopic, [5n]]]),
          1_500,
        );
        yield* recordKafkaLag(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          new Map([[ordersSourceTopic, [-1n]]]),
          2_000,
        );
        const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect(healthRefreshRequestCount).toBe(3);
        expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
          status: "ready",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: null,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: 2_000,
              committedOffset: null,
              lastError: null,
            },
          }),
        });
      }).pipe(Effect.ensuring(runtimeCore.close));
    }),
  );

  it.effect("keeps Kafka empty lag samples unknown", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* Effect.gen(function* () {
        const ledger = makeViewServerKafkaHealthLedger<Topics>({
          regions: kafkaOptions.regions,
          topics: {
            [ordersSourceTopic]: {
              regions: ["local"],
              viewServerTopic: "orders",
            },
          },
        });
        let healthRefreshRequestCount = 0;
        const requestHealthRefresh = Effect.sync(() => {
          healthRefreshRequestCount += 1;
        });

        yield* recordKafkaAssignments(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          [{ topic: ordersSourceTopic, partitions: [0] }],
          1_000,
        );
        yield* recordKafkaLag(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          new Map([[ordersSourceTopic, []]]),
          2_000,
        );
        const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect(healthRefreshRequestCount).toBe(2);
        expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
          status: "ready",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: null,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: 2_000,
              committedOffset: null,
              lastError: null,
            },
          }),
        });
      }).pipe(Effect.ensuring(runtimeCore.close));
    }),
  );

  it.effect("marks omitted configured source topics as unknown in full Kafka lag snapshots", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* Effect.gen(function* () {
        const ledger = makeViewServerKafkaHealthLedger<Topics>({
          regions: kafkaOptions.regions,
          topics: {
            [ordersSourceTopic]: {
              regions: ["local"],
              viewServerTopic: "orders",
            },
            [paymentsSourceTopic]: {
              regions: ["local"],
              viewServerTopic: "orders",
            },
          },
        });
        let healthRefreshRequestCount = 0;
        const requestHealthRefresh = Effect.sync(() => {
          healthRefreshRequestCount += 1;
        });

        yield* ledger.regionConnected("local", 1_000);
        yield* recordKafkaAssignments(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic, paymentsSourceTopic],
          [
            { topic: ordersSourceTopic, partitions: [0] },
            { topic: paymentsSourceTopic, partitions: [0, 1] },
          ],
          1_000,
        );
        yield* recordKafkaLag(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic, paymentsSourceTopic],
          new Map([
            [ordersSourceTopic, [5n]],
            [paymentsSourceTopic, [3n, 4n]],
          ]),
          2_000,
        );
        yield* recordKafkaLag(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic, paymentsSourceTopic],
          new Map([[ordersSourceTopic, [1n]]]),
          3_000,
        );
        const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 3_000);

        expect(healthRefreshRequestCount).toBe(3);
        expect({
          orders: health.kafka?.topics[ordersSourceTopic],
          payments: health.kafka?.topics[paymentsSourceTopic],
        }).toStrictEqual({
          orders: {
            status: "ready",
            sourceTopic: ordersSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 1n,
                lagSampledAt: 3_000,
                committedOffset: null,
                lastError: null,
              },
            }),
          },
          payments: {
            status: "ready",
            sourceTopic: paymentsSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: true,
                assignedPartitions: 2,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: 3_000,
                committedOffset: null,
                lastError: null,
              },
            }),
          },
        });
      }).pipe(Effect.ensuring(runtimeCore.close));
    }),
  );

  it.effect("keeps Kafka assignments authoritative when lag arrives after disconnect", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* Effect.gen(function* () {
        const ledger = makeViewServerKafkaHealthLedger<Topics>({
          regions: kafkaOptions.regions,
          topics: {
            [ordersSourceTopic]: {
              regions: ["local"],
              viewServerTopic: "orders",
            },
          },
        });
        let healthRefreshRequestCount = 0;
        const requestHealthRefresh = Effect.sync(() => {
          healthRefreshRequestCount += 1;
        });

        yield* ledger.regionConnected("local", 1_000);
        yield* recordKafkaAssignments(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          [{ topic: ordersSourceTopic, partitions: [0, 1] }],
          1_000,
        );
        yield* ledger.regionDisconnected("local", "Kafka consumer left group");
        yield* recordKafkaLag(
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          new Map([[ordersSourceTopic, [8n, -1n, 3n]]]),
          2_000,
        );
        const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect(healthRefreshRequestCount).toBe(2);
        expect({
          region: health.kafka?.regions["local"],
          topicStatus: health.kafka?.topics[ordersSourceTopic]?.status,
          topicRegion: health.kafka?.topics[ordersSourceTopic]?.regions["local"],
        }).toStrictEqual({
          region: {
            status: "disconnected",
            brokers: regions.local,
            lastConnectedAt: 1_000,
            lastError: "Kafka consumer left group",
          },
          topicStatus: "degraded",
          topicRegion: {
            connected: false,
            assignedPartitions: 0,
            messagesPerSecond: 0,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: null,
            lastCommitAt: null,
            consumerLagMessages: 11n,
            lagSampledAt: 2_000,
            committedOffset: null,
            lastError: "Kafka consumer left group",
          },
        });
      }).pipe(Effect.ensuring(runtimeCore.close));
    }),
  );

  it.effect("records Kafka health from consumer listener callbacks", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-test",
        groupId: "view-server-listener-test",
      });
      const scope = yield* Scope.make("parallel");
      yield* ledger.regionConnected("local", 1_000);
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        ledger,
        requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );
      yield* listenerRegistration.waitForProcessed(0);

      const degradedWait = yield* listenerRegistration
        .waitForProcessed(5)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
      });
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
      });
      consumer.emit("consumer:lag", new Map([[ordersSourceTopic, [4n, 1n]]]));
      consumer.emit("consumer:group:leave", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
      });
      consumer.emit("consumer:lag:error", new Error("lag read failed"));
      yield* Fiber.join(degradedWait);
      const degradedProcessed = yield* listenerRegistration.processed;
      const degradedHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        healthRefreshRequestCount,
        processed: degradedProcessed,
      }).toStrictEqual({
        healthRefreshRequestCount: 5,
        processed: 5,
      });
      expect({
        region: degradedHealth.kafka?.regions["local"],
        topicStatus: degradedHealth.kafka?.topics[ordersSourceTopic]?.status,
        topicRegion: degradedHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        region: {
          status: "degraded",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: "lag read failed",
        },
        topicStatus: "degraded",
        topicRegion: {
          connected: false,
          assignedPartitions: 0,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          commitFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: 5n,
          lagSampledAt: expect.any(Number),
          committedOffset: null,
          lastError: "lag read failed",
        },
      });

      const recoveredWait = yield* listenerRegistration
        .waitForProcessed(7)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
      });
      consumer.emit("consumer:lag", new Map([[ordersSourceTopic, [0n, 0n]]]));
      yield* Fiber.join(recoveredWait);
      const recoveredProcessed = yield* listenerRegistration.processed;
      const recoveredHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        healthRefreshRequestCount,
        processed: recoveredProcessed,
      }).toStrictEqual({
        healthRefreshRequestCount: 7,
        processed: 7,
      });
      expect({
        region: recoveredHealth.kafka?.regions["local"],
        topicStatus: recoveredHealth.kafka?.topics[ordersSourceTopic]?.status,
        topicRegion: recoveredHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: null,
        },
        topicStatus: "ready",
        topicRegion: {
          connected: true,
          assignedPartitions: 2,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          commitFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: 0n,
          lagSampledAt: expect.any(Number),
          committedOffset: null,
          lastError: null,
        },
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect("marks omitted Kafka listener lag topics as unknown without changing assignments", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
          [paymentsSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-omitted-lag-test",
        groupId: "view-server-listener-omitted-lag-test",
      });
      const scope = yield* Scope.make("parallel");
      yield* ledger.regionConnected("local", 1_000);
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        ledger,
        requestHealthRefresh,
        "local",
        [ordersSourceTopic, paymentsSourceTopic],
        scope,
      );
      yield* listenerRegistration.waitForProcessed(0);

      const processedWait = yield* listenerRegistration
        .waitForProcessed(3)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-omitted-lag-test",
        memberId: "member-1",
        assignments: [
          { topic: ordersSourceTopic, partitions: [0] },
          { topic: paymentsSourceTopic, partitions: [0, 1] },
        ],
      });
      consumer.emit(
        "consumer:lag",
        new Map([
          [ordersSourceTopic, [5n]],
          [paymentsSourceTopic, [9n]],
        ]),
      );
      consumer.emit("consumer:lag", new Map([[ordersSourceTopic, [0n]]]));
      yield* Fiber.join(processedWait);

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        healthRefreshRequestCount,
        orders: health.kafka?.topics[ordersSourceTopic]?.regions["local"],
        payments: health.kafka?.topics[paymentsSourceTopic]?.regions["local"],
      }).toStrictEqual({
        healthRefreshRequestCount: 3,
        orders: {
          connected: true,
          assignedPartitions: 1,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          commitFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: 0n,
          lagSampledAt: expect.any(Number),
          committedOffset: null,
          lastError: null,
        },
        payments: {
          connected: true,
          assignedPartitions: 2,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          commitFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: null,
          lagSampledAt: expect.any(Number),
          committedOffset: null,
          lastError: null,
        },
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect(
    "marks Kafka health disconnected during consumer group rebalance and recovers on join",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
        const ledger = makeViewServerKafkaHealthLedger<Topics>({
          regions: kafkaOptions.regions,
          topics: {
            [ordersSourceTopic]: {
              regions: ["local"],
              viewServerTopic: "orders",
            },
          },
        });
        let healthRefreshRequestCount = 0;
        const requestHealthRefresh = Effect.sync(() => {
          healthRefreshRequestCount += 1;
        });
        const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
          bootstrapBrokers: ["127.0.0.1:1"],
          clientId: "view-server-listener-rebalance-test",
          groupId: "view-server-listener-rebalance-test",
        });
        const scope = yield* Scope.make("parallel");
        yield* ledger.regionConnected("local", 1_000);
        const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
          consumer,
          ledger,
          requestHealthRefresh,
          "local",
          [ordersSourceTopic],
          scope,
        );

        const connectedWait = yield* listenerRegistration
          .waitForProcessed(1)
          .pipe(Effect.forkChild({ startImmediately: true }));
        consumer.emit("consumer:group:join", {
          groupId: "view-server-listener-rebalance-test",
          memberId: "member-1",
          assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        });
        yield* Fiber.join(connectedWait);
        const connectedHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

        expect(healthRefreshRequestCount).toBe(1);
        expect({
          region: connectedHealth.kafka?.regions["local"],
          topicRegion: connectedHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
        }).toStrictEqual({
          region: {
            status: "connected",
            brokers: regions.local,
            lastConnectedAt: expect.any(Number),
            lastError: null,
          },
          topicRegion: {
            connected: true,
            assignedPartitions: 2,
            messagesPerSecond: 0,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: null,
            lastCommitAt: null,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: null,
            lastError: null,
          },
        });

        const rebalanceWait = yield* listenerRegistration
          .waitForProcessed(2)
          .pipe(Effect.forkChild({ startImmediately: true }));
        consumer.emit("consumer:group:rebalance", {
          groupId: "view-server-listener-rebalance-test",
        });
        yield* Fiber.join(rebalanceWait);
        const rebalanceHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

        expect(healthRefreshRequestCount).toBe(2);
        expect({
          region: rebalanceHealth.kafka?.regions["local"],
          topicStatus: rebalanceHealth.kafka?.topics[ordersSourceTopic]?.status,
          topicRegion: rebalanceHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
        }).toStrictEqual({
          region: {
            status: "disconnected",
            brokers: regions.local,
            lastConnectedAt: expect.any(Number),
            lastError: "Kafka consumer group rebalance in progress",
          },
          topicStatus: "degraded",
          topicRegion: {
            connected: false,
            assignedPartitions: 0,
            messagesPerSecond: 0,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: null,
            lastCommitAt: null,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: null,
            lastError: "Kafka consumer group rebalance in progress",
          },
        });

        const recoveredWait = yield* listenerRegistration
          .waitForProcessed(3)
          .pipe(Effect.forkChild({ startImmediately: true }));
        consumer.emit("consumer:group:join", {
          groupId: "view-server-listener-rebalance-test",
          memberId: "member-1",
          assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
        });
        yield* Fiber.join(recoveredWait);
        const recoveredHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

        expect(healthRefreshRequestCount).toBe(3);
        expect({
          region: recoveredHealth.kafka?.regions["local"],
          topicStatus: recoveredHealth.kafka?.topics[ordersSourceTopic]?.status,
          topicRegion: recoveredHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
        }).toStrictEqual({
          region: {
            status: "connected",
            brokers: regions.local,
            lastConnectedAt: expect.any(Number),
            lastError: null,
          },
          topicStatus: "ready",
          topicRegion: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 0,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: null,
            lastCommitAt: null,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: null,
            lastError: null,
          },
        });

        yield* listenerRegistration.close;
        yield* Scope.close(scope, Exit.void);
        yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
        yield* runtimeCore.close;
      }),
  );

  it.effect("applies back-to-back Kafka rebalance and join health events in emit order", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const delayedDisconnectLedger: ViewServerKafkaHealthLedger<Topics> = {
        ...ledger,
        regionDisconnected: (region, message, options) =>
          Effect.promise(() => Promise.resolve()).pipe(
            Effect.andThen(ledger.regionDisconnected(region, message, options)),
          ),
      };
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-rebalance-order-test",
        groupId: "view-server-listener-rebalance-order-test",
      });
      const scope = yield* Scope.make("parallel");
      yield* ledger.regionConnected("local", 1_000);
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        delayedDisconnectLedger,
        requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );

      const connectedWait = yield* listenerRegistration
        .waitForProcessed(1)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-rebalance-order-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
      });
      yield* Fiber.join(connectedWait);

      const recoveredWait = yield* listenerRegistration
        .waitForProcessed(3)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.emit("consumer:group:rebalance", {
        groupId: "view-server-listener-rebalance-order-test",
      });
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-rebalance-order-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
      });
      yield* Fiber.join(recoveredWait);
      const recoveredHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(healthRefreshRequestCount).toBe(3);
      expect({
        region: recoveredHealth.kafka?.regions["local"],
        topicStatus: recoveredHealth.kafka?.topics[ordersSourceTopic]?.status,
        topicRegion: recoveredHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: null,
        },
        topicStatus: "ready",
        topicRegion: {
          connected: true,
          assignedPartitions: 1,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          commitFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: null,
          lagSampledAt: null,
          committedOffset: null,
          lastError: null,
        },
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect("snapshots fallback Kafka assignments when the join event is emitted", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const releaseLagError = yield* Deferred.make<void>();
      const blockingLedger: ViewServerKafkaHealthLedger<Topics> = {
        ...ledger,
        regionDegraded: (region, message) =>
          Deferred.await(releaseLagError).pipe(
            Effect.andThen(ledger.regionDegraded(region, message)),
          ),
      };
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-assignment-snapshot-test",
        groupId: "view-server-listener-assignment-snapshot-test",
      });
      const scope = yield* Scope.make("parallel");
      yield* ledger.regionConnected("local", 1_000);
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        blockingLedger,
        requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );

      consumer.emit("consumer:lag:error", new Error("block join processing"));
      const processedWait = yield* listenerRegistration
        .waitForProcessed(2)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.assignments = [{ topic: ordersSourceTopic, partitions: [0, 1] }];
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-assignment-snapshot-test",
        memberId: "member-1",
      });
      consumer.assignments = [{ topic: ordersSourceTopic, partitions: [0] }];
      yield* Deferred.succeed(releaseLagError, undefined);
      yield* Fiber.join(processedWait);
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(healthRefreshRequestCount).toBe(2);
      expect(health.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 2,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        decodedMessagesPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        commitFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: null,
        lastCommitAt: null,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: null,
        lastError: null,
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect("logs Kafka listener callback failures after applying ledger updates", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const failingLedger: ViewServerKafkaHealthLedger<Topics> = {
        ...ledger,
        topicConnected: (sourceTopic, region, assignedPartitions, nowMillis) =>
          ledger
            .topicConnected(sourceTopic, region, assignedPartitions, nowMillis)
            .pipe(Effect.andThen(Effect.die(new Error("listener ledger defect")))),
      };
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-failure-test",
        groupId: "view-server-listener-failure-test",
      });
      const scope = yield* Scope.make("parallel");
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        failingLedger,
        runtimeCore.requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );
      const processedWait = yield* listenerRegistration
        .waitForProcessed(1)
        .pipe(Effect.forkChild({ startImmediately: true }));

      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-failure-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
      });
      yield* Fiber.join(processedWait);
      const processed = yield* listenerRegistration.processed;
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);
      const log = logs[0];

      expect({
        logCauseHasDefect: Cause.hasDies(log?.cause ?? Cause.empty),
        logMessage: log?.message,
        logCount: logs.length,
        processed,
        region: health.kafka?.regions["local"],
        topicRegion: health.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        logCauseHasDefect: true,
        logMessage: ["Kafka health listener dispatch failed."],
        logCount: 1,
        processed: 1,
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: null,
        },
        topicRegion: {
          connected: true,
          assignedPartitions: 1,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          commitFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: null,
          lagSampledAt: null,
          committedOffset: null,
          lastError: null,
        },
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("does not log pure Kafka listener interruptions", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const interruptingLedger: ViewServerKafkaHealthLedger<Topics> = {
        ...ledger,
        regionDegraded: () => Effect.interrupt,
      };
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-interrupt-test",
        groupId: "view-server-listener-interrupt-test",
      });
      const scope = yield* Scope.make("parallel");
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        interruptingLedger,
        runtimeCore.requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );
      const processedWait = yield* listenerRegistration
        .waitForProcessed(2)
        .pipe(Effect.forkChild({ startImmediately: true }));

      consumer.emit("consumer:lag:error", new Error("interrupted lag failure"));
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-interrupt-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
      });
      yield* Fiber.join(processedWait);
      const processed = yield* listenerRegistration.processed;
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        logCount: logs.length,
        processed,
        region: health.kafka?.regions["local"],
        topicRegion: health.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        logCount: 0,
        processed: 2,
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: null,
        },
        topicRegion: {
          connected: true,
          assignedPartitions: 1,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          commitFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: null,
          lagSampledAt: null,
          committedOffset: null,
          lastError: null,
        },
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("closes Kafka listener scope while a health event is in flight", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const eventStarted = yield* Deferred.make<void>();
      const blockingLedger: ViewServerKafkaHealthLedger<Topics> = {
        ...ledger,
        regionDegraded: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(eventStarted, undefined);
            return yield* Effect.never;
          }),
      };
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-scope-close-test",
        groupId: "view-server-listener-scope-close-test",
      });
      const scope = yield* Scope.make("parallel");
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        blockingLedger,
        runtimeCore.requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );

      consumer.emit("consumer:lag:error", new Error("in-flight scope close"));
      yield* Deferred.await(eventStarted).pipe(Effect.timeout("1 second"));
      yield* Scope.close(scope, Exit.void).pipe(Effect.timeout("1 second"));
      const processed = yield* listenerRegistration.processed;

      expect(processed).toBe(1);

      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect("reports Kafka overlay ready and starting runtime statuses", () =>
    Effect.gen(function* () {
      const readyRuntimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const readyLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions: {
          local: regions.local,
        },
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* readyLedger.regionConnected("local", 1_000);
      yield* readyLedger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const readyHealth = readyLedger.healthOverlay(yield* readyRuntimeCore.client.health(), 1_000);

      const startingRuntimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const startingLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local", "cold"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* startingLedger.regionConnected("local", 2_000);
      yield* startingLedger.topicConnected(ordersSourceTopic, "local", 1, 2_000);
      const startingHealth = startingLedger.healthOverlay(
        yield* startingRuntimeCore.client.health(),
        2_000,
      );

      expect({
        ready: {
          status: readyHealth.status,
          kafka: readyHealth.kafka,
        },
        starting: {
          status: startingHealth.status,
          kafka: startingHealth.kafka,
        },
      }).toStrictEqual({
        ready: {
          status: "ready",
          kafka: {
            startFrom: kafkaOptions.consume,
            regions: nullRecord({
              local: {
                status: "connected",
                brokers: regions.local,
                lastConnectedAt: 1_000,
                lastError: null,
              },
            }),
            topics: nullRecord({
              [ordersSourceTopic]: {
                status: "ready",
                sourceTopic: ordersSourceTopic,
                viewServerTopic: "orders",
                regions: nullRecord({
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    publishFailuresPerSecond: 0,
                    commitFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    lagSampledAt: null,
                    committedOffset: null,
                    lastError: null,
                  },
                }),
              },
            }),
          },
        },
        starting: {
          status: "starting",
          kafka: {
            startFrom: kafkaOptions.consume,
            regions: nullRecord({
              cold: {
                status: "starting",
                brokers: regions.cold,
                lastConnectedAt: null,
                lastError: null,
              },
              local: {
                status: "connected",
                brokers: regions.local,
                lastConnectedAt: 2_000,
                lastError: null,
              },
            }),
            topics: nullRecord({
              [ordersSourceTopic]: {
                status: "starting",
                sourceTopic: ordersSourceTopic,
                viewServerTopic: "orders",
                regions: nullRecord({
                  cold: {
                    connected: false,
                    assignedPartitions: 0,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    publishFailuresPerSecond: 0,
                    commitFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    lagSampledAt: null,
                    committedOffset: null,
                    lastError: null,
                  },
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    publishFailuresPerSecond: 0,
                    commitFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    lagSampledAt: null,
                    committedOffset: null,
                    lastError: null,
                  },
                }),
              },
            }),
          },
        },
      });

      yield* readyRuntimeCore.close;
      yield* startingRuntimeCore.close;
    }),
  );

  it.effect("reports Kafka per-second counters over a rolling one-second window", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "1",
        nowMillis: 1_000,
      });

      const activeHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      const boundaryHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      const idleHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_001);

      expect(activeHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 1,
        messagesPerSecond: 1,
        bytesPerSecond: 10,
        decodedMessagesPerSecond: 1,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        commitFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 1_000,
        lastCommitAt: 1_000,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: "1",
        lastError: null,
      });
      expect(boundaryHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 1,
        messagesPerSecond: 1,
        bytesPerSecond: 10,
        decodedMessagesPerSecond: 1,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        commitFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 1_000,
        lastCommitAt: 1_000,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: "1",
        lastError: null,
      });
      expect(idleHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 1,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        decodedMessagesPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        commitFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 1_000,
        lastCommitAt: 1_000,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: "1",
        lastError: null,
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("keeps Kafka topic errors across assignment refresh until decoding succeeds", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "bad-json",
        nowMillis: 1_000,
      });
      yield* ledger.topicConnected(ordersSourceTopic, "local", 2, 2_000);
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(health.status).toBe("degraded");
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "degraded",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 2,
            messagesPerSecond: 1,
            bytesPerSecond: 5,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: 1_000,
            lastCommitAt: null,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: null,
            lastError: "bad-json",
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("recovers degraded topic status after successful Kafka decoding resumes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "bad-json",
        nowMillis: 1_000,
      });
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "2",
        nowMillis: 1_000,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(health.status).toBe("ready");
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "ready",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 2,
            bytesPerSecond: 15,
            decodedMessagesPerSecond: 1,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: 1_000,
            lastCommitAt: 1_000,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: "2",
            lastError: null,
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );
});
