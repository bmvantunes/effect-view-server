import { describe, expect, it } from "@effect/vitest";
import { Consumer } from "@platformatic/kafka";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Buffer } from "node:buffer";
import { Deferred, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { makeScopedKafkaDelivery } from "./kafka-delivery";
import { makeViewServerKafkaHealthLedger } from "./kafka-health";
import {
  makeViewServerKafkaHealthObserver,
  recordKafkaAssignments,
  recordKafkaLag,
  type ViewServerKafkaHealthObservation,
} from "./kafka-health-observation";
import { registerKafkaConsumerHealthListeners } from "./kafka-ingress";
import {
  kafkaOptions,
  nullRecord,
  ordersSourceTopic,
  regions,
  type Topics,
  viewServer,
} from "../test-harness/kafka-ingress";

const makeHealthLedger = () =>
  makeViewServerKafkaHealthLedger<Topics>({
    regions: kafkaOptions.regions,
    startFrom: kafkaOptions.consume,
    topics: {
      [ordersSourceTopic]: {
        regions: ["local"],
        viewServerTopic: "orders",
      },
    },
  });

describe("Kafka health observation", () => {
  it.effect("refreshes observed delivery health only on the bounded cadence", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const ledger = makeHealthLedger();
      let refreshes = 0;
      const observer = yield* makeViewServerKafkaHealthObserver(
        ledger,
        Effect.sync(() => {
          refreshes += 1;
        }),
        "1 second",
      );

      yield* observer.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "1",
        nowMillis: 1_000,
      });
      yield* observer.messageDecoded(ordersSourceTopic, "local", {
        bytes: 20,
        committedOffset: "2",
        nowMillis: 1_001,
      });
      yield* observer.messagePublishFailed(ordersSourceTopic, "local", {
        bytes: 30,
        message: "publish failed",
        nowMillis: 1_002,
      });

      expect(refreshes).toBe(0);
      yield* TestClock.adjust("999 millis");
      expect(refreshes).toBe(0);
      yield* TestClock.adjust("1 millis");
      expect(refreshes).toBe(1);

      yield* observer.messageCommitFailed(ordersSourceTopic, "local", {
        bytes: 40,
        message: "commit failed",
        nowMillis: 2_000,
      });
      yield* TestClock.adjust("1 second");
      expect(refreshes).toBe(2);

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect({
        commitFailuresPerSecond:
          health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.commitFailuresPerSecond,
        lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
        processingFailuresPerSecond:
          health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.processingFailuresPerSecond,
        publishFailuresPerSecond:
          health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.publishFailuresPerSecond,
      }).toStrictEqual({
        commitFailuresPerSecond: 1,
        lastError: "commit failed",
        processingFailuresPerSecond: 2,
        publishFailuresPerSecond: 1,
      });

      yield* observer.close;
      yield* runtimeCore.close;
    }),
  );

  it.effect("joins an in-flight cadence refresh before flushing observer shutdown", () =>
    Effect.gen(function* () {
      const ledger = makeHealthLedger();
      const refreshStarted = yield* Deferred.make<void>();
      const allowRefresh = yield* Deferred.make<void>();
      const closeCompleted = yield* Deferred.make<void>();
      let refreshes = 0;
      const observer = yield* makeViewServerKafkaHealthObserver(
        ledger,
        Deferred.succeed(refreshStarted, undefined).pipe(
          Effect.andThen(Deferred.await(allowRefresh)),
          Effect.andThen(
            Effect.sync(() => {
              refreshes += 1;
            }),
          ),
        ),
        "1 second",
      );

      yield* observer.regionConnected("local", 1_000);
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(refreshStarted);
      const close = yield* observer.close.pipe(
        Effect.ensuring(Deferred.succeed(closeCompleted, undefined)),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;

      expect(yield* Deferred.isDone(closeCompleted)).toBe(false);
      yield* Deferred.succeed(allowRefresh, undefined);
      yield* Fiber.join(close);
      expect(refreshes).toBe(1);
    }),
  );

  it.effect("routes assignment and lag observations through the same observer", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const ledger = makeHealthLedger();
      const observer = yield* makeViewServerKafkaHealthObserver(ledger, Effect.void, "1 second");

      yield* recordKafkaAssignments(
        observer,
        "local",
        [ordersSourceTopic],
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        1_000,
      );
      yield* recordKafkaLag(
        observer,
        "local",
        [ordersSourceTopic],
        new Map([[ordersSourceTopic, [4n, -1n, 2n]]]),
        2_000,
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

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
        consumerLagMessages: 6n,
        lagSampledAt: 2_000,
        committedOffset: null,
        lastError: null,
      });

      yield* observer.close;
      yield* runtimeCore.close;
    }),
  );

  it.effect("forwards region, skipped, decode, and mapping outcomes through one observer", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const ledger = makeHealthLedger();
      let refreshes = 0;
      const observer = yield* makeViewServerKafkaHealthObserver(
        ledger,
        Effect.sync(() => {
          refreshes += 1;
        }),
        "1 second",
      );

      yield* observer.regionConnected("local", 900);
      yield* observer.regionDisconnected("local", "broker disconnected");
      const disconnected = ledger.healthOverlay(yield* runtimeCore.client.health(), 900);
      expect(disconnected.kafka?.regions["local"]).toStrictEqual({
        status: "disconnected",
        brokers: regions.local,
        lastConnectedAt: 900,
        lastError: "broker disconnected",
      });

      yield* observer.regionDegraded("local", "lag sampling failed");
      yield* observer.messageSkippedCommitted(ordersSourceTopic, "local", {
        committedOffset: "7",
        nowMillis: 1_000,
      });
      yield* observer.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "decode failed",
        nowMillis: 1_001,
      });
      yield* observer.mappingFailed(ordersSourceTopic, "local", {
        bytes: 7,
        message: "mapping failed",
        nowMillis: 1_002,
      });

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_002);
      expect({
        committedOffset: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.committedOffset,
        decodeFailuresPerSecond:
          health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodeFailuresPerSecond,
        lastCommitAt: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastCommitAt,
        lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
        lastMessageAt: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastMessageAt,
        mappingFailuresPerSecond:
          health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.mappingFailuresPerSecond,
        region: health.kafka?.regions["local"],
      }).toStrictEqual({
        committedOffset: "7",
        decodeFailuresPerSecond: 1,
        lastCommitAt: 1_000,
        lastError: "mapping failed",
        lastMessageAt: 1_002,
        mappingFailuresPerSecond: 1,
        region: {
          status: "degraded",
          brokers: regions.local,
          lastConnectedAt: 900,
          lastError: "lag sampling failed",
        },
      });

      expect(refreshes).toBe(0);
      yield* observer.close;
      expect(refreshes).toBe(1);
      yield* runtimeCore.close;
    }),
  );

  it.effect("removes consumer-owned assignment, lag, and rate state when a source stops", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const ledger = makeHealthLedger();
      let refreshes = 0;
      const observer = yield* makeViewServerKafkaHealthObserver(
        ledger,
        Effect.sync(() => {
          refreshes += 1;
        }),
        "1 second",
      );

      yield* recordKafkaAssignments(
        observer,
        "local",
        [ordersSourceTopic],
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        1_000,
      );
      yield* recordKafkaLag(
        observer,
        "local",
        [ordersSourceTopic],
        new Map([[ordersSourceTopic, [5n, 3n]]]),
        1_000,
      );
      yield* observer.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "2",
        nowMillis: 1_000,
      });
      yield* observer.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "decode failed",
        nowMillis: 1_000,
      });
      yield* observer.mappingFailed(ordersSourceTopic, "local", {
        bytes: 6,
        message: "mapping failed",
        nowMillis: 1_000,
      });
      yield* observer.messagePublishFailed(ordersSourceTopic, "local", {
        bytes: 7,
        message: "publish failed",
        nowMillis: 1_000,
      });
      yield* observer.messageCommitFailed(ordersSourceTopic, "local", {
        bytes: 8,
        message: "commit failed",
        nowMillis: 1_000,
      });
      const beforeStop = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      expect({
        decodeFailuresPerSecond:
          beforeStop.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodeFailuresPerSecond,
        mappingFailuresPerSecond:
          beforeStop.kafka?.topics[ordersSourceTopic]?.regions["local"]?.mappingFailuresPerSecond,
        processingFailuresPerSecond:
          beforeStop.kafka?.topics[ordersSourceTopic]?.regions["local"]
            ?.processingFailuresPerSecond,
        publishFailuresPerSecond:
          beforeStop.kafka?.topics[ordersSourceTopic]?.regions["local"]?.publishFailuresPerSecond,
        commitFailuresPerSecond:
          beforeStop.kafka?.topics[ordersSourceTopic]?.regions["local"]?.commitFailuresPerSecond,
      }).toStrictEqual({
        decodeFailuresPerSecond: 1,
        mappingFailuresPerSecond: 1,
        processingFailuresPerSecond: 2,
        publishFailuresPerSecond: 1,
        commitFailuresPerSecond: 1,
      });
      yield* observer.regionStopped("local");
      yield* observer.regionStopped("unknown");

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      expect({
        region: health.kafka?.regions["local"],
        topic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        region: {
          status: "disconnected",
          brokers: regions.local,
          lastConnectedAt: 1_000,
          lastError: null,
        },
        topic: {
          status: "degraded",
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
              lastMessageAt: 1_000,
              lastCommitAt: 1_000,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: "2",
              lastError: "commit failed",
            },
          }),
        },
      });

      expect(refreshes).toBe(0);
      yield* observer.close;
      yield* observer.close;
      expect(refreshes).toBe(1);

      yield* runtimeCore.close;
    }),
  );

  it.effect("clears assignment and lag after an in-flight listener observation finishes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const ledger = makeHealthLedger();
      const assignmentRecorded = yield* Deferred.make<void>();
      const lagObservationStarted = yield* Deferred.make<void>();
      const allowLagObservation = yield* Deferred.make<void>();
      const workerStarted = yield* Deferred.make<void>();
      const observation = {
        ...ledger,
        topicConnected: (sourceTopic, region, assignedPartitions, nowMillis) =>
          ledger
            .topicConnected(sourceTopic, region, assignedPartitions, nowMillis)
            .pipe(Effect.andThen(Deferred.succeed(assignmentRecorded, undefined))),
        topicLagSampled: (sourceTopic, region, input) =>
          Deferred.succeed(lagObservationStarted, undefined).pipe(
            Effect.andThen(Deferred.await(allowLagObservation)),
            Effect.andThen(ledger.topicLagSampled(sourceTopic, region, input)),
            Effect.uninterruptible,
          ),
      } satisfies ViewServerKafkaHealthObservation<Topics>;
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-shutdown-observation-test",
        groupId: "view-server-shutdown-observation-test",
      });

      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.gen(function* () {
            const workerScope = yield* Effect.scope;
            yield* Effect.acquireRelease(
              registerKafkaConsumerHealthListeners(
                consumer,
                observation,
                "local",
                [ordersSourceTopic],
                workerScope,
              ),
              (registration) =>
                registration.close.pipe(
                  Effect.andThen(Effect.promise(() => Promise.resolve(consumer.close(true)))),
                  Effect.andThen(Deferred.succeed(allowLagObservation, undefined)),
                ),
            );
          }),
          () => Deferred.succeed(workerStarted, undefined).pipe(Effect.andThen(Effect.never)),
          ledger.regionStopped("local"),
        ),
      );

      yield* Deferred.await(workerStarted);
      consumer.emit("consumer:group:join", {
        groupId: "view-server-shutdown-observation-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
      });
      yield* Deferred.await(assignmentRecorded);
      consumer.emit("consumer:lag", new Map([[ordersSourceTopic, [7n, 5n]]]));
      yield* Deferred.await(lagObservationStarted);

      yield* delivery.close;

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);
      expect(health.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
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
      });

      yield* runtimeCore.close;
    }),
  );
});
