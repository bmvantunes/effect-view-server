import { describe, expect, it } from "@effect/vitest";
import { grpcServerLayer, type GrpcRuntimeClient } from "effect-view-server/grpc/server";
import { kafkaServer, type KafkaServerRegion } from "effect-view-server/kafka/server";
import { makeViewServerRuntime } from "effect-view-server/runtime";
import { Deferred, Effect, Layer, Option, Scope, Stream } from "effect";
import { combinedService } from "./grpc-descriptors";
import { viewServer } from "./view-server.config";

const kafkaMetrics = (region: string) => ({
  region,
  assignments: [],
  commits: 0n,
  commitFailures: 0n,
  decoded: 0n,
  decodeFailures: 0n,
  mapped: 0n,
  mappingFailures: 0n,
  rejections: 0n,
  reconnects: 0n,
  rebalances: 0n,
  closes: 0n,
  closeFailures: 0n,
});

const makeKafkaRegion = (
  region: string,
  acquired: Deferred.Deferred<void>,
  finalized: Deferred.Deferred<void>,
): KafkaServerRegion => ({
  acquire: () =>
    Effect.gen(function* () {
      yield* Deferred.succeed(acquired, undefined);
      const scope = yield* Effect.scope;
      yield* Scope.addFinalizer(scope, Deferred.succeed(finalized, undefined).pipe(Effect.asVoid));
      return {
        records: Stream.never,
        recordDecoded: Effect.void,
        recordDecodeFailure: Effect.void,
        recordMapped: Effect.void,
        recordMappingFailure: Effect.void,
        recordRejection: Effect.void,
      };
    }),
  metrics: () => Effect.succeed(kafkaMetrics(region)),
});

const makeIdleGrpcClient = (
  invoked: Deferred.Deferred<string>,
  finalized: Deferred.Deferred<void>,
): GrpcRuntimeClient => ({
  service: combinedService,
  invoke: (method, _request, signal) => {
    Effect.runFork(Deferred.succeed(invoked, method));
    return {
      [Symbol.asyncIterator]() {
        return {
          next: () =>
            new Promise<IteratorResult<unknown>>((resolve) => {
              signal.addEventListener(
                "abort",
                () => {
                  resolve({ done: true, value: undefined });
                },
                { once: true },
              );
            }),
          return: () => {
            Effect.runFork(Deferred.succeed(finalized, undefined));
            return Promise.resolve({ done: true, value: undefined });
          },
        };
      },
    };
  },
});

const firstEvent = <A, E, R>(events: Stream.Stream<A, E, R>) =>
  Stream.runHead(events).pipe(Effect.map(Option.getOrThrow));

describe("combined-sources runtime composition", () => {
  it.live("composes real Kafka and gRPC aggregate Layers through the generic runtime port", () =>
    Effect.gen(function* () {
      const kafkaUsaAcquired = yield* Deferred.make<void>();
      const kafkaLondonAcquired = yield* Deferred.make<void>();
      const kafkaUsaFinalized = yield* Deferred.make<void>();
      const kafkaLondonFinalized = yield* Deferred.make<void>();
      const strategiesInvoked = yield* Deferred.make<string>();
      const ordersInvoked = yield* Deferred.make<string>();
      const strategiesFinalized = yield* Deferred.make<void>();
      const ordersFinalized = yield* Deferred.make<void>();
      const KafkaLive = kafkaServer.layer({
        consumerGroupPrefix: "combined-sources-integration",
        regions: new Map([
          ["usa", makeKafkaRegion("usa", kafkaUsaAcquired, kafkaUsaFinalized)],
          ["london", makeKafkaRegion("london", kafkaLondonAcquired, kafkaLondonFinalized)],
        ]),
      });
      const GrpcLive = grpcServerLayer(viewServer, {
        orders: makeIdleGrpcClient(ordersInvoked, ordersFinalized),
        strategies: makeIdleGrpcClient(strategiesInvoked, strategiesFinalized),
      });
      const SourcesLive = Layer.mergeAll(KafkaLive, GrpcLive);
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishHost: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      }).pipe(Effect.provide(SourcesLive));

      expect(runtime.tcpPublishUrl).toMatch(/^tcp:\/\/127\.0\.0\.1:\d+$/);
      yield* Deferred.await(kafkaUsaAcquired);
      yield* Deferred.await(kafkaLondonAcquired);
      expect(yield* Deferred.await(strategiesInvoked)).toBe("streamStrategies");

      const tradesHealthSubscription = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "trades",
      });
      const strategiesHealthSubscription = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "strategies",
      });
      const tradesHealth = yield* firstEvent(tradesHealthSubscription.events);
      const strategiesHealth = yield* firstEvent(strategiesHealthSubscription.events);

      expect({
        kafkaAdapter: tradesHealth.adapter.name,
        kafkaTarget: tradesHealth.target._tag,
        grpcAdapter: strategiesHealth.adapter.name,
        grpcTarget: strategiesHealth.target._tag,
      }).toStrictEqual({
        kafkaAdapter: "kafka",
        kafkaTarget: "Materialized",
        grpcAdapter: "grpc",
        grpcTarget: "Materialized",
      });

      const ordersSubscription = yield* runtime.liveClient.subscribe("orders", {
        routeBy: { strategyId: "strategy-1", region: "eu" },
        select: ["id", "customerId"],
      });
      expect(yield* Deferred.await(ordersInvoked)).toBe("streamOrders");
      const ordersHealthSubscription = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
        routeBy: { strategyId: "strategy-1", region: "eu" },
      });
      const ordersHealth = yield* firstEvent(ordersHealthSubscription.events);
      expect({
        adapter: ordersHealth._tag === "Active" ? ordersHealth.health.adapter.name : "inactive",
        route: ordersHealth.route,
        state: ordersHealth._tag,
      }).toStrictEqual({
        adapter: "grpc",
        route: { strategyId: "strategy-1", region: "eu" },
        state: "Active",
      });

      yield* ordersHealthSubscription.close();
      yield* ordersSubscription.close();
      yield* tradesHealthSubscription.close();
      yield* strategiesHealthSubscription.close();
      yield* runtime.close;
      yield* Deferred.await(kafkaUsaFinalized);
      yield* Deferred.await(kafkaLondonFinalized);
      yield* Deferred.await(strategiesFinalized);
      yield* Deferred.await(ordersFinalized);
    }),
  );
});
