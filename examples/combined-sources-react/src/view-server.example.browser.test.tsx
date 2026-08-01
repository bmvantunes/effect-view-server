import { describe, expect, it } from "@effect/vitest";
import {
  GrpcSourceAdapter,
  type GrpcLeasedMetrics,
  type GrpcMaterializedMetrics,
} from "effect-view-server/grpc/contract";
import {
  KafkaSourceAdapter,
  type KafkaMaterializedRegionMetrics,
} from "effect-view-server/kafka/contract";
import { makeInMemoryViewServerReact } from "effect-view-server/react/testing";
import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
import { Chunk, Effect, Layer, Schedule, Scope, Stream } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { render } from "vitest-browser-react";
import { viewServerReact } from "./view-server.config";
import {
  CombinedSourcesExampleApp,
  leasedSourceHealthLabel,
  materializedSourceHealthLabel,
} from "./view-server.example";

const kafkaRegionMetrics = (region: string): KafkaMaterializedRegionMetrics => ({
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
  retention: {
    declaredCleanupPolicy: "delete",
    observedCleanupPolicy: "delete",
    configuredRetention: { _tag: "Forever" },
    resolvedRetention: { _tag: "Forever" },
    trackedRows: 0,
    lastSweepRetryableFailures: 0,
    expiredRows: 0n,
    authoritativeExpiredDeletes: 0n,
    failedWorkBacklog: 0,
    expirationRetryFailures: 0n,
    latestExpirationFailure: null,
    lastSweepAtNanos: null,
    lastSweepDurationNanos: null,
    sweepIntervalNanos: 900_000_000_000n,
  },
});

const KafkaBrowserApplicationState = SourceAdapterServer.applicationState({
  sweepIntervalNanos: 900_000_000_000n,
  initialState: () => undefined,
  reduce: () => undefined,
  metrics: () => undefined,
  runDueSweep: () => Effect.void,
});

const grpcMetrics = (activeFeeds: bigint): GrpcLeasedMetrics => ({
  logicalClient: "browser",
  method: "browser",
  activeInvocations: activeFeeds,
  acquisitionCount: 1n,
  reconnectCount: 0n,
  completionCount: 0n,
  streamFailureCount: 0n,
  messageCount: 1n,
  mappedMessageCount: 1n,
  rejectedMessageCount: 0n,
  cancellationCount: 0n,
  finalizationCount: 0n,
  finalizationFailureCount: 0n,
  activeFeeds,
});

const grpcMaterializedMetrics = (): GrpcMaterializedMetrics => {
  const { activeFeeds: _activeFeeds, ...metrics } = grpcMetrics(1n);
  return metrics;
};

describe("combined sources React example", () => {
  it("renders real adapter-delivered rows and Topic-bound Source Health", async () => {
    expect(materializedSourceHealthLabel(AsyncResult.initial())).toBe("loading");
    expect(leasedSourceHealthLabel(AsyncResult.initial())).toBe("loading");
    expect(
      leasedSourceHealthLabel(
        AsyncResult.success({
          _tag: "Inactive",
        }),
      ),
    ).toBe("grpc / Inactive");
    const acquisitions = { grpcLeased: 0, grpcMaterialized: 0, kafka: 0 };
    const GrpcBrowserTest = SourceAdapterServer.make(GrpcSourceAdapter, {
      materialized: {
        acquire: (input) =>
          Effect.gen(function* () {
            acquisitions.grpcMaterialized += 1;
            yield* Scope.addFinalizer(
              yield* Effect.scope,
              Effect.sync(() => {
                acquisitions.grpcMaterialized -= 1;
              }),
            );
            const mutation = yield* input.toolkit.decodeUpsert({
              id: "strategy-alpha:usa",
              strategyId: "strategy-alpha",
              region: "usa",
              status: "active",
              notional: 100,
              updatedAt: 1,
            });
            const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
            return SourceAdapterServer.attempt([
              SourceAdapterServer.lane({
                id: "grpc-materialized-browser",
                events: Stream.concat(Stream.make(delivery), Stream.never),
              }),
            ]);
          }),
        metrics: () => Effect.succeed(grpcMaterializedMetrics()),
        retry: Schedule.recurs(0),
      },
      leased: {
        acquire: (input) =>
          Effect.gen(function* () {
            acquisitions.grpcLeased += 1;
            yield* Scope.addFinalizer(
              yield* Effect.scope,
              Effect.sync(() => {
                acquisitions.grpcLeased -= 1;
              }),
            );
            const mutation = yield* input.toolkit.decodeUpsert({
              id: "strategy-alpha:usa:customer-browser",
              customerId: "customer-browser",
              status: "open",
              price: 15,
              region: "usa",
              strategyId: "strategy-alpha",
              updatedAt: 1,
            });
            const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
            return SourceAdapterServer.attempt([
              SourceAdapterServer.lane({
                id: "grpc-leased-browser",
                events: Stream.concat(Stream.make(delivery), Stream.never),
              }),
            ]);
          }),
        metrics: () => Effect.succeed(grpcMetrics(BigInt(acquisitions.grpcLeased))),
        retry: Schedule.recurs(0),
      },
    });
    const KafkaBrowserTest = SourceAdapterServer.make(KafkaSourceAdapter, {
      materialized: {
        applicationState: KafkaBrowserApplicationState,
        initialLaneIds: (input) => [input.definition.regions[0]],
        acquire: (input) =>
          Effect.gen(function* () {
            acquisitions.kafka += 1;
            yield* Scope.addFinalizer(
              yield* Effect.scope,
              Effect.sync(() => {
                acquisitions.kafka -= 1;
              }),
            );
            const mutation = yield* input.toolkit.decodeUpsert({
              id: "london:trade-browser",
              symbol: "EFFECT",
              side: "buy",
              quantity: 7,
              region: "london",
              updatedAt: 2,
            });
            const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
            return SourceAdapterServer.attempt([
              SourceAdapterServer.lane({
                id: input.definition.regions[0],
                events: Stream.concat(Stream.make(delivery), Stream.never),
              }),
            ]);
          }),
        metrics: (input) => {
          const [firstRegion, ...remainingRegions] = input.definition.regions;
          return Effect.succeed({
            activeGroupId: "combined-sources-browser",
            start: { _tag: "Pending" },
            regions: [kafkaRegionMetrics(firstRegion), ...remainingRegions.map(kafkaRegionMetrics)],
          });
        },
        retry: Schedule.recurs(0),
      },
    });
    const inMemory = await Effect.runPromise(
      makeInMemoryViewServerReact(viewServerReact).pipe(
        Effect.provide(Layer.mergeAll(GrpcBrowserTest, KafkaBrowserTest)),
      ),
    );
    const screen = await render(
      <inMemory.ViewServerInMemoryProvider>
        <CombinedSourcesExampleApp />
      </inMemory.ViewServerInMemoryProvider>,
    );

    await expect
      .element(
        screen.getByRole("heading", {
          name: "Kafka plus leased and materialized gRPC",
          exact: true,
        }),
      )
      .toBeVisible();
    await expect
      .element(screen.getByText("Leased orders source: grpc / Ready", { exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByText("Materialized strategies source: grpc / Ready", { exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByText("Kafka trades source: kafka / Ready", { exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByText("strategy-alpha:usa:customer-browser", { exact: true }))
      .toBeVisible();
    await expect.element(screen.getByText("strategy-alpha:usa", { exact: true })).toBeVisible();
    await expect.element(screen.getByText("london:trade-browser", { exact: true })).toBeVisible();
    expect(acquisitions).toStrictEqual({
      grpcLeased: 1,
      grpcMaterialized: 1,
      kafka: 1,
    });

    await screen.unmount();
    await Effect.runPromise(inMemory.close);
    await expect
      .poll(() => acquisitions)
      .toStrictEqual({
        grpcLeased: 0,
        grpcMaterialized: 0,
        kafka: 0,
      });
  });
});
