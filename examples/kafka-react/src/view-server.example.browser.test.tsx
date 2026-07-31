import { describe, expect, it } from "@effect/vitest";
import {
  KafkaSourceAdapter,
  type KafkaMaterializedRegionMetrics,
} from "effect-view-server/kafka/contract";
import { makeInMemoryViewServerReact } from "effect-view-server/react/testing";
import { SourceAdapterServer } from "effect-view-server/source-adapter/server";
import { Chunk, Effect, Schedule, Stream } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { render } from "vitest-browser-react";
import { viewServerReact } from "./view-server.config";
import { KafkaExampleApp, sourceHealthStatusLabel } from "./view-server.example";

const emptyRegionMetrics = (region: string): KafkaMaterializedRegionMetrics => ({
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
    dueBacklog: 0,
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

const KafkaBrowserTest = SourceAdapterServer.make(KafkaSourceAdapter, {
  materialized: {
    applicationState: KafkaBrowserApplicationState,
    initialLaneIds: (input) => [input.definition.regions[0]],
    acquire: (input) =>
      Effect.gen(function* () {
        const row =
          input.toolkit.topic === "orders"
            ? {
                id: "usa:order-kafka-browser",
                customerId: "customer-kafka-browser",
                status: "open",
                price: 42,
                region: "usa",
                updatedAt: 1,
              }
            : {
                id: "london:trade-kafka-browser",
                symbol: "EVS",
                side: "buy",
                quantity: 1,
                region: "london",
                updatedAt: 1,
              };
        const mutation = yield* input.toolkit.decodeUpsert(row);
        const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
        const firstRegion = input.definition.regions[0];
        return SourceAdapterServer.attempt([
          SourceAdapterServer.lane({
            id: firstRegion,
            events: Stream.concat(Stream.make(delivery), Stream.never),
          }),
        ]);
      }),
    metrics: (input) => {
      const [firstRegion, ...remainingRegions] = input.definition.regions;
      return Effect.succeed({
        activeGroupId: "view-server-example-browser",
        start: { _tag: "Pending" },
        regions: [emptyRegionMetrics(firstRegion), ...remainingRegions.map(emptyRegionMetrics)],
      });
    },
    retry: Schedule.recurs(0),
  },
});

describe("Kafka React example", () => {
  it("renders real in-memory Source Health and Source-delivered rows", async () => {
    expect(
      sourceHealthStatusLabel(
        AsyncResult.initial<{
          readonly status: { readonly _tag: "Ready" };
        }>(),
      ),
    ).toBe("loading");
    const inMemory = await Effect.runPromise(
      makeInMemoryViewServerReact(viewServerReact).pipe(Effect.provide(KafkaBrowserTest)),
    );
    const screen = await render(
      <inMemory.ViewServerInMemoryProvider>
        <KafkaExampleApp />
      </inMemory.ViewServerInMemoryProvider>,
    );

    await expect
      .element(
        screen.getByRole("heading", {
          name: "Apache Kafka to View Server to React",
          exact: true,
        }),
      )
      .toBeVisible();
    await expect.element(screen.getByText("Orders source: Ready", { exact: true })).toBeVisible();
    await expect
      .element(screen.getByRole("cell", { name: "usa:order-kafka-browser", exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByRole("cell", { name: "customer-kafka-browser", exact: true }))
      .toBeVisible();

    await screen.unmount();
    await Effect.runPromise(inMemory.close);
  });
});
