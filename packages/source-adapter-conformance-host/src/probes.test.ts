import { describe, expect, it } from "@effect/vitest";
import {
  SourceAdapterConformanceRow,
  SourceFixture,
} from "@effect-view-server/source-adapter-testing";
import { Effect, Exit, Stream } from "effect";
import {
  invokeEffect,
  openHealth,
  openQuery,
  requireCallbackBridge,
  requireLeased,
  requireMaterialized,
  rows,
  snapshotHealth,
} from "./probes";

const runtimeHealth = (runtime: Readonly<Record<string, unknown>>) => ({
  _tag: "Ready",
  status: {
    _tag: "Ready",
    attempt: 1n,
  },
  metrics: {
    adapter: {
      observed: 1n,
    },
    runtime,
  },
});

describe("Source Adapter conformance host probes", () => {
  it("validates and snapshots generic Source Health values", () => {
    expect(snapshotHealth({ _tag: "Active", health: { _tag: "Inactive" } })).toStrictEqual({
      statusTag: "Inactive",
      attempt: undefined,
      retryAtNanos: undefined,
      adapterMetrics: undefined,
      rejectedItemCount: 0n,
      failedSettlementCount: 0n,
      lastRuntimeFailureTag: undefined,
      lastExecutionFailure: undefined,
      latestRejectionFailureTag: undefined,
      latestRejectionFailure: undefined,
      latestRejectionLocation: undefined,
      latestRejectedAtNanos: undefined,
      bufferHighWaterMark: 0,
      bufferOverflowCount: 0n,
    });
    expect(
      snapshotHealth(
        runtimeHealth({
          lanes: [
            null,
            { buffer: null },
            {
              buffer: {
                highWaterMark: "invalid",
                overflowCount: "invalid",
              },
            },
            {
              buffer: {
                highWaterMark: 3,
                overflowCount: 2n,
              },
            },
          ],
          rejectedItemCount: 4n,
          failedSettlementCount: 5n,
        }),
      ),
    ).toStrictEqual({
      statusTag: "Ready",
      attempt: 1n,
      retryAtNanos: undefined,
      adapterMetrics: {
        observed: 1n,
      },
      rejectedItemCount: 4n,
      failedSettlementCount: 5n,
      lastRuntimeFailureTag: undefined,
      lastExecutionFailure: undefined,
      latestRejectionFailureTag: undefined,
      latestRejectionFailure: undefined,
      latestRejectionLocation: undefined,
      latestRejectedAtNanos: undefined,
      bufferHighWaterMark: 3,
      bufferOverflowCount: 2n,
    });
    expect(
      snapshotHealth(
        runtimeHealth({
          lanes: "invalid",
          rejectedItemCount: 0n,
          failedSettlementCount: 0n,
        }),
      ).bufferHighWaterMark,
    ).toBe(0);

    expect(() => snapshotHealth(null)).toThrow("Source Health to be an object");
    expect(() => snapshotHealth({})).toThrow("status to be an object");
    expect(() =>
      snapshotHealth({
        status: { _tag: 1 },
        metrics: {
          runtime: {
            lanes: [],
            rejectedItemCount: 0n,
            failedSettlementCount: 0n,
          },
        },
      }),
    ).toThrow("_tag to be a string");
    expect(() =>
      snapshotHealth(
        runtimeHealth({
          lanes: [],
          rejectedItemCount: "invalid",
          failedSettlementCount: 0n,
        }),
      ),
    ).toThrow("rejectedItemCount to be a bigint");
  });

  it("extracts exact execution failures from retry and reacquisition states", () => {
    const failure = {
      _tag: "AdapterFailure",
      failure: {
        _tag: "PackageFailure",
        message: "offline",
      },
    };
    const snapshot = (status: Readonly<Record<string, unknown>>) =>
      snapshotHealth({
        _tag: "Ready",
        status,
        metrics: {
          adapter: {},
          runtime: {
            lanes: [],
            rejectedItemCount: 0n,
            failedSettlementCount: 0n,
          },
        },
      }).lastExecutionFailure;

    expect(
      snapshot({
        _tag: "WaitingToRetry",
        termination: {
          _tag: "Failed",
          failure,
        },
      }),
    ).toStrictEqual(failure);
    expect(
      snapshot({
        _tag: "Reacquiring",
        previousTermination: {
          _tag: "Failed",
          failure,
        },
      }),
    ).toStrictEqual(failure);
  });

  it("rejects missing and non-Effect dynamic methods", () => {
    expect(Exit.isFailure(Effect.runSyncExit(invokeEffect({}, "missing", [])))).toBe(true);
    expect(
      Exit.isFailure(
        Effect.runSyncExit(
          invokeEffect(
            {
              invalid: () => "not-an-effect",
            },
            "invalid",
            [],
          ),
        ),
      ),
    ).toBe(true);
    expect(
      Effect.runSync(
        invokeEffect(
          {
            valid: (value: unknown) => Effect.succeed(value),
          },
          "valid",
          ["result"],
        ),
      ),
    ).toBe("result");
  });

  it.effect("validates generic health and query subscription results", () =>
    Effect.gen(function* () {
      expect(
        Exit.isFailure(
          yield* openHealth({
            subscribeSourceHealth: () => Effect.succeed({}),
          }).pipe(Effect.exit),
        ),
      ).toBe(true);

      const health = yield* openHealth({
        subscribeSourceHealth: () =>
          Effect.succeed({
            events: Stream.make({ _tag: "Inactive" }),
            close: () => Effect.void,
          }),
      });
      expect(yield* health.events.pipe(Stream.runHead)).toStrictEqual(
        expect.objectContaining({
          value: expect.objectContaining({
            statusTag: "Inactive",
          }),
        }),
      );
      yield* health.close();

      expect(
        Exit.isFailure(
          yield* openQuery(
            {
              subscribe: () => Effect.succeed(null),
            },
            {},
          ).pipe(Effect.exit),
        ),
      ).toBe(true);
      const query = yield* openQuery(
        {
          subscribe: () =>
            Effect.succeed({
              events: Stream.empty,
              close: () => Effect.void,
            }),
        },
        {},
      );
      yield* query.close();
    }),
  );

  it.effect("rejects malformed snapshot subscriptions and rows", () =>
    Effect.gen(function* () {
      const runtime = (snapshot: unknown) => ({
        liveClient: {
          subscribe: () =>
            Effect.succeed({
              events: Stream.make({ type: "stale" }, snapshot),
              close: () => Effect.void,
            }),
        },
      });
      expect(
        Exit.isFailure(
          yield* rows({
            liveClient: {
              subscribe: () => Effect.succeed(null),
            },
          }).pipe(Effect.exit),
        ),
      ).toBe(true);
      expect(
        Exit.isFailure(
          yield* rows(runtime({ type: "snapshot", rows: "invalid" })).pipe(Effect.exit),
        ),
      ).toBe(true);
      expect(
        Exit.isFailure(yield* rows(runtime({ type: "snapshot", rows: [null] })).pipe(Effect.exit)),
      ).toBe(true);
      expect(
        Exit.isFailure(
          yield* rows(runtime({ type: "snapshot", rows: [{ id: 1 }] })).pipe(Effect.exit),
        ),
      ).toBe(true);
    }),
  );

  it.effect("requires each enabled driver capability", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(SourceAdapterConformanceRow);
      const driver = SourceFixture.conformanceDriver(fixture);
      expect(requireMaterialized(driver)).toBe(driver.materialized);
      expect(requireLeased(driver)).toBe(driver.leased);
      expect(requireCallbackBridge(driver)).toBe(driver.callbackBridge);

      expect(() => requireMaterialized({ ...driver, materialized: undefined })).toThrow(
        "omitted Materialized",
      );
      expect(() => requireLeased({ ...driver, leased: undefined })).toThrow("omitted Leased");
      expect(() =>
        requireCallbackBridge({
          ...driver,
          callbackBridge: undefined,
        }),
      ).toThrow("omitted its callback bridge");
    }),
  );
});
