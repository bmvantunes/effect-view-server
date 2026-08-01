import { env } from "node:process";
import { Effect } from "effect";
import { closeRemoteTestResources } from "./resource-cleanup";

type Project = {
  readonly provide: (
    key: "viewServerRemoteUrl" | "viewServerSourceRemoteUrl" | "viewServerDiagnosticRemoteUrl",
    value: string,
  ) => void;
};

const skipRemoteGlobalSetup = (): boolean =>
  env["VIEW_SERVER_REACT_SKIP_REMOTE_GLOBAL_SETUP"] === "1";

export const setup = async (project: Project) => {
  if (skipRemoteGlobalSetup()) {
    project.provide("viewServerRemoteUrl", "ws://127.0.0.1:0/rpc");
    project.provide("viewServerSourceRemoteUrl", "ws://127.0.0.1:0/rpc");
    project.provide("viewServerDiagnosticRemoteUrl", "ws://127.0.0.1:0/rpc");
    return () => Promise.resolve();
  }

  const { ViewServerId, defineViewServerConfig } = await import("@effect-view-server/config");
  const { createInMemoryViewServerTesting, makeInMemoryViewServerTesting } =
    await import("@effect-view-server/in-memory/testing");
  const { makeViewServerWebSocketServer } = await import("@effect-view-server/server");
  const { SourceAdapter } = await import("@effect-view-server/source-adapter");
  const { SourceAdapterServer } = await import("@effect-view-server/source-adapter/server");
  const { Schedule, Schema, Stream } = await import("effect");

  const Order = Schema.Struct({
    id: ViewServerId,
    customerId: Schema.String,
    status: Schema.Literals(["open", "closed", "cancelled"]),
    price: Schema.Number,
    region: Schema.String,
    updatedAt: Schema.Number,
  });

  const Trade = Schema.Struct({
    id: ViewServerId,
    symbol: Schema.String,
    quantity: Schema.BigInt,
    price: Schema.Number,
    region: Schema.String,
  });

  const viewServer = defineViewServerConfig({
    topics: {
      orders: {
        schema: Order,
      },
      trades: {
        schema: Trade,
      },
    },
  });
  const SourceHealthOrder = Schema.Struct({
    id: ViewServerId,
    region: Schema.String,
  });
  const sourceAdapter = SourceAdapter.make({
    identity: { name: "react-browser-source" },
    failure: Schema.Never,
    materialized: {
      metrics: Schema.Struct({ observed: Schema.BigInt }),
      rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
      definitionOptions: SourceAdapter.definitionOptions<undefined>(),
    },
    leased: {
      metrics: Schema.Struct({ observed: Schema.BigInt }),
      rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
      definitionOptions: SourceAdapter.definitionOptions<undefined>(),
    },
  });
  const DiagnosticNonNegativeBigInt = Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n));
  const DiagnosticKafkaExpirationFailure = Schema.Struct({
    region: Schema.Literal("eu"),
    topic: Schema.Literal("source-orders"),
    id: Schema.NonEmptyString,
    generation: Schema.BigInt.check(Schema.isGreaterThanBigInt(0n)),
    failedAtNanos: DiagnosticNonNegativeBigInt,
    message: Schema.Literal("Kafka retention expiration Delete failed."),
  });
  const DiagnosticKafkaRetentionMetrics = Schema.Struct({
    declaredCleanupPolicy: Schema.Literal("compact-and-delete"),
    observedCleanupPolicy: Schema.Literal("compact-and-delete"),
    configuredRetention: Schema.TaggedStruct("Finite", {
      durationNanos: Schema.Literal(5_000_000_000n),
    }),
    resolvedRetention: Schema.TaggedStruct("Finite", {
      durationNanos: Schema.Literal(5_000_000_000n),
    }),
    trackedRows: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    lastSweepRetryableFailures: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    expiredRows: DiagnosticNonNegativeBigInt,
    authoritativeExpiredDeletes: DiagnosticNonNegativeBigInt,
    failedWorkBacklog: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    expirationRetryFailures: DiagnosticNonNegativeBigInt,
    latestExpirationFailure: Schema.NullOr(DiagnosticKafkaExpirationFailure),
    lastSweepAtNanos: Schema.NullOr(DiagnosticNonNegativeBigInt),
    lastSweepDurationNanos: Schema.NullOr(DiagnosticNonNegativeBigInt),
    sweepIntervalNanos: Schema.Literal(900_000_000_000n),
  });
  const DiagnosticKafkaMetrics = Schema.Struct({
    activeGroupId: Schema.Literal("browser:diagnostics"),
    start: Schema.TaggedStruct("Resolved", {
      position: Schema.Struct({
        mode: Schema.Literal("earliest"),
      }),
    }),
    regions: Schema.Tuple([
      Schema.Struct({
        region: Schema.Literal("eu"),
        assignments: Schema.Array(
          Schema.Struct({
            partition: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
            offset: DiagnosticNonNegativeBigInt,
            lag: DiagnosticNonNegativeBigInt,
          }),
        ),
        commits: DiagnosticNonNegativeBigInt,
        commitFailures: DiagnosticNonNegativeBigInt,
        decoded: DiagnosticNonNegativeBigInt,
        decodeFailures: DiagnosticNonNegativeBigInt,
        mapped: DiagnosticNonNegativeBigInt,
        mappingFailures: DiagnosticNonNegativeBigInt,
        rejections: DiagnosticNonNegativeBigInt,
        reconnects: DiagnosticNonNegativeBigInt,
        rebalances: DiagnosticNonNegativeBigInt,
        closes: DiagnosticNonNegativeBigInt,
        closeFailures: DiagnosticNonNegativeBigInt,
        retention: DiagnosticKafkaRetentionMetrics,
      }),
    ]),
  });
  const DiagnosticKafkaRejectionLocation = Schema.Struct({
    region: Schema.Literal("eu"),
    topic: Schema.Literal("source-orders"),
    partition: Schema.Literal(0),
    offset: DiagnosticNonNegativeBigInt,
    phase: Schema.Literal("mapping"),
    message: Schema.NonEmptyString,
  });
  const diagnosticRemoteAdapter = SourceAdapter.make({
    identity: {
      name: "kafka",
      version: "1",
    },
    failure: Schema.Never,
    materialized: {
      metrics: DiagnosticKafkaMetrics,
      rejectionLocation: DiagnosticKafkaRejectionLocation,
      definitionOptions: SourceAdapter.definitionOptions<undefined>(),
    },
    leased: undefined,
  });
  const DiagnosticRemoteRow = Schema.Struct({
    id: ViewServerId,
    value: Schema.String,
  });
  const diagnosticRemoteViewServer = defineViewServerConfig({
    topics: {
      diagnostics: {
        schema: DiagnosticRemoteRow,
        source: diagnosticRemoteAdapter.materializedSource(undefined),
      },
    },
  });
  const sourceAdapterLayer = SourceAdapterServer.make(sourceAdapter, {
    materialized: {
      acquire: () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "react-browser",
              events: Stream.never,
            }),
          ]),
        ),
      metrics: () => Effect.succeed({ observed: 1n }),
      retry: Schedule.recurs(0),
    },
    leased: {
      acquire: () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "react-browser",
              events: Stream.never,
            }),
          ]),
        ),
      metrics: () => Effect.succeed({ observed: 1n }),
      retry: Schedule.recurs(0),
    },
  });
  const sourceHealthViewServer = defineViewServerConfig({
    topics: {
      orders: {
        schema: SourceHealthOrder,
        source: sourceAdapter.leasedSource(["region"], undefined),
      },
    },
  });

  const runtimeCore = createInMemoryViewServerTesting(viewServer);
  const server = await Effect.runPromise(
    makeViewServerWebSocketServer(viewServer, {
      liveClient: runtimeCore.serverLiveClient,
      runtime: runtimeCore.client,
    }),
  );
  project.provide("viewServerRemoteUrl", server.url);
  const sourceRuntimeCore = await Effect.runPromise(
    makeInMemoryViewServerTesting(sourceHealthViewServer, {}).pipe(
      Effect.provide(sourceAdapterLayer),
    ),
  );
  const sourceServer = await Effect.runPromise(
    makeViewServerWebSocketServer(sourceHealthViewServer, {
      liveClient: sourceRuntimeCore.serverLiveClient,
      runtime: sourceRuntimeCore.client,
    }),
  );
  project.provide("viewServerSourceRemoteUrl", sourceServer.url);
  const diagnosticAdapterMetrics = {
    activeGroupId: "browser:diagnostics",
    start: {
      _tag: "Resolved",
      position: {
        mode: "earliest",
      },
    },
    regions: [
      {
        region: "eu",
        assignments: [],
        commits: 7n,
        commitFailures: 1n,
        decoded: 9n,
        decodeFailures: 1n,
        mapped: 8n,
        mappingFailures: 1n,
        rejections: 1n,
        reconnects: 0n,
        rebalances: 0n,
        closes: 0n,
        closeFailures: 0n,
        retention: {
          declaredCleanupPolicy: "compact-and-delete",
          observedCleanupPolicy: "compact-and-delete",
          configuredRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          resolvedRetention: {
            _tag: "Finite",
            durationNanos: 5_000_000_000n,
          },
          trackedRows: 3,
          lastSweepRetryableFailures: 1,
          expiredRows: 4n,
          authoritativeExpiredDeletes: 1n,
          failedWorkBacklog: 1,
          expirationRetryFailures: 2n,
          latestExpirationFailure: {
            region: "eu",
            topic: "source-orders",
            id: "eu:0:azEyMw",
            generation: 4n,
            failedAtNanos: 99n,
            message: "Kafka retention expiration Delete failed.",
          },
          lastSweepAtNanos: 98n,
          lastSweepDurationNanos: 2n,
          sweepIntervalNanos: 900_000_000_000n,
        },
      },
    ],
  } as const;
  const diagnosticRuntimeMetrics = {
    startedAtNanos: 1n,
    lastAttemptStartedAtNanos: 1n,
    lastDeliveryAtNanos: 90n,
    lastRejectionAtNanos: 95n,
    lastAppliedMutationAtNanos: 90n,
    lastTerminationAtNanos: null,
    currentAttempt: 1n,
    retryCount: 0n,
    receivedDeliveryCount: 9n,
    rejectedItemCount: 1n,
    attemptedMutationCount: 8n,
    appliedUpsertCount: 7n,
    appliedDeleteCount: 1n,
    failedMutationCount: 1n,
    completedSettlementCount: 7n,
    failedSettlementCount: 1n,
    retainedRowCount: 3,
    lanes: [
      {
        id: "eu",
        buffer: {
          _tag: "Unbuffered",
        },
      },
    ],
  } as const;
  const sourceHealth = <const Status extends object>(status: Status, sampledAtNanos: bigint) => ({
    adapter: {
      name: "kafka",
      version: "1",
    },
    target: {
      _tag: "Materialized",
    },
    status,
    metrics: {
      runtime: diagnosticRuntimeMetrics,
      adapter: diagnosticAdapterMetrics,
    },
    sampledAtNanos,
  });
  const maintenanceOnly = sourceHealth(
    {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 100n,
      reasons: [{ _tag: "AdapterMaintenanceFailure" }],
    },
    101n,
  );
  const combined = sourceHealth(
    {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 100n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidSourceDelivery",
                message: "Kafka mapped row does not satisfy the Topic Schema.",
              },
            },
            location: {
              region: "eu",
              topic: "source-orders",
              partition: 0,
              offset: 42n,
              phase: "mapping",
              message: "Kafka Mapping rejected the record.",
            },
            rejectedAtNanos: 95n,
          },
        },
        {
          _tag: "AdapterMaintenanceFailure",
        },
      ],
    },
    102n,
  );
  const recovered = sourceHealth(
    {
      _tag: "Ready",
      attempt: 1n,
      readyAtNanos: 150n,
    },
    151n,
  );
  const newEpisode = sourceHealth(
    {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 200n,
      reasons: [{ _tag: "AdapterMaintenanceFailure" }],
    },
    201n,
  );
  const invalidSettlement = sourceHealth(
    {
      _tag: "Exhausted",
      exhaustion: {
        _tag: "RetryExhausted",
        lastTermination: {
          _tag: "Failed",
          failure: {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidSourceSettlement",
              message: "Source Settlement callback threw before returning an Effect",
            },
          },
        },
      },
      exhaustedAtNanos: 300n,
    },
    301n,
  );
  const diagnosticAdapterLayer = SourceAdapterServer.make(diagnosticRemoteAdapter, {
    materialized: {
      acquire: () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "eu",
              events: Stream.never,
            }),
          ]),
        ),
      metrics: () => Effect.succeed(diagnosticAdapterMetrics),
      retry: Schedule.recurs(0),
    },
  });
  const diagnosticRuntimeCore = await Effect.runPromise(
    makeInMemoryViewServerTesting(diagnosticRemoteViewServer, {}).pipe(
      Effect.provide(diagnosticAdapterLayer),
    ),
  );
  const diagnosticLiveClient = {
    ...diagnosticRuntimeCore.serverLiveClient,
    subscribeProtocolSourceHealth: () =>
      Effect.succeed({
        events: Stream.make(maintenanceOnly).pipe(
          Stream.concat(Stream.fromEffect(Effect.sleep("300 millis").pipe(Effect.as(combined)))),
          Stream.concat(Stream.fromEffect(Effect.sleep("300 millis").pipe(Effect.as(recovered)))),
          Stream.concat(Stream.fromEffect(Effect.sleep("300 millis").pipe(Effect.as(newEpisode)))),
          Stream.concat(
            Stream.fromEffect(Effect.sleep("300 millis").pipe(Effect.as(invalidSettlement))),
          ),
        ),
        close: () => Effect.void,
      }),
  };
  const diagnosticServer = await Effect.runPromise(
    makeViewServerWebSocketServer(diagnosticRemoteViewServer, {
      liveClient: diagnosticLiveClient,
      runtime: diagnosticRuntimeCore.client,
    }),
  );
  project.provide("viewServerDiagnosticRemoteUrl", diagnosticServer.url);

  return async () => {
    await Effect.runPromise(
      closeRemoteTestResources([
        server.close,
        runtimeCore.close,
        sourceServer.close,
        sourceRuntimeCore.close,
        diagnosticServer.close,
        diagnosticRuntimeCore.close,
      ]),
    );
  };
};
