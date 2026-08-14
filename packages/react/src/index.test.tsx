import { describe, expect, inject, it, vi } from "@effect/vitest";
import type { ViewServerLiveClient } from "@effect-view-server/client";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  ViewServerId,
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
  type Where,
} from "@effect-view-server/config";
import { createInMemoryViewServer as createCoreInMemoryViewServer } from "@effect-view-server/in-memory";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { Cause, Effect, Match, Option, Queue, Schedule, Schema, Scope, Stream } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { Component, Suspense, type ReactNode } from "react";
import { render } from "vitest-browser-react";
import { createViewServerReact } from "./index";
import {
  deleteMapEntryIfCurrent,
  installMapEntryIfVacant,
  ViewServerReactClientProvider,
} from "./internal";
import {
  createInMemoryViewServerReact,
  makeInMemoryViewServerReact,
  type ViewServerInMemoryOptions,
} from "./testing";

declare module "vitest" {
  export interface ProvidedContext {
    readonly viewServerRemoteUrl: string;
    readonly viewServerSourceRemoteUrl: string;
    readonly viewServerDiagnosticRemoteUrl: string;
  }
}

describe("Source Health cache entry helpers", () => {
  it("keeps a newer cache entry when an older finalizer runs late", () => {
    const original = {};
    const replacement = {};
    const entries = new Map([["health", replacement]]);

    deleteMapEntryIfCurrent(entries, "health", original);

    expect(entries.get("health")).toBe(replacement);
    deleteMapEntryIfCurrent(entries, "health", replacement);
    expect(entries.has("health")).toBe(false);
  });

  it("does not replace a newer entry when an older atom recomputes", () => {
    const original = {};
    const replacement = {};
    const entries = new Map([["health", replacement]]);

    installMapEntryIfVacant(entries, "health", original);
    expect(entries.get("health")).toBe(replacement);

    entries.delete("health");
    installMapEntryIfVacant(entries, "health", original);
    expect(entries.get("health")).toBe(original);
  });
});

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

const sourceHealthViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: SourceHealthOrder,
      source: sourceAdapter.leasedSource(["region"], undefined),
    },
  },
});

const diagnosticHealthViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: SourceHealthOrder,
      source: sourceAdapter.materializedSource(undefined),
    },
  },
});

const react = createViewServerReact(viewServer);
const { useLiveQuery, useViewServerHealth, useViewServerHealthSummary } = react;
const ViewServerClientProvider = react[ViewServerReactClientProvider];
const sourceHealthReact = createViewServerReact(sourceHealthViewServer);
const SourceHealthClientProvider = sourceHealthReact[ViewServerReactClientProvider];
const diagnosticHealthReact = createViewServerReact(diagnosticHealthViewServer);
const DiagnosticHealthClientProvider = diagnosticHealthReact[ViewServerReactClientProvider];
const diagnosticRemoteReact = createViewServerReact(diagnosticRemoteViewServer);

const makeSourceHealthAdapterLayer = (leaseState: { active: number }) =>
  SourceAdapterServer.make(sourceAdapter, {
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
        Effect.gen(function* () {
          leaseState.active += 1;
          yield* Scope.addFinalizer(
            yield* Effect.scope,
            Effect.sync(() => {
              leaseState.active -= 1;
            }),
          );
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "react-browser",
              events: Stream.never,
            }),
          ]);
        }),
      metrics: () => Effect.succeed({ observed: 1n }),
      retry: Schedule.recurs(0),
    },
  });

type TestInMemoryOptions = ViewServerInMemoryOptions<typeof viewServer.topics>;

const createInMemoryViewServer = (options?: TestInMemoryOptions) =>
  createInMemoryViewServerReact(react, options);

type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

const healthTopicRow = (
  status: "ready" | "degraded" | "starting" | "stopping",
): ViewServerHealthTopicRow<"orders"> => ({
  id: "orders",
  status,
  rowCount: 0,
  liveRowCount: 0,
  deletedRowCount: 0,
  version: 0,
  lastMutationAt: null,
  mutationsPerSecond: 0,
  rowsPerSecond: 0,
  pendingMutationBatches: 0,
  activeFallbackGroupedViews: 0,
  activeIncrementalGroupedViews: 0,
  activeViews: 0,
  groupedFullEvaluationCount: 0,
  groupedPatchedEvaluationCount: 0,
  activeSubscriptions: 0,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
  updatedAtNanos: 1n,
});

const healthSummaryRow = (
  runtimeStatus: "ready" | "degraded" | "starting" | "stopping",
): ViewServerHealthSummaryRow<typeof viewServer.topics> => ({
  id: "summary",
  status: runtimeStatus,
  runtimeStatus,
  connectionStatus: "connected",
  unhealthyTopics: runtimeStatus === "ready" ? [] : ["orders"],
  updatedAtNanos: 1n,
});

type FakeHealthClient = {
  readonly close: Effect.Effect<void>;
  readonly client: ViewServerLiveClient<typeof viewServer.topics>;
};

const fakeHealthClient = (
  status: "ready" | "degraded" | "starting" | "stopping",
): FakeHealthClient => {
  const inMemory = createCoreInMemoryViewServer(viewServer);
  return {
    close: inMemory.close,
    client: {
      ...inMemory.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make({
            type: "snapshot",
            topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            queryId: "health-summary",
            version: 1,
            keys: ["summary"],
            rows: [healthSummaryRow(status)],
            totalRows: 1,
          }),
          close: () => Effect.void,
        }),
      subscribeHealth: () =>
        Effect.succeed({
          events: Stream.make({
            type: "snapshot",
            topic: VIEW_SERVER_HEALTH_TOPIC,
            queryId: "health",
            version: 1,
            keys: ["orders"],
            rows: [healthTopicRow(status)],
            totalRows: 1,
          }),
          close: () => Effect.void,
        }),
    },
  };
};

type ProviderErrorState = { readonly message: string | null };
type ProviderErrorStateWithMessage = { readonly message: string };

class ProviderErrorBoundary extends Component<
  { readonly children: ReactNode },
  ProviderErrorState
> {
  override readonly state: ProviderErrorState = {
    message: null,
  };

  static getDerivedStateFromError<ErrorValue>(error: ErrorValue): ProviderErrorStateWithMessage {
    return {
      message: error instanceof Error ? error.message : String(error),
    };
  }

  override render(): ReactNode {
    if (this.state.message !== null) {
      return (
        <output aria-label="provider error" role="alert">
          {this.state.message}
        </output>
      );
    }
    return this.props.children;
  }
}

describe("createViewServerReact", () => {
  it("cleans subscriptions without closing caller-owned generic provider clients", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={inMemory.liveClient}>
        <OrdersView />
      </ViewServerClientProvider>,
    );
    await Effect.runPromise(inMemory.client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("orders: a", { exact: true })).toBeVisible();

    await view.unmount();

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(inMemory.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await Effect.runPromise(inMemory.client.publish("orders", order("b", 20)));
    const health = await Effect.runPromise(inMemory.client.health());
    expect(health.status).toBe("ready");
    expect(health.engine.topics.orders.rowCount).toBe(2);
    await Effect.runPromise(inMemory.close);
  });

  it("surfaces missing provider clients", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    function HealthView() {
      const health = useViewServerHealthSummary();
      return <output role="status">{health.status}</output>;
    }

    const missingProvider = await render(
      <ProviderErrorBoundary>
        <HealthView />
      </ProviderErrorBoundary>,
    );
    await expect
      .element(
        missingProvider.getByText("ViewServerProvider is missing a client.", { exact: true }),
      )
      .toBeVisible();
    await missingProvider.unmount();
    consoleError.mockRestore();
  });

  it("merges disconnected summary subscription status into public health status", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);
    const disconnectedClient = {
      ...inMemory.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make({
            type: "status",
            topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            queryId: "health-summary",
            status: "error",
            code: "TransportError",
            message: "socket closed",
          }),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;

    function HealthView() {
      const health = useViewServerHealthSummary();
      return (
        <output role="status">
          {health.runtimeStatus}:{health.connectionStatus}:{health.status}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={disconnectedClient}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("starting:disconnected:disconnected", { exact: true }))
      .toBeVisible();
    await view.unmount();
    await Effect.runPromise(inMemory.close);
  });

  it("derives detailed runtime status from pushed health rows", async () => {
    const stopping = fakeHealthClient("stopping");
    const degraded = fakeHealthClient("degraded");
    const starting = fakeHealthClient("starting");

    function HealthView() {
      const health = useViewServerHealth();
      return (
        <output role="status">
          {`${health.runtimeStatus}:${health.connectionStatus}:${health.status}:${health.statusCode ?? "none"}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={stopping.client}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("stopping:connected:stopping:none", { exact: true }))
      .toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={degraded.client}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("degraded:connected:degraded:none", { exact: true }))
      .toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={starting.client}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("starting:connected:starting:none", { exact: true }))
      .toBeVisible();

    await view.unmount();
    await Effect.runPromise(stopping.close);
    await Effect.runPromise(degraded.close);
    await Effect.runPromise(starting.close);
  });

  it("derives health status while summary and detail streams are connecting or disconnected", async () => {
    const summaryConnectedNoRowRuntime = createCoreInMemoryViewServer(viewServer);
    const summaryDisconnectedWithRowRuntime = createCoreInMemoryViewServer(viewServer);
    const detailConnectingRuntime = createCoreInMemoryViewServer(viewServer);
    const detailDisconnectedWithRowRuntime = createCoreInMemoryViewServer(viewServer);
    const summaryConnectedNoRowClient = {
      ...summaryConnectedNoRowRuntime.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make({
            type: "status",
            topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            queryId: "health-summary",
            status: "ready",
            code: "Ready",
          }),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const summaryDisconnectedWithRowClient = {
      ...summaryDisconnectedWithRowRuntime.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make(
            {
              type: "snapshot",
              topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
              queryId: "health-summary",
              version: 1,
              keys: ["summary"],
              rows: [
                {
                  id: "summary",
                  status: "degraded",
                  runtimeStatus: "degraded",
                  connectionStatus: "connected",
                  unhealthyTopics: ["orders"],
                  updatedAtNanos: 1n,
                },
              ],
              totalRows: 1,
            },
            {
              type: "status",
              topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
              queryId: "health-summary",
              status: "error",
              code: "TransportError",
              message: "socket closed",
            },
          ),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const detailConnectingClient = {
      ...detailConnectingRuntime.liveClient,
      subscribeHealth: () =>
        Effect.succeed({
          events: Stream.fromEffect(Effect.never),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const detailDisconnectedWithRowClient = {
      ...detailDisconnectedWithRowRuntime.liveClient,
      subscribeHealth: () =>
        Effect.succeed({
          events: Stream.make(
            {
              type: "snapshot",
              topic: VIEW_SERVER_HEALTH_TOPIC,
              queryId: "health",
              version: 1,
              keys: ["orders"],
              rows: [healthTopicRow("ready")],
              totalRows: 1,
            },
            {
              type: "status",
              topic: VIEW_SERVER_HEALTH_TOPIC,
              queryId: "health",
              status: "error",
              code: "TransportError",
              message: "socket closed",
            },
          ),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;

    function SummaryHealthView() {
      const health = useViewServerHealthSummary();
      return (
        <output role="status">
          {health.runtimeStatus}:{health.connectionStatus}:{health.status}
        </output>
      );
    }

    function DetailedHealthView() {
      const health = useViewServerHealth();
      return (
        <output role="status">
          {`${health.runtimeStatus}:${health.connectionStatus}:${health.status}:${health.statusCode ?? "none"}`}
        </output>
      );
    }

    const summaryConnectedView = await render(
      <ViewServerClientProvider client={summaryConnectedNoRowClient}>
        <SummaryHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(summaryConnectedView.getByText("starting:connected:starting", { exact: true }))
      .toBeVisible();
    await summaryConnectedView.unmount();

    const summaryDisconnectedView = await render(
      <ViewServerClientProvider client={summaryDisconnectedWithRowClient}>
        <SummaryHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(
        summaryDisconnectedView.getByText("degraded:disconnected:disconnected", { exact: true }),
      )
      .toBeVisible();
    await summaryDisconnectedView.unmount();

    const detailSummaryDisconnectedView = await render(
      <ViewServerClientProvider client={summaryDisconnectedWithRowClient}>
        <DetailedHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(
        detailSummaryDisconnectedView.getByText("degraded:disconnected:disconnected:none", {
          exact: true,
        }),
      )
      .toBeVisible();
    await detailSummaryDisconnectedView.unmount();

    const detailConnectingView = await render(
      <ViewServerClientProvider client={detailConnectingClient}>
        <DetailedHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(detailConnectingView.getByText("ready:connecting:connecting:none", { exact: true }))
      .toBeVisible();
    await detailConnectingView.unmount();

    const detailDisconnectedView = await render(
      <ViewServerClientProvider client={detailDisconnectedWithRowClient}>
        <DetailedHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(
        detailDisconnectedView.getByText("ready:disconnected:disconnected:TransportError", {
          exact: true,
        }),
      )
      .toBeVisible();
    await detailDisconnectedView.unmount();

    await Effect.runPromise(summaryConnectedNoRowRuntime.close);
    await Effect.runPromise(summaryDisconnectedWithRowRuntime.close);
    await Effect.runPromise(detailConnectingRuntime.close);
    await Effect.runPromise(detailDisconnectedWithRowRuntime.close);
  });

  it("switches hook clients when the generic provider client prop changes", async () => {
    const first = createCoreInMemoryViewServer(viewServer);
    const second = createCoreInMemoryViewServer(viewServer);

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={first.liveClient}>
        <OrdersView />
      </ViewServerClientProvider>,
    );
    await Effect.runPromise(first.client.publish("orders", order("first", 10)));
    await expect.element(view.getByText("orders: first", { exact: true })).toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={second.liveClient}>
        <OrdersView />
      </ViewServerClientProvider>,
    );
    await Effect.runPromise(second.client.publish("orders", order("second", 20)));
    await expect.element(view.getByText("orders: second", { exact: true })).toBeVisible();

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(first.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await view.unmount();
    await Effect.runPromise(first.close);
    await Effect.runPromise(second.close);
  });

  it("keeps nested provider contexts isolated per binding instance", async () => {
    const outerReact = createViewServerReact(viewServer);
    const innerReact = createViewServerReact(viewServer);
    const outer = createInMemoryViewServerReact(outerReact);
    const inner = createInMemoryViewServerReact(innerReact);

    function OuterOrdersView() {
      const result = outerReact.useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="outer orders" role="status">
          outer orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    function InnerOrdersView() {
      const result = innerReact.useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="inner orders" role="status">
          inner orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <outer.ViewServerInMemoryProvider>
        <inner.ViewServerInMemoryProvider>
          <OuterOrdersView />
          <InnerOrdersView />
        </inner.ViewServerInMemoryProvider>
      </outer.ViewServerInMemoryProvider>,
    );

    await Effect.runPromise(outer.client.publish("orders", order("outer", 10)));
    await Effect.runPromise(inner.client.publish("orders", order("inner", 20)));

    await expect.element(view.getByText("outer orders: outer", { exact: true })).toBeVisible();
    await expect.element(view.getByText("inner orders: inner", { exact: true })).toBeVisible();

    await view.unmount();
  });

  it("streams runtime-published snapshots and live deltas in browser providers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }
    function HealthView() {
      const health = useViewServerHealth();
      const rowCount = health.rows[0]?.rowCount ?? 0;
      return (
        <output aria-label="health" role="status">
          {rowCount}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publishMany("orders", [order("b", 20), order("a", 10)]));

    await expect.element(view.getByText("orders: a:10|b:20", { exact: true })).toBeVisible();
    await expect.element(view.getByText("2", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publish("orders", order("c", 5)));

    await expect.element(view.getByText("orders: c:5|a:10|b:20", { exact: true })).toBeVisible();
    await expect.element(view.getByText("3", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("uses the same component with in-memory and remote providers", async () => {
    function OrdersView(props: { readonly id: string }) {
      const result = useLiveQuery("orders", {
        where: [{ field: "id", type: "equals", filter: props.id }],
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label={`orders ${props.id}`} role="status">
          {rows === "" ? `orders ${props.id}: none` : `orders ${props.id}: ${rows}`}
        </output>
      );
    }
    function HealthView(props: { readonly label: string }) {
      const health = useViewServerHealthSummary();
      return (
        <output aria-label={props.label} role="status">
          {props.label}: {health.status}
        </output>
      );
    }

    const local = createInMemoryViewServer();
    const localId = `local-${crypto.randomUUID()}`;
    const localView = await render(
      <local.ViewServerInMemoryProvider>
        <OrdersView id={localId} />
        <HealthView label={`local health ${localId}`} />
      </local.ViewServerInMemoryProvider>,
    );
    await expect
      .element(localView.getByText(`orders ${localId}: none`, { exact: true }))
      .toBeVisible();
    await expect
      .element(localView.getByText(`local health ${localId}: ready`, { exact: true }))
      .toBeVisible();

    await Effect.runPromise(local.client.publish("orders", order(localId, 10)));
    await expect
      .element(localView.getByText(`orders ${localId}: ${localId}:10`, { exact: true }))
      .toBeVisible();
    await localView.unmount();

    const remoteId = `remote-${crypto.randomUUID()}`;
    const remoteView = await render(
      <react.ViewServerProvider url={inject("viewServerRemoteUrl")}>
        <OrdersView id={remoteId} />
        <HealthView label={`remote health ${remoteId}`} />
      </react.ViewServerProvider>,
    );
    await expect
      .element(remoteView.getByText(`orders ${remoteId}: none`, { exact: true }))
      .toBeVisible();
    await expect
      .element(remoteView.getByText(`remote health ${remoteId}: ready`, { exact: true }))
      .toBeVisible();

    const remoteProbe = await Effect.runPromise(
      makeViewServerClient(viewServer, {
        url: inject("viewServerRemoteUrl"),
      }),
    );
    const readRemoteOrderSubscriptions = async () => {
      const subscription = await Effect.runPromise(remoteProbe.subscribeHealth());
      const events = await Effect.runPromise(
        subscription.events.pipe(Stream.take(1), Stream.runCollect),
      );
      await Effect.runPromise(subscription.close());
      const event = events[0];
      return event?.type === "snapshot"
        ? (event.rows.find((row) => row.id === "orders")?.activeSubscriptions ?? -1)
        : -1;
    };
    await expect.poll(readRemoteOrderSubscriptions).toBe(1);

    await remoteView.unmount();

    await expect.poll(readRemoteOrderSubscriptions).toBe(0);
    await Effect.runPromise(remoteProbe.close);
  });

  it("owns remote client creation from provider URL and options", async () => {
    function HealthView(props: { readonly label: string }) {
      const health = useViewServerHealthSummary();
      return (
        <output aria-label={props.label} role="status">
          {props.label}: {health.status}
        </output>
      );
    }

    const remoteProviderView = await render(
      <react.ViewServerProvider subscriptionBufferSize={8} url={inject("viewServerRemoteUrl")}>
        <HealthView label="remote provider health" />
      </react.ViewServerProvider>,
    );
    await expect
      .element(remoteProviderView.getByText("remote provider health: ready", { exact: true }))
      .toBeVisible();
    await remoteProviderView.unmount();
  });

  it("recreates remote provider clients when URL options change", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    function HealthView() {
      const health = useViewServerHealthSummary();
      return <output role="status">{health.status}</output>;
    }

    const provider = await render(
      <ProviderErrorBoundary>
        <react.ViewServerProvider url={inject("viewServerRemoteUrl")}>
          <HealthView />
        </react.ViewServerProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(provider.getByText("ready", { exact: true })).toBeVisible();

    await provider.rerender(
      <ProviderErrorBoundary>
        <react.ViewServerProvider url="ws://127.0.0.1:1/rpc">
          <HealthView />
        </react.ViewServerProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(provider.getByRole("alert")).toBeVisible();
    await provider.unmount();
    consoleError.mockRestore();
  });

  it("surfaces remote provider connection failures through error boundaries", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    function HealthView() {
      const health = useViewServerHealthSummary();
      return <output role="status">{health.status}</output>;
    }

    const failedProvider = await render(
      <ProviderErrorBoundary>
        <react.ViewServerProvider url="ws://127.0.0.1:1/rpc">
          <HealthView />
        </react.ViewServerProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(failedProvider.getByRole("alert")).toBeVisible();
    await failedProvider.unmount();
    consoleError.mockRestore();
  });

  it("closes live subscriptions when browser components unmount", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const rows = result.rows.map((row) => row.id).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("orders: a", { exact: true })).toBeVisible();

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    await view.unmount();
  });

  it("adopts the committed winner for competing same-key Source Health consumers", async () => {
    const leaseState = { active: 0 };
    const sourceAdapterLayer = makeSourceHealthAdapterLayer(leaseState);
    const runtime = await Effect.runPromise(
      makeViewServerRuntimeCore(sourceHealthViewServer, {}).pipe(
        Effect.provide(sourceAdapterLayer),
      ),
    );
    let subscribeCount = 0;
    let closeCount = 0;
    const subscribeSourceHealth: typeof runtime.liveClient.subscribeSourceHealth = (input) =>
      runtime.liveClient.subscribeSourceHealth(input).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            subscribeCount += 1;
          }),
        ),
        Effect.map((subscription) => ({
          events: subscription.events,
          close: () =>
            subscription.close().pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  closeCount += 1;
                }),
              ),
            ),
        })),
      );
    const trackedClient = {
      ...runtime.liveClient,
      subscribeSourceHealth,
    } satisfies ViewServerLiveClient<typeof sourceHealthViewServer.topics>;

    function SourceHealthView(props: { readonly label: string; readonly region: string }) {
      const result = sourceHealthReact.useSourceHealth({
        topic: "orders",
        routeBy: { region: props.region },
      });
      const text = AsyncResult.isSuccess(result)
        ? result.value._tag === "Inactive"
          ? `Inactive:${result.value.route.region}`
          : `Active:${result.value.health.adapter.name}`
        : "Loading";
      return (
        <output aria-label={props.label} role="status">
          {text}
        </output>
      );
    }

    const view = await render(
      <SourceHealthClientProvider client={trackedClient}>
        <SourceHealthView label="first source health" region="eu" />
        <SourceHealthView label="second source health" region="eu" />
        <SourceHealthView label="us source health" region="us" />
      </SourceHealthClientProvider>,
    );
    await expect
      .element(view.getByRole("status", { name: "first source health" }))
      .toHaveTextContent(/^Inactive:eu$/);
    await expect
      .element(view.getByRole("status", { name: "second source health" }))
      .toHaveTextContent(/^Inactive:eu$/);
    await expect
      .element(view.getByRole("status", { name: "us source health" }))
      .toHaveTextContent(/^Inactive:us$/);
    await expect.poll(() => subscribeCount).toBe(2);
    await expect.poll(() => leaseState.active).toBe(0);

    const liveSubscription = await Effect.runPromise(
      runtime.liveClient.subscribe("orders", {
        select: ["id"],
        routeBy: { region: "eu" },
      }),
    );
    await expect
      .element(view.getByRole("status", { name: "first source health" }))
      .toHaveTextContent(/^Active:react-browser-source$/);
    await expect.poll(() => leaseState.active).toBe(1);
    await Effect.runPromise(liveSubscription.close());
    await expect
      .element(view.getByRole("status", { name: "second source health" }))
      .toHaveTextContent(/^Inactive:eu$/);
    await expect.poll(() => leaseState.active).toBe(0);

    await view.rerender(
      <SourceHealthClientProvider client={trackedClient}>
        <SourceHealthView label="us source health" region="us" />
      </SourceHealthClientProvider>,
    );
    await expect.poll(() => closeCount).toBe(1);
    await expect.poll(() => subscribeCount).toBe(2);

    await view.rerender(
      <SourceHealthClientProvider client={trackedClient}>
        <SourceHealthView label="us source health" region="us" />
        <SourceHealthView label="remounted eu source health" region="eu" />
      </SourceHealthClientProvider>,
    );
    await expect
      .element(view.getByRole("status", { name: "remounted eu source health" }))
      .toHaveTextContent(/^Inactive:eu$/);
    await expect.poll(() => subscribeCount).toBe(3);

    await view.rerender(<SourceHealthClientProvider client={trackedClient} />);
    await expect.poll(() => closeCount).toBe(3);

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("does not retain Source Health entries from discarded renders", async () => {
    const leaseState = { active: 0 };
    const runtime = await Effect.runPromise(
      makeViewServerRuntimeCore(sourceHealthViewServer, {}).pipe(
        Effect.provide(makeSourceHealthAdapterLayer(leaseState)),
      ),
    );
    const subscribedInputKeys: Array<string> = [];
    const subscribeSourceHealth: typeof runtime.liveClient.subscribeSourceHealth = (input) => {
      subscribedInputKeys.push(Object.keys(input).join("|"));
      return runtime.liveClient.subscribeSourceHealth(input);
    };
    const trackedClient = {
      ...runtime.liveClient,
      subscribeSourceHealth,
    } satisfies ViewServerLiveClient<typeof sourceHealthViewServer.topics>;
    const pending = new Promise<never>(() => undefined);

    function DiscardedSourceHealthView(): ReactNode {
      sourceHealthReact.useSourceHealth({
        routeBy: { region: "eu" },
        topic: "orders",
      });
      throw pending;
    }

    function CommittedSourceHealthView() {
      const result = sourceHealthReact.useSourceHealth({
        topic: "orders",
        routeBy: { region: "eu" },
      });
      return (
        <output role="status">
          {AsyncResult.isSuccess(result) && result.value._tag === "Inactive"
            ? result.value.route.region
            : "Loading"}
        </output>
      );
    }

    const view = await render(
      <SourceHealthClientProvider client={trackedClient}>
        <Suspense fallback={<output role="status">Suspended</output>}>
          <DiscardedSourceHealthView />
        </Suspense>
      </SourceHealthClientProvider>,
    );
    await expect.element(view.getByRole("status")).toHaveTextContent(/^Suspended$/);
    expect(subscribedInputKeys).toStrictEqual([]);

    await view.rerender(
      <SourceHealthClientProvider client={trackedClient}>
        <CommittedSourceHealthView />
      </SourceHealthClientProvider>,
    );
    await expect.poll(() => subscribedInputKeys.length).toBe(1);
    await expect.element(view.getByRole("status")).toHaveTextContent(/^eu$/);
    expect(subscribedInputKeys).toStrictEqual(["topic|routeBy"]);

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("surfaces hostile Source Health inputs as typed errors without crashing render", async () => {
    const leaseState = { active: 0 };
    const runtime = await Effect.runPromise(
      makeViewServerRuntimeCore(sourceHealthViewServer, {}).pipe(
        Effect.provide(makeSourceHealthAdapterLayer(leaseState)),
      ),
    );
    type SourceHealthInput = {
      readonly topic: "orders";
      readonly routeBy: {
        readonly region: string;
      };
    };
    let getterReads = 0;

    function HostileSourceHealthView(props: {
      readonly label: string;
      readonly input: SourceHealthInput;
    }) {
      const result = sourceHealthReact.useSourceHealth(props.input);
      let text = "Loading";
      if (AsyncResult.isSuccess(result)) {
        text = "Success";
      } else if (AsyncResult.isFailure(result)) {
        const error = Option.getOrUndefined(Cause.findErrorOption(result.cause));
        text =
          error?._tag === "ViewServerRuntimeError" ? `error:${error.code}` : "error:Unexpected";
      }
      return (
        <output aria-label={props.label} role="status">
          {text}
        </output>
      );
    }

    const accessorInput = {
      topic: "orders",
      routeBy: { region: "eu" },
    } satisfies SourceHealthInput;
    Object.defineProperty(accessorInput, "routeBy", {
      enumerable: true,
      get: () => {
        getterReads += 1;
        throw new Error("Source Health accessor must not escape render.");
      },
    });
    const proxyInput = new Proxy(
      {
        topic: "orders",
        routeBy: { region: "us" },
      } satisfies SourceHealthInput,
      {
        ownKeys: () => {
          throw new Error("Source Health proxy trap must not escape render.");
        },
      },
    );

    const accessorView = await render(
      <ProviderErrorBoundary>
        <SourceHealthClientProvider client={runtime.liveClient}>
          <HostileSourceHealthView label="accessor source health" input={accessorInput} />
        </SourceHealthClientProvider>
      </ProviderErrorBoundary>,
    );
    await expect
      .element(accessorView.getByRole("status", { name: "accessor source health" }))
      .toHaveTextContent(/^error:InvalidQuery$/);
    expect(getterReads).toBe(0);
    await accessorView.unmount();

    const proxyView = await render(
      <ProviderErrorBoundary>
        <SourceHealthClientProvider client={runtime.liveClient}>
          <HostileSourceHealthView label="proxy source health" input={proxyInput} />
        </SourceHealthClientProvider>
      </ProviderErrorBoundary>,
    );
    await expect
      .element(proxyView.getByRole("status", { name: "proxy source health" }))
      .toHaveTextContent(/^error:InvalidQuery$/);
    await proxyView.unmount();

    expect(leaseState.active).toBe(0);
    await Effect.runPromise(runtime.close);
  });

  it("provides source-backed in-memory React diagnostics and owns lease cleanup", async () => {
    const leaseState = { active: 0 };
    const sourceAdapterLayer = makeSourceHealthAdapterLayer(leaseState);
    const local = await Effect.runPromise(
      makeInMemoryViewServerReact(sourceHealthReact).pipe(Effect.provide(sourceAdapterLayer)),
    );

    function SourceHealthView() {
      const result = sourceHealthReact.useSourceHealth({
        topic: "orders",
        routeBy: { region: "browser" },
      });
      const text = AsyncResult.isSuccess(result)
        ? result.value._tag === "Inactive"
          ? `Inactive:${result.value.route.region}`
          : `Active:${result.value.health.adapter.name}`
        : "Loading";
      return <output role="status">{text}</output>;
    }

    function ActiveSourceView() {
      const result = sourceHealthReact.useLiveQuery("orders", {
        select: ["id"],
        routeBy: { region: "browser" },
      });
      return <output aria-label="source rows">{result.status}</output>;
    }

    const view = await render(
      <local.ViewServerInMemoryProvider>
        <SourceHealthView />
      </local.ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("Inactive:browser", { exact: true })).toBeVisible();
    expect(leaseState.active).toBe(0);

    await view.rerender(
      <local.ViewServerInMemoryProvider>
        <SourceHealthView />
        <ActiveSourceView />
      </local.ViewServerInMemoryProvider>,
    );
    await expect
      .element(view.getByText("Active:react-browser-source", { exact: true }))
      .toBeVisible();
    await expect
      .element(view.getByRole("status", { name: "source rows" }))
      .toHaveTextContent(/^ready$/);
    expect(leaseState.active).toBe(1);

    await view.unmount();
    await expect.poll(() => leaseState.active).toBe(0);
    await Effect.runPromise(local.close);
  });

  it("renders combined maintenance degradation and InvalidSourceSettlement diagnostics", async () => {
    const leaseState = { active: 0 };
    const runtime = await Effect.runPromise(
      makeViewServerRuntimeCore(diagnosticHealthViewServer, {}).pipe(
        Effect.provide(makeSourceHealthAdapterLayer(leaseState)),
      ),
    );
    const advanceDiagnostics = await Effect.runPromise(Queue.unbounded<void>());
    const subscribeSourceHealth: typeof runtime.liveClient.subscribeSourceHealth = (input) =>
      runtime.liveClient.subscribeSourceHealth(input).pipe(
        Effect.map((subscription) => ({
          close: subscription.close,
          events: subscription.events.pipe(
            Stream.take(1),
            Stream.flatMap((result) => {
              const combined = {
                ...result,
                status: {
                  _tag: "Degraded",
                  attempt: 1n,
                  degradedAtNanos: 20n,
                  reasons: [
                    {
                      _tag: "SourceItemRejection",
                      latestRejection: {
                        failure: {
                          _tag: "RuntimeFailure",
                          failure: {
                            _tag: "InvalidSourceDelivery",
                            message: "invalid browser source item",
                          },
                        },
                        location: { offset: 42n },
                        rejectedAtNanos: 21n,
                      },
                    },
                    {
                      _tag: "AdapterMaintenanceFailure",
                    },
                  ],
                },
                sampledAtNanos: 22n,
              } as const;
              const invalidSettlement = {
                ...result,
                status: {
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
                  exhaustedAtNanos: 23n,
                },
                sampledAtNanos: 24n,
              } as const;
              return Stream.make(combined).pipe(
                Stream.concat(
                  Stream.fromEffect(
                    Queue.take(advanceDiagnostics).pipe(Effect.as(invalidSettlement)),
                  ),
                ),
              );
            }),
          ),
        })),
      );
    const diagnosticClient = {
      ...runtime.liveClient,
      subscribeSourceHealth,
    } satisfies ViewServerLiveClient<typeof diagnosticHealthViewServer.topics>;

    function DiagnosticView() {
      const result = diagnosticHealthReact.useSourceHealth({
        topic: "orders",
      });
      const text = AsyncResult.match(result, {
        onInitial: () => "Loading",
        onFailure: () => "Loading",
        onSuccess: ({ value }) =>
          Match.value(value.status).pipe(
            Match.when(
              { _tag: "Degraded" },
              (status) => `${status._tag}:${status.reasons.map((reason) => reason._tag).join("+")}`,
            ),
            Match.when(
              {
                _tag: "Exhausted",
                exhaustion: {
                  lastTermination: {
                    _tag: "Failed",
                    failure: { _tag: "RuntimeFailure" },
                  },
                },
              },
              (status) =>
                `${status._tag}:${status.exhaustion.lastTermination.failure.failure._tag}`,
            ),
            Match.orElse((status) => status._tag),
          ),
      });
      return <output role="status">{text}</output>;
    }

    const view = await render(
      <DiagnosticHealthClientProvider client={diagnosticClient}>
        <DiagnosticView />
      </DiagnosticHealthClientProvider>,
    );
    await expect
      .element(view.getByRole("status"))
      .toHaveTextContent(/^Degraded:SourceItemRejection\+AdapterMaintenanceFailure$/);

    await Effect.runPromise(Queue.offer(advanceDiagnostics, undefined));
    await expect
      .element(view.getByRole("status"))
      .toHaveTextContent(/^Exhausted:InvalidSourceSettlement$/);

    await view.unmount();
    await Effect.runPromise(runtime.close);
  });

  it("streams the same Source Health contract through the remote provider", async () => {
    function RemoteSourceHealthView() {
      const result = sourceHealthReact.useSourceHealth({
        topic: "orders",
        routeBy: { region: "remote" },
      });
      const text = AsyncResult.match(result, {
        onInitial: () => "Loading",
        onFailure: () => "Loading",
        onSuccess: ({ value }) =>
          Match.value(value).pipe(
            Match.when({ _tag: "Inactive" }, (inactive) => `Inactive:${inactive.route.region}`),
            Match.when({ _tag: "Active" }, (active) => `Active:${active.health.adapter.name}`),
            Match.exhaustive,
          ),
      });
      return <output role="status">{text}</output>;
    }

    const view = await render(
      <sourceHealthReact.ViewServerProvider url={inject("viewServerSourceRemoteUrl")}>
        <RemoteSourceHealthView />
      </sourceHealthReact.ViewServerProvider>,
    );
    await expect.element(view.getByText("Inactive:remote", { exact: true })).toBeVisible();

    const remoteClient = await Effect.runPromise(
      makeViewServerClient(sourceHealthViewServer, {
        url: inject("viewServerSourceRemoteUrl"),
      }),
    );
    const liveSubscription = await Effect.runPromise(
      remoteClient.subscribe("orders", {
        select: ["id"],
        routeBy: { region: "remote" },
      }),
    );
    await expect
      .element(view.getByText("Active:react-browser-source", { exact: true }))
      .toBeVisible();

    await Effect.runPromise(liveSubscription.close());
    await expect.element(view.getByText("Inactive:remote", { exact: true })).toBeVisible();
    await view.unmount();
    await Effect.runPromise(remoteClient.close);
  });

  it("streams exact Kafka maintenance episodes and settlement failures to remote browsers", async () => {
    function RemoteDiagnosticView() {
      const result = diagnosticRemoteReact.useSourceHealth({
        topic: "diagnostics",
      });
      const text = AsyncResult.match(result, {
        onInitial: () => "Loading",
        onFailure: () => "Loading",
        onSuccess: ({ value: health }) =>
          Match.value(health.status).pipe(
            Match.when({ _tag: "Degraded" }, (status) => {
              const retention = health.metrics.adapter.regions[0].retention;
              return [
                "Degraded",
                status.degradedAtNanos,
                status.reasons.map((reason) => reason._tag).join("+"),
                `backlog=${retention.failedWorkBacklog}`,
                `failure=${retention.latestExpirationFailure?.message ?? "none"}`,
                `retries=${retention.expirationRetryFailures}`,
              ].join(":");
            }),
            Match.when({ _tag: "Ready" }, (status) => `Ready:${status.readyAtNanos}`),
            Match.when(
              {
                _tag: "Exhausted",
                exhaustion: {
                  lastTermination: {
                    _tag: "Failed",
                    failure: { _tag: "RuntimeFailure" },
                  },
                },
              },
              (status) => `Exhausted:${status.exhaustion.lastTermination.failure.failure._tag}`,
            ),
            Match.orElse((status) => status._tag),
          ),
      });
      return <output role="status">{text}</output>;
    }

    const view = await render(
      <diagnosticRemoteReact.ViewServerProvider url={inject("viewServerDiagnosticRemoteUrl")}>
        <RemoteDiagnosticView />
      </diagnosticRemoteReact.ViewServerProvider>,
    );
    const status = view.getByRole("status");
    await expect
      .element(status)
      .toHaveTextContent(
        /^Degraded:100:AdapterMaintenanceFailure:backlog=1:failure=Kafka retention expiration Delete failed\.:retries=2$/,
      );
    await expect
      .element(status)
      .toHaveTextContent(
        /^Degraded:100:SourceItemRejection\+AdapterMaintenanceFailure:backlog=1:failure=Kafka retention expiration Delete failed\.:retries=2$/,
      );
    await expect.element(status).toHaveTextContent(/^Ready:150$/);
    await expect
      .element(status)
      .toHaveTextContent(
        /^Degraded:200:AdapterMaintenanceFailure:backlog=1:failure=Kafka retention expiration Delete failed\.:retries=2$/,
      );
    await expect.element(status).toHaveTextContent(/^Exhausted:InvalidSourceSettlement$/);
    await view.unmount();
  });

  it("reuses subscriptions when recursive where syntax has the same engine meaning", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);
    let subscribeCount = 0;
    let closeCount = 0;
    const trackedClient = {
      ...inMemory.liveClient,
      subscribe: (topic, query) =>
        inMemory.liveClient.subscribe(topic, query).pipe(
          Effect.map((subscription) => {
            subscribeCount += 1;
            return {
              events: subscription.events,
              close: () =>
                subscription.close().pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      closeCount += 1;
                    }),
                  ),
                ),
            };
          }),
        ),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;

    function EquivalentOrdersView(props: { readonly expanded: boolean }) {
      const where: Where<OrderRow> = props.expanded
        ? [
            {
              field: "id",
              type: "contains",
              filter: "Résumé",
              caseSensitive: false,
              accentSensitive: false,
            },
            { field: "id", type: "in", filter: [] },
            {
              type: "OR",
              conditions: [
                { type: "OR", conditions: [] },
                { field: "price", type: "equals", filter: -0 },
                { field: "price", type: "equals", filter: 0 },
                {
                  type: "OR",
                  conditions: [
                    { field: "customerId", type: "equals", filter: "CUSTOMER-A" },
                    { field: "customerId", type: "equals", filter: "customer-a" },
                  ],
                },
              ],
            },
            {
              type: "NOT",
              condition: {
                type: "NOT",
                condition: { field: "region", type: "equals", filter: "USÁ" },
              },
            },
          ]
        : [
            {
              type: "AND",
              conditions: [
                { field: "region", type: "equals", filter: "usa" },
                {
                  type: "OR",
                  conditions: [
                    { field: "customerId", type: "equals", filter: "customer-a" },
                    { field: "price", type: "equals", filter: 0 },
                  ],
                },
                { field: "id", type: "contains", filter: "resume" },
              ],
            },
          ];
      const result = useLiveQuery("orders", { select: ["id"], where });
      return <output role="status">{result.status}</output>;
    }

    const view = await render(
      <ViewServerClientProvider client={trackedClient}>
        <EquivalentOrdersView expanded />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("ready", { exact: true })).toBeVisible();
    await expect.poll(() => subscribeCount).toBe(1);

    await view.rerender(
      <ViewServerClientProvider client={trackedClient}>
        <EquivalentOrdersView expanded={false} />
      </ViewServerClientProvider>,
    );

    await expect.element(view.getByText("ready", { exact: true })).toBeVisible();
    expect(subscribeCount).toBe(1);
    expect(closeCount).toBe(0);

    await view.unmount();
    await expect.poll(() => closeCount).toBe(1);
    await Effect.runPromise(inMemory.close);
  });

  it("captures query identity once per reference across live rerenders", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);
    await Effect.runPromise(
      inMemory.client.publishMany("orders", [order("first", 10), order("second", 20)]),
    );

    let subscribeCount = 0;
    let closeCount = 0;
    const trackedClient = {
      ...inMemory.liveClient,
      subscribe: (topic, query) =>
        inMemory.liveClient.subscribe(topic, query).pipe(
          Effect.map((subscription) => {
            subscribeCount += 1;
            return {
              events: subscription.events,
              close: () =>
                subscription.close().pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      closeCount += 1;
                    }),
                  ),
                ),
            };
          }),
        ),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;

    type StableQueryInput = {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "price"; readonly direction: "asc" }];
      limit: number;
    };
    const queryInput: StableQueryInput = {
      select: ["id"],
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 1,
    };
    let originalReflectionCount = 0;
    const stableQuery = new Proxy(queryInput, {
      ownKeys: (target) => {
        originalReflectionCount += 1;
        return Reflect.ownKeys(target);
      },
    });

    function OrdersView(props: { readonly query: typeof queryInput }) {
      const result = useLiveQuery("orders", props.query);
      return (
        <output aria-label="captured orders" role="status">
          orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={trackedClient}>
        <OrdersView query={stableQuery} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("orders: first", { exact: true })).toBeVisible();
    await expect.poll(() => subscribeCount).toBe(1);
    const reflectionCountAfterCapture = originalReflectionCount;

    queryInput.limit = 2;
    await view.rerender(
      <ViewServerClientProvider client={trackedClient}>
        <OrdersView query={stableQuery} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("orders: first", { exact: true })).toBeVisible();
    expect(originalReflectionCount).toBe(reflectionCountAfterCapture);
    expect(subscribeCount).toBe(1);

    await Effect.runPromise(inMemory.client.publish("orders", order("new-first", 5)));
    await expect.element(view.getByText("orders: new-first", { exact: true })).toBeVisible();
    expect(originalReflectionCount).toBe(reflectionCountAfterCapture);
    expect(subscribeCount).toBe(1);

    const replacementQuery: typeof queryInput = {
      select: ["id"],
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 2,
    };
    let replacementReflectionCount = 0;
    const replacementQueryReference = new Proxy(replacementQuery, {
      ownKeys: (target) => {
        replacementReflectionCount += 1;
        return Reflect.ownKeys(target);
      },
    });
    await view.rerender(
      <ViewServerClientProvider client={trackedClient}>
        <OrdersView query={replacementQueryReference} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("orders: new-first|first", { exact: true })).toBeVisible();
    await expect.poll(() => subscribeCount).toBe(2);
    await expect.poll(() => closeCount).toBe(1);
    expect(replacementReflectionCount).toBeGreaterThan(0);

    await view.unmount();
    await expect.poll(() => closeCount).toBe(2);
    await Effect.runPromise(inMemory.close);
  });

  it("resubscribes when a normalized operand changes schema validity", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);
    let subscribeCount = 0;
    const trackedClient = {
      ...inMemory.liveClient,
      subscribe: (topic, query) => {
        subscribeCount += 1;
        return inMemory.liveClient.subscribe(topic, query);
      },
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const validQuery = {
      select: ["id"],
      where: [{ field: "status", type: "equals", filter: "open" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "equals";
          readonly filter: "open";
        },
      ];
    };
    const invalidQuery = {
      select: ["id"],
      where: [{ field: "status", type: "equals", filter: "OPEN" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "equals";
          readonly filter: "OPEN";
        },
      ];
    };

    function ValidationSensitiveOrdersView(props: {
      readonly query: { readonly select: readonly ["id"] };
    }) {
      const result = useLiveQuery("orders", props.query);
      return (
        <output role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={trackedClient}>
        <ValidationSensitiveOrdersView query={validQuery} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("ready:Ready", { exact: true })).toBeVisible();
    await expect.poll(() => subscribeCount).toBe(1);

    await view.rerender(
      <ViewServerClientProvider client={trackedClient}>
        <ValidationSensitiveOrdersView query={invalidQuery} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("error:InvalidQuery", { exact: true })).toBeVisible();
    await expect.poll(() => subscribeCount).toBe(2);

    await view.rerender(
      <ViewServerClientProvider client={trackedClient}>
        <ValidationSensitiveOrdersView query={validQuery} />
      </ViewServerClientProvider>,
    );
    await expect.element(view.getByText("ready:Ready", { exact: true })).toBeVisible();
    await expect.poll(() => subscribeCount).toBe(3);

    await view.unmount();
    await Effect.runPromise(inMemory.close);
  });

  it("surfaces malformed queries as typed errors without crashing render", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);
    let getterReads = 0;

    function HostileOrdersView(props: {
      readonly label: string;
      readonly query: { readonly select: readonly ["id"] };
    }) {
      const result = useLiveQuery("orders", props.query);
      return (
        <output aria-label={props.label} role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    function HostileTopicView() {
      // @ts-expect-error unknown topics are still surfaced through the hook result.
      const result = useLiveQuery("doesNotExist", { select: ["id"] });
      return (
        <output aria-label="unknown topic" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const accessorQuery = {
      select: ["id"],
    } satisfies { readonly select: readonly ["id"] };
    Object.defineProperty(accessorQuery, "where", {
      enumerable: true,
      get: () => {
        getterReads += 1;
        return [];
      },
    });
    const sparseWhere: Array<unknown> = [];
    sparseWhere.length = 1;
    const sparseQuery = {
      select: ["id"],
    } satisfies { readonly select: readonly ["id"] };
    Object.defineProperty(sparseQuery, "where", {
      enumerable: true,
      value: sparseWhere,
    });
    const symbolicQuery = {
      select: ["id"],
    } satisfies { readonly select: readonly ["id"] };
    Object.defineProperty(symbolicQuery, Symbol("query"), {
      enumerable: true,
      value: true,
    });
    const proxyQuery = new Proxy(
      {
        select: ["id"],
      } satisfies { readonly select: readonly ["id"] },
      {
        ownKeys: () => {
          throw new Error("proxy trap must not escape render");
        },
      },
    );

    const accessorView = await render(
      <ProviderErrorBoundary>
        <ViewServerClientProvider client={inMemory.liveClient}>
          <HostileOrdersView label="accessor query" query={accessorQuery} />
        </ViewServerClientProvider>
      </ProviderErrorBoundary>,
    );
    await expect
      .element(accessorView.getByText("error:InvalidQuery", { exact: true }))
      .toBeVisible();
    expect(getterReads).toBe(0);
    await accessorView.unmount();

    const sparseView = await render(
      <ProviderErrorBoundary>
        <ViewServerClientProvider client={inMemory.liveClient}>
          <HostileOrdersView label="sparse query" query={sparseQuery} />
        </ViewServerClientProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(sparseView.getByText("error:InvalidQuery", { exact: true })).toBeVisible();
    await sparseView.unmount();

    const symbolicView = await render(
      <ProviderErrorBoundary>
        <ViewServerClientProvider client={inMemory.liveClient}>
          <HostileOrdersView label="symbolic query" query={symbolicQuery} />
        </ViewServerClientProvider>
      </ProviderErrorBoundary>,
    );
    await expect
      .element(symbolicView.getByText("error:InvalidQuery", { exact: true }))
      .toBeVisible();
    await symbolicView.unmount();

    const proxyView = await render(
      <ProviderErrorBoundary>
        <ViewServerClientProvider client={inMemory.liveClient}>
          <HostileOrdersView label="proxy query" query={proxyQuery} />
        </ViewServerClientProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(proxyView.getByText("error:InvalidQuery", { exact: true })).toBeVisible();
    await proxyView.unmount();

    const unknownTopicView = await render(
      <ProviderErrorBoundary>
        <ViewServerClientProvider client={inMemory.liveClient}>
          <HostileTopicView />
        </ViewServerClientProvider>
      </ProviderErrorBoundary>,
    );
    await expect
      .element(unknownTopicView.getByText("error:InvalidTopic", { exact: true }))
      .toBeVisible();
    await unknownTopicView.unmount();
    await Effect.runPromise(inMemory.close);
  });

  it("keeps the in-memory engine open while a mounted provider has no hook consumers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("orders: a:10", { exact: true })).toBeVisible();

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await Effect.runPromise(client.publish("orders", order("b", 20)));

    await view.rerender(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: a:10|b:20", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("applies update, move, remove, patch, snapshot, and reset paths", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publishMany("orders", [order("a", 10), order("b", 20)]));
    await expect.element(view.getByText("orders: a:10|b:20", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publish("orders", order("a", 30)));
    await expect.element(view.getByText("orders: b:20|a:30", { exact: true })).toBeVisible();

    await Effect.runPromise(client.patch("orders", "a", { price: 5 }));
    await expect.element(view.getByText("orders: a:5|b:20", { exact: true })).toBeVisible();

    await Effect.runPromise(client.delete("orders", "a"));
    await expect.element(view.getByText("orders: b:20", { exact: true })).toBeVisible();

    const snapshot = await Effect.runPromise(
      client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      }),
    );
    expect(snapshot.rows).toStrictEqual([{ id: "b", price: 20 }]);

    await Effect.runPromise(client.reset());
    expect((await Effect.runPromise(client.health())).engine.topics.orders.rowCount).toBe(0);
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("coalesces in-memory health refreshes under concurrent publishes", async () => {
    const { client } = createInMemoryViewServer();

    await Promise.all(
      Array.from({ length: 50 }, (_, index) =>
        Effect.runPromise(client.publish("orders", order(`coalesced-${index}`, index))),
      ),
    );

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.rowCount;
      })
      .toBe(50);
  });

  it("surfaces live query failures as error results", async () => {
    const { ViewServerInMemoryProvider } = createInMemoryViewServer();

    function BrokenOrdersView() {
      // @ts-expect-error invalid selected fields are still surfaced through the hook result.
      const result = useLiveQuery("orders", {
        select: ["prcie"],
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <BrokenOrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("error:InvalidQuery", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("maps runtime errors", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    const view = await render(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await Effect.runPromise(client.publish("orders", order("a", 10)));

    const invalidTopic = await Effect.runPromise(
      Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown topics.
        client.publish("missing", order("b", 20)),
      ),
    );
    const invalidRow = await Effect.runPromise(
      Effect.flip(
        client.publish("orders", {
          id: "bad",
          customerId: "customer-bad",
          // @ts-expect-error hostile runtime callers can still send malformed rows.
          status: "unknown",
          price: 20,
          region: "usa",
          updatedAt: 20,
        }),
      ),
    );
    const groupedSnapshot = await Effect.runPromise(
      client.snapshot("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
      }),
    );
    const invalidQuery = await Effect.runPromise(
      Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown projected fields.
        client.snapshot("orders", {
          select: ["prcie"],
        }),
      ),
    );

    expect(invalidTopic.code).toBe("InvalidTopic");
    expect(invalidRow.code).toBe("InvalidRow");
    expect(groupedSnapshot.rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
    expect(invalidQuery.code).toBe("InvalidQuery");
    await view.unmount();
  });

  it("keeps query memoization safe for bigint query values", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function TradesView() {
      const result = useLiveQuery("trades", {
        where: [{ field: "quantity", type: "greaterThanOrEqual", filter: 10n }],
        select: ["id", "quantity"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.quantity}`).join("|");
      return (
        <output aria-label="trades" role="status">
          {rows === "" ? "trades: none" : `trades: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <TradesView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("trades: none", { exact: true })).toBeVisible();

    await Effect.runPromise(
      client.publishMany("trades", [
        { id: "a", symbol: "AAPL", quantity: 5n, price: 100, region: "usa" },
        { id: "b", symbol: "MSFT", quantity: 10n, price: 200, region: "usa" },
      ]),
    );

    await expect.element(view.getByText("trades: b:10", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("closes the owned in-memory runtime after provider disposal", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function HealthView() {
      const health = useViewServerHealthSummary();
      return (
        <output aria-label="health" role="status">
          {health.status}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("ready", { exact: true })).toBeVisible();

    await view.unmount();
    await expect
      .poll(async () => {
        return Effect.runPromise(Effect.flip(client.publish("orders", order("a", 10)))).then(
          (error) => error.code,
          () => "success",
        );
      })
      .toBe("RuntimeUnavailable");
  });

  it("surfaces closed status and clears rows when runtime closes while subscribed", async () => {
    const { ViewServerInMemoryProvider, client, close } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}:{result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("ready:Ready:a", { exact: true })).toBeVisible();

    await Effect.runPromise(close);

    await expect
      .element(view.getByText("closed:SubscriptionClosed:", { exact: true }))
      .toBeVisible();
    await view.unmount();
  });

  it("returns close for disposing in-memory helpers without mounting a provider", async () => {
    const { client, close } = createInMemoryViewServer();

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await Effect.runPromise(close);

    const health = await Effect.runPromise(client.health());
    expect(health.status).toBe("stopping");

    await expect
      .poll(async () => {
        return Effect.runPromise(Effect.flip(client.publish("orders", order("b", 20)))).then(
          (error) => error.code,
          () => "success",
        );
      })
      .toBe("RuntimeUnavailable");
  });

  it("surfaces status events from bounded subscription queues", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer({
      subscriptionQueueCapacity: 1,
    });

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("ready:Ready", { exact: true })).toBeVisible();

    for (let index = 0; index < 50; index += 1) {
      await Effect.runPromise(client.publish("orders", order(`burst-${index}`, index)));
    }

    expect((await Effect.runPromise(client.health())).transport.backpressureEvents).toBe(1);
    await expect
      .element(view.getByText("closed:BackpressureExceeded", { exact: true }))
      .toBeVisible();
    await view.unmount();
  });
});
