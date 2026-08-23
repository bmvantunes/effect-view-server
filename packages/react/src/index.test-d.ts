import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveClient } from "@effect-view-server/client";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import {
  ViewServerId,
  defineViewServerConfig,
  type LiveQueryResult,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { createViewServerReact as createViewServerReactFromPackage } from "@effect-view-server/react";
import {
  createInMemoryViewServerReact as createInMemoryViewServerReactFromPackageTesting,
  makeInMemoryViewServerReact as makeInMemoryViewServerReactFromPackageTesting,
  type ViewServerInMemoryOptions as ViewServerInMemoryOptionsFromPackageTesting,
} from "@effect-view-server/react/testing";
import { Context, type Effect, Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import type * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import type { ReactNode } from "react";
import { createViewServerReact } from "./index";
import { ViewServerReactClientProvider } from "./internal";
import {
  createInMemoryViewServerReact,
  makeInMemoryViewServerReact,
  type ViewServerInMemoryOptions,
} from "./testing";

const Order = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Position = Schema.Struct({
  id: ViewServerId,
  quantity: Schema.Number,
});

const ReactSourceFailure = Schema.TaggedStruct("ReactSourceFailure", {
  code: Schema.Literals(["Disconnected", "Decode"]),
});
const ReactSourceRejectionLocation = Schema.Struct({
  partition: Schema.String,
  offset: Schema.BigInt,
});

const sourceAdapter = SourceAdapter.make({
  identity: { name: "react-type-source" },
  failure: ReactSourceFailure,
  materialized: {
    metrics: Schema.Struct({ observed: Schema.BigInt }),
    rejectionLocation: ReactSourceRejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<undefined>(),
  },
  leased: {
    metrics: Schema.Struct({ observed: Schema.BigInt }),
    rejectionLocation: ReactSourceRejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<undefined>(),
  },
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const heterogeneousViewServer = defineViewServerConfig({
  topics: {
    orders: { schema: Order },
    positions: { schema: Position },
  },
});

const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["region", "status"], undefined),
    },
    allOrders: {
      schema: Order,
      source: sourceAdapter.materializedSource(undefined),
    },
  },
});

const react = createViewServerReact(viewServer);
const { ViewServerProvider, useLiveQuery, useViewServerHealth, useViewServerHealthSummary } = react;
const ViewServerClientProvider = react[ViewServerReactClientProvider];
const leasedReact = createViewServerReact(leasedViewServer);
const heterogeneousReact = createViewServerReact(heterogeneousViewServer);

type TestInMemoryOptions = ViewServerInMemoryOptions<typeof viewServer.topics>;

const createInMemoryViewServer = (options?: TestInMemoryOptions) =>
  createInMemoryViewServerReact(react, options);

declare const liveClient: ViewServerLiveClient<typeof viewServer.topics>;

declare const dynamicSingleField: "id" | "price";
declare const heterogeneousTopic: "orders" | "positions";

describe("React type contracts", () => {
  it("preserves selected row result types", () => {
    const selected = useLiveQuery("orders", {
      select: ["id", "price"],
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 5,
    });

    expectTypeOf(selected).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
  });

  it("requires dynamic topic-union filters to exist on every possible topic", () => {
    const common = heterogeneousReact.useLiveQuery(heterogeneousTopic, {
      select: ["id"],
      where: [{ field: "id", type: "equals", filter: "row-1" }],
    });
    const orderOnlyQuery = {
      select: ["id"],
      where: [{ field: "price", type: "greaterThan", filter: 10 }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        { readonly field: "price"; readonly type: "greaterThan"; readonly filter: 10 },
      ];
    };
    const invalid = heterogeneousReact.useLiveQuery(
      heterogeneousTopic,
      // @ts-expect-error dynamic topic-union queries must be valid for every possible topic.
      orderOnlyQuery,
    );

    expectTypeOf(common.rows[0]).toEqualTypeOf<{ readonly id: string } | undefined>();
    expectTypeOf(invalid).not.toBeAny();
  });

  it("requires explicit selected row result types", () => {
    const selectedRows = useLiveQuery("orders", {
      select: ["id", "customerId", "status", "price", "region", "updatedAt"],
      where: [
        { field: "status", type: "equals", filter: "open" },
        { field: "customerId", type: "startsWith", filter: "customer-" },
        { field: "price", type: "greaterThanOrEqual", filter: 10 },
      ],
      orderBy: [{ field: "updatedAt", direction: "asc" }],
      limit: 10,
    });

    expectTypeOf(selectedRows.rows[0]).toEqualTypeOf<
      | {
          readonly id: string;
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly region: string;
          readonly updatedAt: number;
        }
      | undefined
    >();
    expectTypeOf(selectedRows.status).toEqualTypeOf<
      "loading" | "ready" | "stale" | "closed" | "error"
    >();
    expectTypeOf(selectedRows.statusCode).toEqualTypeOf<
      | "Ready"
      | "SnapshotStale"
      | "SubscriptionClosed"
      | "TransportError"
      | "BackpressureExceeded"
      | "InvalidTopic"
      | "InvalidRow"
      | "InvalidQuery"
      | "UnsupportedQuery"
      | "RuntimeUnavailable"
      | "RuntimeResetFailed"
      | undefined
    >();
  });

  it("rejects invalid raw query select", () => {
    const missingSelectQuery = {
      where: [{ field: "status", type: "equals", filter: "open" }],
    };
    // @ts-expect-error raw queries must explicitly select columns.
    useLiveQuery("orders", missingSelectQuery);

    const emptySelectQuery = {
      select: [],
    };
    // @ts-expect-error raw queries must select at least one column.
    useLiveQuery("orders", emptySelectQuery);

    const unknownWhereFieldQuery = {
      select: ["id"],
      where: [{ field: "prcie", type: "equals", filter: 10 }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        { readonly field: "prcie"; readonly type: "equals"; readonly filter: 10 },
      ];
    };
    // @ts-expect-error unknown where fields are rejected.
    useLiveQuery("orders", unknownWhereFieldQuery);

    const unknownOrderByFieldQuery = {
      select: ["id"],
      orderBy: [
        {
          field: "prcie",
          direction: "asc",
        },
      ],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [
        {
          readonly field: "prcie";
          readonly direction: "asc";
        },
      ];
    };
    // @ts-expect-error unknown orderBy fields are rejected.
    useLiveQuery("orders", unknownOrderByFieldQuery);

    const unknownProjectedFieldQuery = {
      select: ["id", "prcie"],
    } satisfies {
      readonly select: readonly ["id", "prcie"];
    };
    // @ts-expect-error unknown projected fields are rejected.
    useLiveQuery("orders", unknownProjectedFieldQuery);

    const undefinedSelectedFieldQuery = {
      select: [undefined],
    } satisfies {
      readonly select: readonly [undefined];
    };
    // @ts-expect-error selected fields must be topic field names, not undefined.
    useLiveQuery("orders", undefinedSelectedFieldQuery);

    const nullSelectedFieldQuery = {
      select: [null],
    } satisfies {
      readonly select: readonly [null];
    };
    // @ts-expect-error selected fields must be topic field names, not null.
    useLiveQuery("orders", nullSelectedFieldQuery);

    const dynamicSingleTupleSelectedFieldsQuery = {
      select: [dynamicSingleField],
    } satisfies {
      readonly select: readonly [typeof dynamicSingleField];
    };
    const dynamicSelected = useLiveQuery("orders", dynamicSingleTupleSelectedFieldsQuery);
    expectTypeOf(dynamicSelected.rows[0]).toEqualTypeOf<
      Partial<{ readonly id: string; readonly price: number }> | undefined
    >();
  });

  it("rejects invalid raw query operators", () => {
    const stringRangeFilterQuery = {
      select: ["id"],
      where: [{ field: "status", type: "greaterThanOrEqual", filter: "open" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "greaterThanOrEqual";
          readonly filter: "open";
        },
      ];
    };
    // @ts-expect-error string fields do not support range filters.
    useLiveQuery("orders", stringRangeFilterQuery);

    const numericStringFilterQuery = {
      select: ["id"],
      where: [{ field: "price", type: "startsWith", filter: "10" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "price";
          readonly type: "startsWith";
          readonly filter: "10";
        },
      ];
    };
    // @ts-expect-error numeric fields do not support string filters.
    useLiveQuery("orders", numericStringFilterQuery);
  });

  it("requires exact leased Source route values in React hooks", () => {
    const routedRows = leasedReact.useLiveQuery("orders", {
      where: [
        { field: "region", type: "equals", filter: "usa" },
        { field: "status", type: "equals", filter: "open" },
        { field: "customerId", type: "startsWith", filter: "customer-" },
      ],
      routeBy: { region: "UsÁ", status: "open" },
      orderBy: [{ field: "updatedAt", direction: "desc" }],
      select: ["id", "customerId", "price"],
      limit: 25,
    });

    expectTypeOf(routedRows).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly customerId: string;
        readonly price: number;
      }>
    >();

    const missingRouteQuery = {
      where: [{ field: "region", type: "equals", filter: "usa" }],
      select: ["id"],
    } satisfies {
      readonly where: readonly [
        { readonly field: "region"; readonly type: "equals"; readonly filter: "usa" },
      ];
      readonly select: readonly ["id"];
    };
    const partialRouteQuery = {
      where: [
        { field: "region", type: "equals", filter: "usa" },
        { field: "status", type: "in", filter: ["open"] },
      ],
      routeBy: { region: "UsÁ" },
      select: ["id"],
    } satisfies {
      readonly where: readonly [
        { readonly field: "region"; readonly type: "equals"; readonly filter: "usa" },
        { readonly field: "status"; readonly type: "in"; readonly filter: readonly ["open"] },
      ];
      readonly routeBy: { readonly region: "UsÁ" };
      readonly select: readonly ["id"];
    };

    // @ts-expect-error leased Source queries require every routeBy field.
    leasedReact.useLiveQuery("orders", missingRouteQuery);

    // @ts-expect-error leased Source routeBy must contain every configured route field.
    leasedReact.useLiveQuery("orders", partialRouteQuery);
  });

  it("binds exact whole-result queries to one Viewport Source topic", () => {
    const source = leasedReact.useLiveQueryViewport("orders");
    const facet = source.useWholeResult({
      routeBy: { region: "UsÁ", status: "open" },
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where: [{ field: "customerId", type: "startsWith", filter: "customer-" }],
      orderBy: [{ field: "status", direction: "asc" }],
    });

    expectTypeOf(facet).toEqualTypeOf<
      LiveQueryResult<{
        readonly status: "open" | "closed" | "cancelled";
        readonly rowCount: bigint;
      }>
    >();

    // @ts-expect-error the topic-bound whole-result query requires every Route Field.
    source.useWholeResult({
      routeBy: { region: "UsÁ" },
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    // @ts-expect-error the topic-bound whole-result query rejects extra Route Fields.
    source.useWholeResult({
      routeBy: { region: "UsÁ", status: "open", desk: "north" },
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    // @ts-expect-error the topic-bound whole-result query preserves exact Route values.
    source.useWholeResult({
      routeBy: { region: 1, status: "open" },
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where: [],
      orderBy: [],
    });
    // @ts-expect-error a whole-result query cannot truncate with offset.
    source.useWholeResult({
      routeBy: { region: "UsÁ", status: "open" },
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where: [],
      orderBy: [],
      offset: 1,
    });
    // @ts-expect-error a whole-result query cannot truncate with limit.
    source.useWholeResult({
      routeBy: { region: "UsÁ", status: "open" },
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where: [],
      orderBy: [],
      limit: 10,
    });
  });

  it("preserves exact Materialized and Leased Source Health diagnostics", () => {
    const materialized = leasedReact.useSourceHealth({ topic: "allOrders" });
    const leased = leasedReact.useSourceHealth({
      topic: "orders",
      routeBy: { region: "usa", status: "open" },
    });
    type MaterializedHealth = AsyncResult.AsyncResult.Success<typeof materialized>;
    type LeasedHealth = AsyncResult.AsyncResult.Success<typeof leased>;
    type MaterializedDegraded = Extract<
      MaterializedHealth["status"],
      { readonly _tag: "Degraded" }
    >;
    type LeasedActive = Extract<LeasedHealth, { readonly _tag: "Active" }>;
    type LeasedDegraded = Extract<LeasedActive["health"]["status"], { readonly _tag: "Degraded" }>;
    type MaterializedRejection = Extract<
      MaterializedDegraded["reasons"][number],
      { readonly _tag: "SourceItemRejection" }
    >["latestRejection"];
    type LeasedRejection = Extract<
      LeasedDegraded["reasons"][number],
      { readonly _tag: "SourceItemRejection" }
    >["latestRejection"];
    type MaterializedMaintenance = Extract<
      MaterializedDegraded["reasons"][number],
      { readonly _tag: "AdapterMaintenanceFailure" }
    >;
    type MaterializedCombined = Extract<
      MaterializedDegraded["reasons"],
      readonly [
        { readonly _tag: "SourceItemRejection" },
        { readonly _tag: "AdapterMaintenanceFailure" },
      ]
    >;
    type MaterializedExhausted = Extract<
      MaterializedHealth["status"],
      { readonly _tag: "Exhausted" }
    >;
    type MaterializedInvalidSettlement = Extract<
      Extract<
        MaterializedExhausted["exhaustion"]["lastTermination"],
        { readonly _tag: "Failed" }
      >["failure"],
      { readonly _tag: "RuntimeFailure" }
    >["failure"];

    expectTypeOf<MaterializedHealth["metrics"]["adapter"]["observed"]>().toEqualTypeOf<bigint>();
    expectTypeOf<
      Extract<MaterializedRejection["failure"], { readonly _tag: "AdapterFailure" }>["failure"]
    >().toEqualTypeOf<typeof ReactSourceFailure.Type>();
    expectTypeOf<MaterializedRejection["location"]>().toEqualTypeOf<
      typeof ReactSourceRejectionLocation.Type
    >();
    expectTypeOf<MaterializedMaintenance>().toEqualTypeOf<{
      readonly _tag: "AdapterMaintenanceFailure";
    }>();
    expectTypeOf<MaterializedCombined[0]["_tag"]>().toEqualTypeOf<"SourceItemRejection">();
    expectTypeOf<MaterializedCombined[1]["_tag"]>().toEqualTypeOf<"AdapterMaintenanceFailure">();
    expectTypeOf<
      Extract<
        MaterializedInvalidSettlement,
        { readonly _tag: "InvalidSourceSettlement" }
      >["message"]
    >().toEqualTypeOf<"Source Settlement callback threw before returning an Effect">();
    expectTypeOf<MaterializedHealth["status"]["_tag"]>().toEqualTypeOf<
      | "Starting"
      | "Ready"
      | "Degraded"
      | "WaitingToRetry"
      | "Reacquiring"
      | "Exhausted"
      | "Stopping"
    >();
    expectTypeOf<LeasedHealth["_tag"]>().toEqualTypeOf<"Inactive" | "Active">();
    expectTypeOf<LeasedHealth["route"]>().toEqualTypeOf<{
      readonly region: string;
      readonly status: "open" | "closed" | "cancelled";
    }>();
    expectTypeOf<
      Extract<LeasedRejection["failure"], { readonly _tag: "AdapterFailure" }>["failure"]
    >().toEqualTypeOf<typeof ReactSourceFailure.Type>();
    expectTypeOf<LeasedRejection["location"]>().toEqualTypeOf<
      typeof ReactSourceRejectionLocation.Type
    >();
    const unknownDegradationReason: MaterializedDegraded["reasons"][number] = {
      // @ts-expect-error Source Health rejects unknown degradation-reason variants.
      _tag: "UnknownDegradationReason",
    };
    const unknownRuntimeFailure: MaterializedInvalidSettlement = {
      // @ts-expect-error Source Health rejects unknown runtime-failure variants.
      _tag: "UnknownRuntimeFailure",
    };
    void unknownDegradationReason;
    void unknownRuntimeFailure;

    // @ts-expect-error source-free Topics do not expose Source Health.
    react.useSourceHealth({ topic: "orders" });
    // @ts-expect-error Leased Source diagnostics require routeBy.
    leasedReact.useSourceHealth({ topic: "orders" });
    const materializedWithRoute: {
      readonly topic: "allOrders";
      readonly routeBy: {
        readonly region: string;
        readonly status: "open";
      };
    } = {
      topic: "allOrders",
      routeBy: { region: "usa", status: "open" },
    };
    // @ts-expect-error Materialized Source diagnostics reject routeBy.
    leasedReact.useSourceHealth(materializedWithRoute);
    leasedReact.useSourceHealth({
      topic: "orders",
      // @ts-expect-error Leased Source diagnostics require every route field.
      routeBy: { region: "usa" },
    });
    leasedReact.useSourceHealth({
      topic: "orders",
      routeBy: {
        region: "usa",
        status: "open",
        // @ts-expect-error Leased Source diagnostics reject extra route fields.
        extra: true,
      },
    });
    leasedReact.useSourceHealth({
      topic: "orders",
      // @ts-expect-error Leased Source diagnostics preserve route scalar types.
      routeBy: {
        region: "usa",
        status: "unknown",
      },
    });
    leasedReact.useSourceHealth({
      topic: "orders",
      routeBy: { region: "usa", status: "open" },
      // @ts-expect-error Source Health diagnostics reject extra input fields.
      adapter: "react-type-source",
    });
  });

  it("keeps health and in-memory client keyed by configured topics", () => {
    const health = useViewServerHealth();
    const healthSummary = useViewServerHealthSummary();
    const provider = ViewServerProvider({ url: "ws://127.0.0.1:8080/rpc", children: null });
    const clientProvider = ViewServerClientProvider({ client: liveClient, children: null });
    const inMemoryViewServer = createInMemoryViewServer({ subscriptionQueueCapacity: 1 });
    type Client = typeof inMemoryViewServer.client;
    const publish = inMemoryViewServer.client.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });

    expectTypeOf(health.rows[0]?.rowCount).toEqualTypeOf<number | undefined>();
    expectTypeOf(health.rows[0]?.id).toEqualTypeOf<"orders" | undefined>();
    expectTypeOf(healthSummary.status).toEqualTypeOf<
      "ready" | "degraded" | "starting" | "stopping" | "connecting" | "disconnected"
    >();
    expectTypeOf(healthSummary).not.toHaveProperty("maxKafkaLag");
    expectTypeOf(provider).toEqualTypeOf<ReactNode>();
    expectTypeOf(clientProvider).toEqualTypeOf<ReactNode>();
    expectTypeOf<Parameters<Client["publish"]>>().toEqualTypeOf<
      [topic: "orders", row: typeof Order.Type]
    >();
    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
  });

  it("rejects provider seed data", () => {
    const inMemoryViewServer = createInMemoryViewServer();
    void inMemoryViewServer.ViewServerInMemoryProvider({
      children: null,
      // @ts-expect-error setup data must go through runtime.publish or runtime.publishMany.
      seed: {},
    });
  });

  it("requires testing helpers to reuse React bindings", () => {
    // @ts-expect-error testing helpers need the app binding, not just the config.
    createInMemoryViewServerReact(viewServer);
    // @ts-expect-error synchronous testing helpers cannot provide Source Adapter services.
    createInMemoryViewServerReact(leasedReact);
  });

  it("preserves grouped query result types for React and in-memory clients", () => {
    const { client } = createInMemoryViewServer();
    const groupedRows = useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: {
        rowCount: { aggFunc: "count" },
        totalPrice: { aggFunc: "sum", field: "price" },
      },
      orderBy: [
        { field: "status", direction: "asc" },
        { aggregate: "totalPrice", direction: "desc" },
      ],
    });
    const groupedSnapshot = client.snapshot("orders", {
      groupBy: ["status"],
      aggregates: {
        rowCount: { aggFunc: "count" },
        totalPrice: { aggFunc: "sum", field: "price" },
      },
      orderBy: [
        { field: "status", direction: "asc" },
        { aggregate: "totalPrice", direction: "desc" },
      ],
    });

    const invalidPatch = client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });

    expectTypeOf(groupedRows).toEqualTypeOf<
      LiveQueryResult<{
        readonly status: "open" | "closed" | "cancelled";
        readonly rowCount: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
      }>
    >();
    expectTypeOf<Effect.Success<typeof groupedSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{
        readonly status: "open" | "closed" | "cancelled";
        readonly rowCount: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
      }>
    >();
    expectTypeOf(invalidPatch).not.toBeAny();

    // @ts-expect-error grouped orderBy field must be present in groupBy.
    useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      orderBy: [{ field: "price", direction: "asc" }],
    });

    // @ts-expect-error grouped orderBy aggregate must reference an aggregate alias.
    useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
    });
  });

  it("preserves consumer types through @effect-view-server/react package imports", () => {
    const consumerReact = createViewServerReactFromPackage(viewServer);
    const selected = consumerReact.useLiveQuery("orders", {
      select: ["id", "price"],
    });
    const provider = consumerReact.ViewServerProvider({
      url: "ws://127.0.0.1:8080/rpc",
      children: null,
    });

    expectTypeOf(selected).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    expectTypeOf(provider).toEqualTypeOf<ReactNode>();

    void consumerReact.ViewServerProvider({
      // @ts-expect-error public production provider accepts a URL, not a caller-owned client.
      client: liveClient,
      children: null,
    });

    // @ts-expect-error consumer package imports still reject unknown selected fields.
    consumerReact.useLiveQuery("orders", {
      select: ["prcie"],
    });

    // @ts-expect-error consumer package imports still reject undefined selected fields.
    consumerReact.useLiveQuery("orders", {
      select: [undefined],
    });

    // @ts-expect-error consumer package imports still reject null selected fields.
    consumerReact.useLiveQuery("orders", {
      select: [null],
    });
  });

  it("preserves consumer testing types through @effect-view-server/react/testing package imports", () => {
    const consumerReact = createViewServerReactFromPackage(viewServer);
    const options = {
      subscriptionQueueCapacity: 1,
    } satisfies ViewServerInMemoryOptionsFromPackageTesting;
    const inMemory = createInMemoryViewServerReactFromPackageTesting(consumerReact, options);
    const provider = inMemory.ViewServerInMemoryProvider({ children: null });
    const publish = inMemory.client.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });

    expectTypeOf(provider).toEqualTypeOf<ReactNode>();
    expectTypeOf<Parameters<typeof inMemory.client.publish>>().toEqualTypeOf<
      [topic: "orders", row: typeof Order.Type]
    >();
    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();

    // @ts-expect-error testing helper consumers must pass React bindings, not config.
    createInMemoryViewServerReactFromPackageTesting(viewServer);

    const invalidPublish = inMemory.client.publish("orders", {
      id: "order-2",
      customerId: "customer-2",
      status: "open",
      price: 42,
      region: "usa",
      // @ts-expect-error consumer testing client keeps exact topic row requirements.
      updateddAt: 1,
    });
    expectTypeOf(invalidPublish).not.toBeAny();
  });

  it("preserves source Layer requirements through React testing helpers", () => {
    const localEffect = makeInMemoryViewServerReact(leasedReact);
    const packageEffect = makeInMemoryViewServerReactFromPackageTesting(leasedReact);

    expectTypeOf<Effect.Services<typeof localEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Services<typeof packageEffect>>().toEqualTypeOf<
      Context.Service.Identifier<typeof sourceAdapter.runtimeService>
    >();
    expectTypeOf<Effect.Success<typeof localEffect>>().toEqualTypeOf<
      Effect.Success<typeof packageEffect>
    >();
  });
});
