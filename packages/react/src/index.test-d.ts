import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveClient } from "@effect-view-server/client";
import {
  defineViewServerConfig,
  grpc,
  type GrpcRuntimeClients,
  type LiveQueryResult,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { createViewServerReact as createViewServerReactFromPackage } from "@effect-view-server/react";
import {
  createInMemoryViewServerReact as createInMemoryViewServerReactFromPackageTesting,
  type ViewServerInMemoryOptions as ViewServerInMemoryOptionsFromPackageTesting,
} from "@effect-view-server/react/testing";
import type { Effect } from "effect";
import type { Stream } from "effect";
import { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import type { ReactNode } from "react";
import { createViewServerReact } from "./index";
import { ViewServerReactClientProvider } from "./internal";
import { createInMemoryViewServerReact, type ViewServerInMemoryOptions } from "./testing";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Position = Schema.Struct({
  id: Schema.String,
  quantity: Schema.Number,
});

declare const grpcRuntimeClients: GrpcRuntimeClients;
declare const grpcRuntimeStream: Stream.Stream<unknown, unknown, never>;

const grpcTopicSources = grpc.topicSources(grpcRuntimeClients);

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const heterogeneousViewServer = defineViewServerConfig({
  topics: {
    orders: { schema: Order, key: "id" },
    positions: { schema: Position, key: "id" },
  },
});

const leasedViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region", "status"],
      request: (route) => route,
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({
        id: "order-1",
        customerId: "customer-1",
        status: route.status,
        price: 0,
        region: route.region,
        updatedAt: 0,
      }),
    }),
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

  it("requires exact leased gRPC route values in React hooks", () => {
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

    // @ts-expect-error leased gRPC queries require every routeBy field.
    leasedReact.useLiveQuery("orders", missingRouteQuery);

    // @ts-expect-error leased gRPC routeBy must contain every configured route field.
    leasedReact.useLiveQuery("orders", partialRouteQuery);
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
    expectTypeOf(healthSummary.maxKafkaLag).toEqualTypeOf<bigint | null>();
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

    useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      // @ts-expect-error grouped orderBy field must be present in groupBy.
      orderBy: [{ field: "price", direction: "asc" }],
    });

    useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      // @ts-expect-error grouped orderBy aggregate must reference an aggregate alias.
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

    consumerReact.useLiveQuery("orders", {
      // @ts-expect-error consumer package imports still reject unknown selected fields.
      select: ["prcie"],
    });

    consumerReact.useLiveQuery("orders", {
      // @ts-expect-error consumer package imports still reject undefined selected fields.
      select: [undefined],
    });

    consumerReact.useLiveQuery("orders", {
      // @ts-expect-error consumer package imports still reject null selected fields.
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
});
