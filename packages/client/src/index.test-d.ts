import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  grpc,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
} from "@effect-view-server/config";
import type {
  ExactLiveQueryInputForTopic,
  ExactLiveQuery,
  ExactRawQuery,
  FilterExpression,
  GrpcRuntimeClients,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
  TopicRow,
} from "@effect-view-server/config";
import type { Effect } from "effect";
import type { Stream } from "effect";
import { Schema } from "effect";
import { stableQueryKeyForRowSchema } from "./index";
import type { ViewServerLiveClient, ViewServerLiveSubscription } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const Position = Schema.Struct({
  id: Schema.String,
  quantity: Schema.Number,
});

type ValidClientIdCondition = {
  readonly field: "id";
  readonly type: "equals";
  readonly filter: "order-1";
};

type QueryUnionWithInvalidWhere =
  | { readonly select: readonly ["id"] }
  | {
      readonly select: readonly ["id"];
      readonly where: readonly [ValidClientIdCondition & { readonly unexpected: true }];
    };

type ValidRawOrGroupedClientQuery =
  | { readonly select: readonly ["id"] }
  | {
      readonly groupBy: readonly ["price"];
      readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
    };

type ValidRawOrInvalidGroupedClientQuery =
  | { readonly select: readonly ["id"] }
  | {
      readonly groupBy: readonly ["missing"];
      readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
    };

type InvalidRawOrValidGroupedClientQuery =
  | { readonly select: readonly ["missing"] }
  | {
      readonly groupBy: readonly ["price"];
      readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
    };

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
    orders: {
      schema: Order,
      key: "id",
    },
    positions: {
      schema: Position,
      key: "id",
    },
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
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({
        id: route.id,
        price: 0,
      }),
    }),
  },
});

const mixedSourceViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({
        id: route.id,
        price: 0,
      }),
    }),
    positions: {
      schema: Order,
      key: "id",
    },
  },
});

const mismatchedLeasedViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({ id: route.id, price: 0 }),
    }),
    positions: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["price"],
      request: ({ price }) => ({ price }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({ id: "position", price: route.price }),
    }),
  },
});

const identicalLeasedViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({ id: route.id, price: 0 }),
    }),
    positions: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({ id: route.id, price: 0 }),
    }),
  },
});

declare const client: ViewServerLiveClient<typeof viewServer.topics>;
declare const heterogeneousClient: ViewServerLiveClient<typeof heterogeneousViewServer.topics>;
declare const heterogeneousTopic: "orders" | "positions";
declare const leasedClient: ViewServerLiveClient<typeof leasedViewServer.topics>;
declare const mixedSourceClient: ViewServerLiveClient<typeof mixedSourceViewServer.topics>;
declare const mixedSourceTopic: "orders" | "positions";
declare const mismatchedLeasedClient: ViewServerLiveClient<
  typeof mismatchedLeasedViewServer.topics
>;
declare const mismatchedLeasedTopic: "orders" | "positions";
declare const identicalLeasedClient: ViewServerLiveClient<typeof identicalLeasedViewServer.topics>;
declare const identicalLeasedTopic: "orders" | "positions";

describe("client type contracts", () => {
  it("types schema-aware stable query identity", () => {
    expectTypeOf(stableQueryKeyForRowSchema({ select: ["id"] }, Order)).toEqualTypeOf<string>();

    // @ts-expect-error schema-aware identity requires an admitted row schema.
    stableQueryKeyForRowSchema({ select: ["id"] }, { fields: {} });
  });

  it("preserves selected row types through live subscriptions", () => {
    const canonicalExpression: FilterExpression<typeof Order.Type> = {
      field: "id",
      type: "equals",
      filter: "order-1",
    };
    const subscription = client.subscribe("orders", {
      select: ["id"],
      where: [canonicalExpression],
    });

    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf<Effect.Error<typeof subscription>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();

    const acceptValidRawOrGroupedUnion = (query: ValidRawOrGroupedClientQuery) => {
      const mixedSubscription = client.subscribe("orders", query);
      expectTypeOf<Effect.Success<typeof mixedSubscription>>().toEqualTypeOf<
        ViewServerLiveSubscription<
          { readonly id: string } | { readonly price: number; readonly rowCount: bigint }
        >
      >();
    };
    expectTypeOf(acceptValidRawOrGroupedUnion).toBeFunction();

    const rejectValidRawOrInvalidGroupedUnion = (query: ValidRawOrInvalidGroupedClientQuery) => {
      // @ts-expect-error one invalid grouped member poisons the whole subscription query union.
      const rejected = client.subscribe("orders", query);
      expectTypeOf(rejected).not.toBeAny();
    };
    expectTypeOf(rejectValidRawOrInvalidGroupedUnion).toBeFunction();
    expectTypeOf<
      ExactLiveQuery<typeof Order.Type, ValidRawOrInvalidGroupedClientQuery>
    >().toBeNever();

    const rejectInvalidRawOrValidGroupedUnion = (query: InvalidRawOrValidGroupedClientQuery) => {
      // @ts-expect-error one invalid raw member poisons the whole subscription query union.
      const rejected = client.subscribe("orders", query);
      expectTypeOf(rejected).not.toBeAny();
    };
    expectTypeOf(rejectInvalidRawOrValidGroupedUnion).toBeFunction();
    expectTypeOf<
      ExactLiveQuery<typeof Order.Type, InvalidRawOrValidGroupedClientQuery>
    >().toBeNever();

    const rejectQueryUnion = (query: QueryUnionWithInvalidWhere) => {
      // @ts-expect-error every whole-query union member must be exact.
      const rejected = client.subscribe("orders", query);
      expectTypeOf(rejected).not.toBeAny();
    };
    expectTypeOf(rejectQueryUnion).toBeFunction();
    expectTypeOf<ExactRawQuery<typeof Order.Type, QueryUnionWithInvalidWhere>>().toBeNever();
    expectTypeOf<ExactLiveQuery<typeof Order.Type, QueryUnionWithInvalidWhere>>().toBeNever();
    expectTypeOf<
      ExactLiveQueryInputForTopic<typeof viewServer.topics, "orders", QueryUnionWithInvalidWhere>
    >().toBeNever();
  });

  it("rejects nullish selected fields", () => {
    // @ts-expect-error selected fields must be topic field names, not undefined.
    const undefinedSelectedField = client.subscribe("orders", {
      select: [undefined],
    });

    // @ts-expect-error selected fields must be topic field names, not null.
    const nullSelectedField = client.subscribe("orders", {
      select: [null],
    });

    expectTypeOf(undefinedSelectedField).not.toBeAny();
    expectTypeOf(nullSelectedField).not.toBeAny();
  });

  it("requires dynamic topic-union queries to be valid for every possible topic", () => {
    const commonSubscription = heterogeneousClient.subscribe(heterogeneousTopic, {
      select: ["id"],
      where: [{ field: "id", type: "equals", filter: "row-1" }],
    });
    const topicSpecificFilter = {
      select: ["id"],
      where: [{ field: "price", type: "greaterThan", filter: 10 }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        { readonly field: "price"; readonly type: "greaterThan"; readonly filter: 10 },
      ];
    };
    // @ts-expect-error positions do not have a price filter field.
    const invalidExactLiveQuery: ExactLiveQuery<
      TopicRow<typeof heterogeneousViewServer.topics, "positions">,
      typeof topicSpecificFilter
    > = topicSpecificFilter;
    // @ts-expect-error positions do not have a price filter field.
    const invalidExactInput: ExactLiveQueryInputForTopic<
      typeof heterogeneousViewServer.topics,
      "positions",
      typeof topicSpecificFilter
    > = topicSpecificFilter;
    // @ts-expect-error a topic union requires filters valid for every member.
    const invalidUnionExactInput: ExactLiveQueryInputForTopic<
      typeof heterogeneousViewServer.topics,
      "orders" | "positions",
      typeof topicSpecificFilter
    > = topicSpecificFilter;
    const invalidSubscription = heterogeneousClient.subscribe(
      heterogeneousTopic,
      // @ts-expect-error dynamic topic-union filters must exist on every possible topic.
      topicSpecificFilter,
    );

    expectTypeOf<Effect.Success<typeof commonSubscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{ readonly id: string }>
    >();
    expectTypeOf(invalidSubscription).not.toBeAny();
    expectTypeOf(invalidExactLiveQuery).not.toBeAny();
    expectTypeOf(invalidExactInput).not.toBeAny();
    expectTypeOf(invalidUnionExactInput).not.toBeAny();
  });

  it("requires leased gRPC route values in live subscriptions", () => {
    const routedSubscription = leasedClient.subscribe("orders", {
      where: [{ field: "id", type: "equals", filter: "order-1" }],
      routeBy: { id: "Order-Á" },
      select: ["id", "price"],
    });
    const missingRouteQuery = {
      select: ["id"],
    } satisfies {
      readonly select: readonly ["id"];
    };
    const wrongRouteValueQuery = {
      routeBy: { id: 1 },
      select: ["id"],
    } satisfies {
      readonly routeBy: { readonly id: 1 };
      readonly select: readonly ["id"];
    };
    // @ts-expect-error leased gRPC subscriptions require routeBy.
    const missingRouteSubscription = leasedClient.subscribe("orders", missingRouteQuery);
    // @ts-expect-error leased gRPC routeBy values must match their configured fields.
    const wrongRouteValueSubscription = leasedClient.subscribe("orders", wrongRouteValueQuery);

    expectTypeOf<Effect.Success<typeof routedSubscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    expectTypeOf(missingRouteSubscription).not.toBeAny();
    expectTypeOf(wrongRouteValueSubscription).not.toBeAny();
  });

  it("rejects ambiguous route ownership until a topic union is narrowed", () => {
    const routedUnionQuery = {
      where: [{ field: "id", type: "equals", filter: "order-1" }],
      routeBy: { id: "Order-Á" },
      select: ["id"],
    } satisfies {
      readonly where: readonly [
        { readonly field: "id"; readonly type: "equals"; readonly filter: "order-1" },
      ];
      readonly routeBy: { readonly id: "Order-Á" };
      readonly select: readonly ["id"];
    };
    const missingRouteQuery = {
      select: ["id"],
    } satisfies {
      readonly select: readonly ["id"];
    };

    // @ts-expect-error dynamic topic unions cannot safely correlate leased and ordinary routes.
    const routedUnionSubscription = mixedSourceClient.subscribe(mixedSourceTopic, routedUnionQuery);
    const missingRouteSubscription = mixedSourceClient.subscribe(
      mixedSourceTopic,
      // @ts-expect-error dynamic topic unions cannot safely correlate leased and ordinary routes.
      missingRouteQuery,
    );

    expectTypeOf(routedUnionSubscription).not.toBeAny();
    expectTypeOf(missingRouteSubscription).not.toBeAny();
  });

  it("correlates leased topic unions only when their route contracts are identical", () => {
    const combinedRoute = {
      routeBy: { id: "order-1", price: 10 },
      select: ["id"],
    } satisfies {
      readonly routeBy: { readonly id: "order-1"; readonly price: 10 };
      readonly select: readonly ["id"];
    };
    // @ts-expect-error leased topic unions with different route contracts cannot be correlated.
    const mismatched = mismatchedLeasedClient.subscribe(mismatchedLeasedTopic, combinedRoute);
    const identical = identicalLeasedClient.subscribe(identicalLeasedTopic, {
      routeBy: { id: "Order-Á" },
      select: ["id"],
    });

    expectTypeOf(mismatched).not.toBeAny();
    expectTypeOf<Effect.Success<typeof identical>>().toEqualTypeOf<
      ViewServerLiveSubscription<{ readonly id: string }>
    >();
  });

  it("exposes health as a read-only ref", () => {
    expectTypeOf(client.health.value).toEqualTypeOf<ViewServerHealth<typeof viewServer.topics>>();

    // @ts-expect-error public live client health must not expose mutation.
    client.health.set(client.health.value);
  });

  it("preserves pushed health subscription row and error types", () => {
    const summary = client.subscribeHealthSummary();
    const details = client.subscribeHealth();

    expectTypeOf<Effect.Success<typeof summary>>().toEqualTypeOf<
      ViewServerLiveSubscription<
        ViewServerHealthSummaryRow<typeof viewServer.topics>,
        typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        "summary"
      >
    >();
    expectTypeOf<Effect.Error<typeof summary>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();
    expectTypeOf<Effect.Success<typeof details>>().toEqualTypeOf<
      ViewServerLiveSubscription<
        ViewServerHealthTopicRow<"orders">,
        typeof VIEW_SERVER_HEALTH_TOPIC,
        "orders"
      >
    >();
    expectTypeOf<Effect.Error<typeof details>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();

    type SummaryEvent = Stream.Success<Effect.Success<typeof summary>["events"]>;
    type SummarySnapshot = Extract<SummaryEvent, { readonly type: "snapshot" }>;
    type SummaryDeltaOperation = Extract<
      SummaryEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<SummarySnapshot["keys"]>().toEqualTypeOf<readonly ["summary"]>();
    expectTypeOf<SummarySnapshot["rows"][0]["id"]>().toEqualTypeOf<"summary">();
    expectTypeOf<SummarySnapshot["rows"][0]["maxKafkaLag"]>().toEqualTypeOf<bigint | null>();
    expectTypeOf<SummarySnapshot["totalRows"]>().toEqualTypeOf<1>();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();

    type DetailEvent = Stream.Success<Effect.Success<typeof details>["events"]>;
    type DetailSnapshot = Extract<DetailEvent, { readonly type: "snapshot" }>;
    type DetailDeltaOperation = Extract<
      DetailEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<DetailSnapshot["keys"][number]>().toEqualTypeOf<"orders">();
    expectTypeOf<DetailSnapshot["rows"][number]["id"]>().toEqualTypeOf<"orders">();
    expectTypeOf<DetailSnapshot["rows"][number]["kafkaLag"]>().toEqualTypeOf<bigint | null>();
    expectTypeOf<
      Extract<DetailDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<DetailDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();
  });
});
