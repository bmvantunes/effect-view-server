import { describe, expectTypeOf, it } from "@effect/vitest";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import {
  ViewServerId,
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
} from "@effect-view-server/config";
import type {
  ExactLiveQueryInputForTopic,
  ExactLiveQuery,
  ExactRawQuery,
  FilterExpression,
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
import type {
  ViewServerLiveClient,
  ViewServerLiveSubscription,
  ViewServerSourceHealthResultForTopic,
} from "./index";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

const Position = Schema.Struct({
  id: ViewServerId,
  quantity: Schema.Number,
});

const SourceFailure = Schema.TaggedStruct("ClientTypeSourceFailure", {
  message: Schema.String,
});
const SourceDeclaration = {
  metrics: Schema.Struct({ observed: Schema.BigInt }),
  rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
  definitionOptions: SourceAdapter.definitionOptions<{
    readonly stream: string;
  }>(),
};
const sourceAdapter = SourceAdapter.make({
  identity: { name: "client-type-source" },
  failure: SourceFailure,
  materialized: SourceDeclaration,
  leased: SourceDeclaration,
});
const SourceRow = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
  shard: Schema.BigInt,
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

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const sourceViewServer = defineViewServerConfig({
  topics: {
    all: {
      schema: SourceRow,
      source: sourceAdapter.materializedSource({ stream: "all" }),
    },
    routed: {
      schema: SourceRow,
      source: sourceAdapter.leasedSource(["region", "shard"], { stream: "routed" }),
    },
    manual: {
      schema: SourceRow,
    },
  },
});

declare const useLeasedSource: boolean;
declare const useRegionRoute: boolean;
const conditionalSourceViewServer = defineViewServerConfig({
  topics: {
    conditional: {
      schema: SourceRow,
      source: useLeasedSource
        ? sourceAdapter.leasedSource(["region", "shard"], {
            stream: "conditional-leased",
          })
        : sourceAdapter.materializedSource({
            stream: "conditional-materialized",
          }),
    },
  },
});
const conditionalLeasedRoutesViewServer = defineViewServerConfig({
  topics: {
    mixedRoutes: {
      schema: SourceRow,
      source: useRegionRoute
        ? sourceAdapter.leasedSource(["region"], { stream: "conditional-region" })
        : sourceAdapter.leasedSource(["shard"], { stream: "conditional-shard" }),
    },
  },
});

const heterogeneousViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
    positions: {
      schema: Position,
    },
  },
});

const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["id"], { stream: "orders" }),
    },
  },
});

const mixedSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["id"], { stream: "orders" }),
    },
    positions: {
      schema: Order,
    },
  },
});

const mismatchedLeasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["id"], { stream: "orders" }),
    },
    positions: {
      schema: Order,
      source: sourceAdapter.leasedSource(["price"], { stream: "positions" }),
    },
  },
});

const identicalLeasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: sourceAdapter.leasedSource(["id"], { stream: "orders" }),
    },
    positions: {
      schema: Order,
      source: sourceAdapter.leasedSource(["id"], { stream: "positions" }),
    },
  },
});

declare const client: ViewServerLiveClient<typeof viewServer.topics>;
declare const sourceClient: ViewServerLiveClient<typeof sourceViewServer.topics>;
declare const conditionalSourceClient: ViewServerLiveClient<
  typeof conditionalSourceViewServer.topics
>;
declare const conditionalLeasedRoutesClient: ViewServerLiveClient<
  typeof conditionalLeasedRoutesViewServer.topics
>;
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
  it("types exact Materialized and Leased Source Health diagnostics", () => {
    expectTypeOf<
      typeof conditionalSourceViewServer.topics.conditional.source.lifecycle
    >().toEqualTypeOf<"materialized" | "leased">();
    const materialized = sourceClient.subscribeSourceHealth({ topic: "all" });
    const leased = sourceClient.subscribeSourceHealth({
      topic: "routed",
      routeBy: {
        region: "eu",
        shard: 7n,
      },
    });
    const conditionalMaterialized = conditionalSourceClient.subscribeSourceHealth({
      topic: "conditional",
    });
    const conditionalLeased = conditionalSourceClient.subscribeSourceHealth({
      topic: "conditional",
      routeBy: {
        region: "eu",
        shard: 7n,
      },
    });
    type MaterializedResult = Stream.Success<Effect.Success<typeof materialized>["events"]>;
    type LeasedResult = Stream.Success<Effect.Success<typeof leased>["events"]>;
    type MaterializedDegraded = Extract<
      MaterializedResult["status"],
      { readonly _tag: "Degraded" }
    >;
    type MaterializedRejection = Extract<
      MaterializedDegraded["reasons"][number],
      { readonly _tag: "SourceItemRejection" }
    >["latestRejection"];
    type MaterializedAdapterFailure = Extract<
      MaterializedRejection["failure"],
      { readonly _tag: "AdapterFailure" }
    >;
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
      MaterializedResult["status"],
      { readonly _tag: "Exhausted" }
    >;
    type MaterializedInvalidSettlement = Extract<
      Extract<
        MaterializedExhausted["exhaustion"]["lastTermination"],
        { readonly _tag: "Failed" }
      >["failure"],
      { readonly _tag: "RuntimeFailure" }
    >["failure"];
    type LeasedActive = Extract<LeasedResult, { readonly _tag: "Active" }>;
    type LeasedDegraded = Extract<LeasedActive["health"]["status"], { readonly _tag: "Degraded" }>;
    type LeasedRejection = Extract<
      LeasedDegraded["reasons"][number],
      { readonly _tag: "SourceItemRejection" }
    >["latestRejection"];
    type LeasedAdapterFailure = Extract<
      LeasedRejection["failure"],
      { readonly _tag: "AdapterFailure" }
    >;
    expectTypeOf<MaterializedResult["metrics"]["adapter"]["observed"]>().toEqualTypeOf<bigint>();
    expectTypeOf<
      MaterializedAdapterFailure["failure"]["_tag"]
    >().toEqualTypeOf<"ClientTypeSourceFailure">();
    expectTypeOf<MaterializedAdapterFailure["failure"]["message"]>().toEqualTypeOf<string>();
    expectTypeOf<MaterializedRejection["location"]>().toEqualTypeOf<{
      readonly offset: bigint;
    }>();
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
    expectTypeOf<LeasedResult["_tag"]>().toEqualTypeOf<"Inactive" | "Active">();
    expectTypeOf<LeasedResult["route"]>().toEqualTypeOf<{
      readonly region: string;
      readonly shard: bigint;
    }>();
    expectTypeOf<
      Stream.Success<Effect.Success<typeof conditionalMaterialized>["events"]>
    >().toEqualTypeOf<Stream.Success<Effect.Success<typeof conditionalLeased>["events"]>>();
    type ConditionalResult = Stream.Success<
      Effect.Success<typeof conditionalMaterialized>["events"]
    >;
    type DirectConditionalResult = ViewServerSourceHealthResultForTopic<
      typeof conditionalSourceViewServer.topics,
      "conditional"
    >;
    expectTypeOf<ConditionalResult>().toEqualTypeOf<DirectConditionalResult>();
    expectTypeOf<
      Extract<ConditionalResult, { readonly adapter: unknown }>["target"]["_tag"]
    >().toEqualTypeOf<"Materialized">();
    expectTypeOf<
      Extract<ConditionalResult, { readonly _tag: "Active" }>["health"]["target"]["_tag"]
    >().toEqualTypeOf<"Leased">();
    expectTypeOf<
      LeasedAdapterFailure["failure"]["_tag"]
    >().toEqualTypeOf<"ClientTypeSourceFailure">();
    expectTypeOf<LeasedAdapterFailure["failure"]["message"]>().toEqualTypeOf<string>();
    expectTypeOf<LeasedRejection["location"]>().toEqualTypeOf<{
      readonly offset: bigint;
    }>();

    // @ts-expect-error Source-free topics do not expose Source Health.
    const invalidSourceFree = sourceClient.subscribeSourceHealth({ topic: "manual" });
    // @ts-expect-error Leased diagnostics require one exact route.
    const invalidMissingRoute = sourceClient.subscribeSourceHealth({ topic: "routed" });
    const invalidPartialRoute = sourceClient.subscribeSourceHealth({
      topic: "routed",
      // @ts-expect-error Leased diagnostics require every configured route field.
      routeBy: { region: "eu" },
    });
    const invalidMaterializedRoute = sourceClient.subscribeSourceHealth({
      // @ts-expect-error Materialized diagnostics do not accept a route.
      topic: "all",
      // @ts-expect-error Materialized diagnostics do not accept a route.
      routeBy: { region: "eu", shard: 7n },
    });
    const invalidExtraRoute = sourceClient.subscribeSourceHealth({
      topic: "routed",
      routeBy: {
        region: "eu",
        shard: 7n,
        // @ts-expect-error Leased routes reject extra fields.
        extra: true,
      },
    });
    const invalidRouteType = sourceClient.subscribeSourceHealth({
      topic: "routed",
      // @ts-expect-error Leased routes preserve configured scalar types.
      routeBy: { region: "eu", shard: 7 },
    });
    const invalidExtraInput = sourceClient.subscribeSourceHealth({
      topic: "routed",
      routeBy: { region: "eu", shard: 7n },
      // @ts-expect-error Source Health input rejects extra top-level fields.
      extra: true,
    });
    const invalidConditionalPartialRoute = conditionalSourceClient.subscribeSourceHealth({
      topic: "conditional",
      // @ts-expect-error Conditional Leased diagnostics still require every route field.
      routeBy: { region: "eu" },
    });
    const invalidConditionalExtraRoute = conditionalSourceClient.subscribeSourceHealth({
      topic: "conditional",
      routeBy: {
        region: "eu",
        shard: 7n,
        // @ts-expect-error Conditional Leased diagnostics reject extra route fields.
        extra: true,
      },
    });
    const mixedLifecycleCalls = (mixedLifecycleTopic: "all" | "routed") => {
      // @ts-expect-error a mixed-lifecycle Topic union must be narrowed before diagnostics.
      const invalidMixedMissingRoute = sourceClient.subscribeSourceHealth({
        topic: mixedLifecycleTopic,
      });
      // @ts-expect-error a mixed-lifecycle Topic union cannot select one route contract safely.
      const invalidMixedRoute = sourceClient.subscribeSourceHealth({
        topic: mixedLifecycleTopic,
        routeBy: {
          region: "eu",
          shard: 7n,
        },
      });
      void invalidMixedMissingRoute;
      void invalidMixedRoute;
    };
    void invalidSourceFree;
    void invalidMissingRoute;
    void invalidPartialRoute;
    void invalidMaterializedRoute;
    void invalidExtraRoute;
    void invalidRouteType;
    void invalidExtraInput;
    void invalidConditionalPartialRoute;
    void invalidConditionalExtraRoute;
    void mixedLifecycleCalls;
  });

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

  it("infers each exact route of a conditional leased Source", () => {
    const regionSubscription = conditionalLeasedRoutesClient.subscribe("mixedRoutes", {
      routeBy: { region: "eu" },
      select: ["id"],
    });
    const shardSubscription = conditionalLeasedRoutesClient.subscribe("mixedRoutes", {
      routeBy: { shard: 7n },
      select: ["id"],
    });

    // @ts-expect-error conditional leased routes accept one exact branch, never both.
    const combinedSubscription = conditionalLeasedRoutesClient.subscribe("mixedRoutes", {
      routeBy: { region: "eu", shard: 7n },
      select: ["id"],
    });
    // @ts-expect-error every conditional leased branch requires its exact route.
    const missingRouteSubscription = conditionalLeasedRoutesClient.subscribe("mixedRoutes", {
      select: ["id"],
    });

    expectTypeOf<Effect.Success<typeof regionSubscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{ readonly id: string }>
    >();
    expectTypeOf<Effect.Success<typeof shardSubscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{ readonly id: string }>
    >();
    expectTypeOf(combinedSubscription).not.toBeAny();
    expectTypeOf(missingRouteSubscription).not.toBeAny();
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
    expectTypeOf<SummarySnapshot["rows"][0]>().not.toHaveProperty("maxKafkaLag");
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
    expectTypeOf<DetailSnapshot["rows"][number]>().not.toHaveProperty("kafkaLag");
    expectTypeOf<
      Extract<DetailDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<DetailDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();
  });
});
