import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  type DeltaEvent,
  type ExactGroupedQuery,
  type LiveQueryResult,
  type RawQuery,
  type SnapshotEvent,
} from "@effect-view-server/config";
import type { Effect, Stream } from "effect";
import type { BigDecimal } from "effect/BigDecimal";
import { Schema } from "effect";
import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineError,
  ColumnLiveViewEngineConfig,
  ColumnLiveViewEngineEvent,
  ColumnLiveViewEngineHealth,
  ColumnLiveViewSubscription,
  ColumnLiveViewTopicHealth,
  EngineClosedError,
  InvalidRowError,
} from "./index";
import { createColumnLiveViewEngine } from "./index";
import type { TopicStore } from "./topic-store";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Position = Schema.Struct({
  id: Schema.String,
  quantity: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
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

type Topics = typeof viewServer.topics;
type LeasedEngineTopics = {
  readonly orders: Topics["orders"] & {
    readonly grpcSource: {
      readonly lifecycle: "leased";
      readonly routeBy: readonly ["region"];
    };
  };
};
type Engine = ColumnLiveViewEngine<Topics>;
type OrderRow = typeof Order.Type;
type EffectSuccess<Value> =
  Value extends Effect.Effect<infer Success, infer _Error, infer _Services> ? Success : never;
type SubscriptionRow<Value> = Value extends ColumnLiveViewSubscription<infer Row> ? Row : never;
type SnapshotRow<Value> = Value extends SnapshotEvent<infer Row> ? Row : never;
type DeltaRow<Value> = Value extends DeltaEvent<infer Row> ? Row : never;
type StreamEvent<Value> =
  Value extends ColumnLiveViewSubscription<infer _Row> ? Stream.Success<Value["events"]> : never;

declare const engine: Engine;
declare const heterogeneousEngine: ColumnLiveViewEngine<typeof heterogeneousViewServer.topics>;
declare const heterogeneousTopic: "orders" | "positions";
declare const leasedEngine: ColumnLiveViewEngine<LeasedEngineTopics>;
declare const dynamicSingleField: "id" | "price";
declare const optionalNarrowFieldsQuery: {
  readonly select?: readonly ["id"];
};

describe("ColumnLiveViewEngine type contract", () => {
  it("types engine construction success and initialization errors", () => {
    const created = createColumnLiveViewEngine({ topics: viewServer.topics });

    expectTypeOf<Effect.Success<typeof created>>().toEqualTypeOf<Engine>();
    expectTypeOf<Effect.Error<typeof created>>().toEqualTypeOf<InvalidRowError>();
  });

  it("requires explicit selected-row snapshots and subscription events", () => {
    const idSnapshot = engine.snapshot("orders", { select: ["id"] });
    expectTypeOf<EffectSuccess<typeof idSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{ readonly id: string }>
    >();

    const fullSubscription = engine.subscribe("orders", { select: ["id"] });
    expectTypeOf<EffectSuccess<typeof fullSubscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<{ readonly id: string }>
    >();
    expectTypeOf<SubscriptionRow<EffectSuccess<typeof fullSubscription>>>().toEqualTypeOf<{
      readonly id: string;
    }>();
    type FullEvent = ColumnLiveViewEngineEvent<
      SubscriptionRow<EffectSuccess<typeof fullSubscription>>
    >;
    expectTypeOf<SnapshotRow<Extract<FullEvent, { readonly type: "snapshot" }>>>().toEqualTypeOf<{
      readonly id: string;
    }>();
    expectTypeOf<DeltaRow<Extract<FullEvent, { readonly type: "delta" }>>>().toEqualTypeOf<{
      readonly id: string;
    }>();
  });

  it("types selected-row snapshots and subscription events", () => {
    const selectedSnapshot = engine.snapshot("orders", {
      select: ["id", "price"],
    });
    expectTypeOf<EffectSuccess<typeof selectedSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    expectTypeOf<EffectSuccess<typeof selectedSnapshot>["rows"][number]>().toEqualTypeOf<{
      readonly id: string;
      readonly price: number;
    }>();

    const subscription = engine.subscribe("orders", {
      select: ["customerId", "status"],
    });
    expectTypeOf<EffectSuccess<typeof subscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<{
        readonly customerId: string;
        readonly status: "open" | "closed" | "cancelled";
      }>
    >();
    type SelectedRow = SubscriptionRow<EffectSuccess<typeof subscription>>;
    expectTypeOf<SelectedRow>().toEqualTypeOf<{
      readonly customerId: string;
      readonly status: "open" | "closed" | "cancelled";
    }>();
    type SelectedEvent = StreamEvent<EffectSuccess<typeof subscription>>;
    expectTypeOf<SelectedEvent>().toEqualTypeOf<ColumnLiveViewEngineEvent<SelectedRow>>();
    type SelectedSnapshot = Extract<SelectedEvent, { readonly type: "snapshot" }>;
    type SelectedDeltaOperation = Extract<
      Extract<SelectedEvent, { readonly type: "delta" }>["operations"][number],
      { readonly type: "insert" | "update" }
    >;
    expectTypeOf<SelectedSnapshot["rows"][number]>().toEqualTypeOf<{
      readonly customerId: string;
      readonly status: "open" | "closed" | "cancelled";
    }>();
    expectTypeOf<SelectedDeltaOperation["row"]>().toEqualTypeOf<{
      readonly customerId: string;
      readonly status: "open" | "closed" | "cancelled";
    }>();

    const health = engine.health();
    expectTypeOf<EffectSuccess<typeof health>>().toEqualTypeOf<
      ColumnLiveViewEngineHealth<Topics>
    >();
    expectTypeOf<
      EffectSuccess<typeof health>["topics"]["orders"]
    >().toEqualTypeOf<ColumnLiveViewTopicHealth>();
    type HealthTopics = EffectSuccess<typeof health>["topics"];
    // @ts-expect-error health topics preserve configured topic keys.
    expectTypeOf<HealthTopics["missing"]>().toEqualTypeOf<ColumnLiveViewTopicHealth>();
    // @ts-expect-error the map key is the topic identity; values do not duplicate it.
    expectTypeOf<HealthTopics["orders"]["topic"]>().toEqualTypeOf<"orders">();
  });

  it("requires dynamic topic-union queries to be valid for every possible topic", () => {
    const commonSnapshot = heterogeneousEngine.snapshot(heterogeneousTopic, {
      select: ["id"],
      where: [{ field: "id", type: "equals", filter: "row-1" }],
    });
    const commonSubscription = heterogeneousEngine.subscribe(heterogeneousTopic, {
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
    const invalidSnapshot = heterogeneousEngine.snapshot(
      heterogeneousTopic,
      // @ts-expect-error dynamic topic-union filters must exist on every possible topic.
      topicSpecificFilter,
    );
    const invalidSubscription = heterogeneousEngine.subscribe(
      heterogeneousTopic,
      // @ts-expect-error dynamic topic-union filters must exist on every possible topic.
      topicSpecificFilter,
    );

    expectTypeOf<EffectSuccess<typeof commonSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{ readonly id: string }>
    >();
    expectTypeOf<EffectSuccess<typeof commonSubscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<{ readonly id: string }>
    >();
    expectTypeOf(invalidSnapshot).not.toBeAny();
    expectTypeOf(invalidSubscription).not.toBeAny();
  });

  it("keeps source routing policy outside engine queries", () => {
    const routeFreeSnapshot = leasedEngine.snapshot("orders", {
      select: ["id"],
      where: [{ field: "region", type: "equals", filter: "usa" }],
    });
    const routedQuery = {
      routeBy: { region: "usa" },
      select: ["id"],
    } satisfies {
      readonly routeBy: { readonly region: "usa" };
      readonly select: readonly ["id"];
    };
    const invalidRoutedSnapshot = leasedEngine.snapshot(
      "orders",
      // @ts-expect-error the engine query seam is source-agnostic and does not accept routeBy.
      routedQuery,
    );

    expectTypeOf<EffectSuccess<typeof routeFreeSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{ readonly id: string }>
    >();
    expectTypeOf(invalidRoutedSnapshot).not.toBeAny();
  });

  it("types valid mutation calls", () => {
    const validPublish = engine.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });
    const validPatch = engine.patch("orders", "order-1", {
      price: 42,
      status: "closed",
    });
    const validReset = engine.reset();
    const validClose = engine.close();
    const validHealth = engine.health();

    expectTypeOf<Effect.Error<typeof validPublish>>().toEqualTypeOf<ColumnLiveViewEngineError>();
    expectTypeOf<EffectSuccess<typeof validPatch>>().toEqualTypeOf<void>();
    expectTypeOf<Effect.Error<typeof validPatch>>().toEqualTypeOf<ColumnLiveViewEngineError>();
    expectTypeOf<Effect.Error<typeof validReset>>().toEqualTypeOf<EngineClosedError>();
    expectTypeOf<Effect.Error<typeof validClose>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Error<typeof validHealth>>().toEqualTypeOf<never>();
  });

  it("rejects invalid raw query select and topics", () => {
    // @ts-expect-error invalid selected field is rejected.
    const _invalidSelectedField = engine.snapshot("orders", { select: ["missing"] });
    // @ts-expect-error invalid selected field is rejected for subscriptions.
    const _invalidSubscribeSelectedField = engine.subscribe("orders", { select: ["missing"] });

    const invalidWhereFieldQuery = {
      select: ["id"],
      where: [{ field: "missing", type: "equals", filter: "value" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        { readonly field: "missing"; readonly type: "equals"; readonly filter: "value" },
      ];
    };
    // @ts-expect-error invalid where field is rejected.
    const _invalidWhereField = engine.snapshot("orders", invalidWhereFieldQuery);

    const invalidOrderFieldQuery = {
      select: ["id"],
      orderBy: [{ field: "missing", direction: "asc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "missing"; readonly direction: "asc" }];
    };
    // @ts-expect-error invalid order field is rejected.
    const _invalidOrderField = engine.snapshot("orders", invalidOrderFieldQuery);

    // @ts-expect-error invalid topic is rejected.
    const _invalidTopic = engine.snapshot("missing", { select: ["id"] });
    // @ts-expect-error invalid subscription topic is rejected.
    const _invalidSubscribeTopic = engine.subscribe("missing", { select: ["id"] });

    const extraRawQuery = {
      select: ["id"],
      typo: true,
    } satisfies {
      readonly select: readonly ["id"];
      readonly typo: true;
    };
    // @ts-expect-error extra raw query keys are rejected through variables.
    const _invalidExtraRawQueryKey = engine.snapshot("orders", extraRawQuery);

    const extraWhereField = {
      select: ["id"],
      where: [
        { field: "status", type: "equals", filter: "open" },
        { field: "missing", type: "equals", filter: "x" },
      ],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        { readonly field: "status"; readonly type: "equals"; readonly filter: "open" },
        { readonly field: "missing"; readonly type: "equals"; readonly filter: "x" },
      ];
    };
    // @ts-expect-error extra where fields are rejected through variables.
    const _invalidExtraWhereField = engine.snapshot("orders", extraWhereField);

    const extraFilterOperator = {
      select: ["id"],
      where: [
        {
          field: "status",
          type: "equals",
          filter: {
            eq: "open",
            typo: true,
          },
        },
      ],
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: readonly [
        {
          readonly field: "status";
          readonly type: "equals";
          readonly filter: {
            readonly eq: "open";
            readonly typo: true;
          };
        },
      ];
    };
    // @ts-expect-error extra filter operator keys are rejected through variables.
    const _invalidExtraFilterOperator = engine.snapshot("orders", extraFilterOperator);

    const extraOrderByEntry = {
      select: ["id"],
      orderBy: [
        {
          field: "id",
          direction: "asc",
          typo: true,
        },
      ],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [
        {
          readonly field: "id";
          readonly direction: "asc";
          readonly typo: true;
        },
      ];
    };
    // @ts-expect-error extra orderBy entry keys are rejected through variables.
    const _invalidExtraOrderByEntry = engine.snapshot("orders", extraOrderByEntry);

    const dynamicSingleTupleSelectedFieldsQuery = {
      select: [dynamicSingleField],
    } satisfies {
      readonly select: readonly [typeof dynamicSingleField];
    };
    const _dynamicSingleTupleSelectedFieldsSnapshot = engine.snapshot(
      "orders",
      dynamicSingleTupleSelectedFieldsQuery,
    );
    expectTypeOf<
      EffectSuccess<typeof _dynamicSingleTupleSelectedFieldsSnapshot>["rows"][number]
    >().toEqualTypeOf<Partial<{ readonly id: string; readonly price: number }>>();

    const broadSelectedFields: ReadonlyArray<"id" | "price"> = ["id", "price"];
    const broadSelectedFieldsQuery = {
      select: broadSelectedFields,
    };
    const _invalidBroadSelectedFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error broad selected field arrays are rejected because result rows must be exact.
      broadSelectedFieldsQuery,
    );

    // @ts-expect-error raw queries must explicitly select projected fields.
    const broadRawQueryWithoutFields: RawQuery<OrderRow> = {
      where: [{ field: "status", type: "equals", filter: "open" }],
    };
    const _invalidOptionalNarrowFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error optional selected fields are rejected because omitted and present cases project different row shapes.
      optionalNarrowFieldsQuery,
    );
    const _invalidOptionalNarrowFieldsSubscription = engine.subscribe(
      "orders",
      // @ts-expect-error optional selected fields are rejected because omitted and present cases project different row shapes.
      optionalNarrowFieldsQuery,
    );

    void _invalidSelectedField;
    void _invalidSubscribeSelectedField;
    void _invalidWhereField;
    void _invalidOrderField;
    void _invalidTopic;
    void _invalidSubscribeTopic;
    void _invalidExtraRawQueryKey;
    void _invalidExtraWhereField;
    void _invalidExtraFilterOperator;
    void _invalidExtraOrderByEntry;
    void _dynamicSingleTupleSelectedFieldsSnapshot;
    void broadRawQueryWithoutFields;
    void _invalidOptionalNarrowFieldsSnapshot;
    void _invalidOptionalNarrowFieldsSubscription;
  });

  it("rejects invalid patch shapes", () => {
    // @ts-expect-error patch row field types are enforced.
    const _invalidPatchType = engine.patch("orders", "order-1", { price: "42" });

    // @ts-expect-error patch row fields are enforced.
    const _invalidPatchField = engine.patch("orders", "order-1", { missing: true });

    const invalidPatchVariable = {
      price: 42,
      missing: true,
    };
    // @ts-expect-error patch row fields are enforced through variables.
    const _invalidPatchVariable = engine.patch("orders", "order-1", invalidPatchVariable);

    void _invalidPatchType;
    void _invalidPatchField;
    void _invalidPatchVariable;
  });

  it("rejects invalid engine topic keys", () => {
    const _invalidKeyConfig: ColumnLiveViewEngineConfig<{
      readonly orders: {
        readonly schema: typeof Order;
        readonly key: "missing";
      };
    }> = {
      topics: {
        orders: {
          schema: Order,
          // @ts-expect-error engine topic keys must be string select in the schema.
          key: "missing",
        },
      },
    };

    void _invalidKeyConfig;
  });

  it("rejects invalid grouped incremental admission limit options", () => {
    const _invalidGroupedAdmissionLimitKey: ColumnLiveViewEngineConfig<Topics> = {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limit keys are exact.
        maxGroupz: 1,
      },
      topics: viewServer.topics,
    };
    const _invalidGroupedAdmissionLimitValue: ColumnLiveViewEngineConfig<Topics> = {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits must be numeric.
        maxGroups: "1",
      },
      topics: viewServer.topics,
    };

    void _invalidGroupedAdmissionLimitKey;
    void _invalidGroupedAdmissionLimitValue;
  });

  it("types grouped aggregate snapshots and subscriptions", () => {
    const groupedSnapshot = engine.snapshot("orders", {
      groupBy: ["status"],
      aggregates: {
        rowCount: { aggFunc: "count" },
        totalPrice: { aggFunc: "sum", field: "price" },
        averagePrice: { aggFunc: "avg", field: "price" },
        distinctCustomers: { aggFunc: "countDistinct", field: "customerId" },
        minRegion: { aggFunc: "min", field: "region" },
      },
      orderBy: [{ aggregate: "rowCount", direction: "desc" }],
    });
    expectTypeOf<EffectSuccess<typeof groupedSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{
        readonly status: "open" | "closed" | "cancelled";
        readonly rowCount: bigint;
        readonly totalPrice: BigDecimal;
        readonly averagePrice: BigDecimal;
        readonly distinctCustomers: bigint;
        readonly minRegion: string;
      }>
    >();

    const groupedSubscription = engine.subscribe("orders", {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    expectTypeOf<EffectSuccess<typeof groupedSubscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<{
        readonly status: "open" | "closed" | "cancelled";
        readonly rowCount: bigint;
      }>
    >();

    const invalidGroupedOrderByAggregateQuery = {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      orderBy: [{ aggregate: "missing", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
      readonly orderBy: readonly [{ readonly aggregate: "missing"; readonly direction: "desc" }];
    };
    // @ts-expect-error grouped orderBy aggregate must reference an aggregate alias.
    const _invalidGroupedOrderByAggregate: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByAggregateQuery
    > = invalidGroupedOrderByAggregateQuery;

    const invalidGroupedFieldQuery = {
      groupBy: ["missing"],
      aggregates: { rowCount: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["missing"];
      readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
    };
    // @ts-expect-error groupBy fields must exist on the topic row.
    const _invalidGroupedField: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedFieldQuery
    > = invalidGroupedFieldQuery;
  });

  it("keeps TopicStore nominal inside engine internals", () => {
    const fakeTopicStore = { topic: "orders" };
    // @ts-expect-error TopicStore cannot be structurally faked with only the public topic field.
    const topicStore: TopicStore = fakeTopicStore;
    expectTypeOf(topicStore).toEqualTypeOf<TopicStore>();
  });
});
