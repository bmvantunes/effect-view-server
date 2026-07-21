import { describe, expectTypeOf, it } from "@effect/vitest";
import { Schema, Stream } from "effect";
import {
  defineViewServerConfig,
  type ExactLiveQueryInputForTopic,
  type LiveQueryResult,
  type TopicRouteBy,
} from "./index";
import { grpcSourceMarkers } from "./internal";
import {
  grpcOrdersByRegionStatusTopic,
  grpcTestClients,
  grpcTestTopicSources,
  grpcTradesMaterializedTopic,
} from "../test-harness/grpc";
import type { LiveQueryCall } from "../test-harness/live-query";
import { Order, Position } from "../test-harness/schemas";

describe("gRPC route generic contracts", () => {
  it("types leased topic route metadata", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: { clients: grpcTestClients },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
        trades: grpcTradesMaterializedTopic,
        positions: { schema: Position, key: "id" },
      },
    });

    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "orders">>().toEqualTypeOf<
      "region" | "status"
    >();
    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "trades">>().toEqualTypeOf<never>();

    defineViewServerConfig({
      grpc: { clients: grpcTestClients },
      topics: {
        orders: grpcTestTopicSources.leased({
          schema: Order,
          key: "id",
          client: "orders",
          method: "streamOrders",
          // @ts-expect-error routeBy fields must exist on the Topic Row.
          routeBy: ["strategyId"],
          request: () => ({ orderId: "invalid" }),
          acquire: () => Stream.never,
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        }),
      },
    });

    const UndefinedRoute = Schema.Struct({ id: Schema.String, value: Schema.Undefined });
    // @ts-expect-error a route field needs at least one defined scalar branch.
    defineViewServerConfig({
      topics: {
        undefinedRoute: {
          schema: UndefinedRoute,
          key: "id",
          grpcSource: grpcSourceMarkers.leased({ routeBy: ["value"] }),
        },
      },
    });

    const NestedRoute = Schema.Struct({
      id: Schema.String,
      profile: Schema.Struct({ country: Schema.String }),
    });
    // @ts-expect-error leased route declarations accept top-level fields, not nested paths.
    defineViewServerConfig({
      topics: {
        nestedRoute: {
          schema: NestedRoute,
          key: "id",
          grpcSource: grpcSourceMarkers.leased({ routeBy: ["profile.country"] }),
        },
      },
    });

    grpcTestTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      // @ts-expect-error leased route declarations must not repeat a field.
      routeBy: ["region", "region"],
      request: () => ({ orderId: "duplicate" }),
      acquire: () => Stream.never,
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
      }),
    });

    const StructuredRoute = Schema.Struct({
      id: Schema.String,
      profile: Schema.Struct({ country: Schema.String }),
    });
    // @ts-expect-error internal leased-source markers still reject structured route fields.
    defineViewServerConfig({
      topics: {
        structured: {
          schema: StructuredRoute,
          key: "id",
          grpcSource: grpcSourceMarkers.leased({ routeBy: ["profile"] }),
        },
      },
    });

    const AnyRoute = Schema.Struct({
      id: Schema.String,
      value: Schema.Any,
    });
    // @ts-expect-error Schema.Any does not promise a scalar leased-route domain.
    defineViewServerConfig({
      topics: {
        anyRoute: {
          schema: AnyRoute,
          key: "id",
          grpcSource: grpcSourceMarkers.leased({ routeBy: ["value"] }),
        },
      },
    });

    const MixedRoute = Schema.Struct({
      id: Schema.String,
      value: Schema.Union([Schema.String, Schema.Struct({ code: Schema.String })]),
    });
    // @ts-expect-error every defined branch of a leased route field must be scalar.
    defineViewServerConfig({
      topics: {
        mixedRoute: {
          schema: MixedRoute,
          key: "id",
          grpcSource: grpcSourceMarkers.leased({ routeBy: ["value"] }),
        },
      },
    });

    const OptionalRouteOrder = Schema.Struct({
      id: Schema.String,
      customerId: Schema.String,
      status: Schema.Literals(["open", "closed", "cancelled"]),
      price: Schema.Number,
      region: Schema.optionalKey(Schema.String),
      updatedAt: Schema.Number,
    });
    grpcTestTopicSources.leased({
      schema: OptionalRouteOrder,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region"],
      request: (route) => {
        expectTypeOf(route).toEqualTypeOf<{ readonly region: string }>();
        return { orderId: route.region };
      },
      acquire: () => Stream.never,
      map: ({ value, route }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        updatedAt: value.updatedAt,
      }),
    });
  });

  it("requires an exact routeBy object independently from where", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: { clients: grpcTestClients },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
        trades: grpcTradesMaterializedTopic,
      },
    });

    const assertGrpcRouteQueryTypes = (
      useLiveQuery: LiveQueryCall<typeof grpcViewServer.topics>,
    ) => {
      const valid = useLiveQuery("orders", {
        routeBy: { region: "UsÁ", status: "open" },
        where: [
          {
            type: "OR",
            conditions: [
              { field: "region", type: "equals", filter: "usa" },
              { field: "region", type: "equals", filter: "london" },
            ],
          },
        ],
        select: ["id", "price"],
      });
      expectTypeOf(valid).toEqualTypeOf<
        LiveQueryResult<{ readonly id: string; readonly price: number }>
      >();

      const missingRoute = { select: ["id"] } satisfies {
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased topics require routeBy.
      useLiveQuery("orders", missingRoute);

      const undefinedRoute = {
        routeBy: undefined,
        select: ["id"],
      } satisfies {
        readonly routeBy: undefined;
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased topics reject routeBy explicitly set to undefined.
      useLiveQuery("orders", undefinedRoute);

      const missingField = {
        routeBy: { region: "usa" },
        select: ["id"],
      } satisfies {
        readonly routeBy: { readonly region: "usa" };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error routeBy must contain every configured route field.
      useLiveQuery("orders", missingField);

      const extraField = {
        routeBy: { region: "usa", status: "open", desk: "equities" },
        select: ["id"],
      } satisfies {
        readonly routeBy: {
          readonly region: "usa";
          readonly status: "open";
          readonly desk: "equities";
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error routeBy must contain only configured route fields.
      useLiveQuery("orders", extraField);

      const wrongValue = {
        routeBy: { region: "usa", status: 1 },
        select: ["id"],
      } satisfies {
        readonly routeBy: { readonly region: "usa"; readonly status: 1 };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error routeBy values must match their Topic Row field types.
      useLiveQuery("orders", wrongValue);

      const materializedRoute = {
        routeBy: { region: "usa" },
        select: ["id"],
      } satisfies {
        readonly routeBy: { readonly region: "usa" };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error non-leased topics reject routeBy.
      useLiveQuery("trades", materializedRoute);
    };

    expectTypeOf(assertGrpcRouteQueryTypes).toBeFunction();
  });

  it("validates every leased route independently across query unions", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: { clients: grpcTestClients },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
      },
    });

    type UsaRawQuery = {
      readonly routeBy: { readonly region: "usa"; readonly status: "open" };
      readonly select: readonly ["id"];
    };
    type LondonRawQuery = {
      readonly routeBy: { readonly region: "london"; readonly status: "closed" };
      readonly select: readonly ["price"];
    };
    type UsaGroupedQuery = {
      readonly routeBy: { readonly region: "usa"; readonly status: "open" };
      readonly groupBy: readonly ["region"];
      readonly aggregates: {
        readonly rowCount: { readonly aggFunc: "count" };
      };
    };

    const assertValidUnionQueries = (useLiveQuery: LiveQueryCall<typeof grpcViewServer.topics>) => {
      const acceptRawUnion = (query: UsaRawQuery | LondonRawQuery) => {
        const result = useLiveQuery("orders", query);
        expectTypeOf(result).toEqualTypeOf<
          LiveQueryResult<{ readonly id: string } | { readonly price: number }>
        >();
      };
      const acceptMixedUnion = (query: LondonRawQuery | UsaGroupedQuery) => {
        const result = useLiveQuery("orders", query);
        expectTypeOf(result).toEqualTypeOf<
          LiveQueryResult<
            { readonly price: number } | { readonly region: string; readonly rowCount: bigint }
          >
        >();
      };

      expectTypeOf(acceptRawUnion).toBeFunction();
      expectTypeOf(acceptMixedUnion).toBeFunction();
    };

    expectTypeOf(assertValidUnionQueries).toBeFunction();

    type ValidQuery = UsaRawQuery;
    type WrongValueQuery = {
      readonly routeBy: { readonly region: "usa"; readonly status: 1 };
      readonly select: readonly ["id"];
    };
    type ExtraFieldQuery = {
      readonly routeBy: {
        readonly region: "usa";
        readonly status: "open";
        readonly desk: "equities";
      };
      readonly select: readonly ["id"];
    };
    type MissingFieldQuery = {
      readonly routeBy: { readonly region: "usa" };
      readonly select: readonly ["id"];
    };
    type WrongFieldQuery = {
      readonly routeBy: { readonly region: "usa"; readonly state: "open" };
      readonly select: readonly ["id"];
    };

    type Topics = typeof grpcViewServer.topics;
    expectTypeOf<
      ExactLiveQueryInputForTopic<Topics, "orders", ValidQuery | WrongValueQuery>
    >().toBeNever();
    expectTypeOf<
      ExactLiveQueryInputForTopic<Topics, "orders", ValidQuery | ExtraFieldQuery>
    >().toBeNever();
    expectTypeOf<
      ExactLiveQueryInputForTopic<Topics, "orders", ValidQuery | MissingFieldQuery>
    >().toBeNever();
    expectTypeOf<
      ExactLiveQueryInputForTopic<Topics, "orders", ValidQuery | WrongFieldQuery>
    >().toBeNever();

    const assertInvalidUnionQueries = (useLiveQuery: LiveQueryCall<Topics>) => {
      const rejectWrongValue = (query: ValidQuery | WrongValueQuery) => {
        // @ts-expect-error one invalid route value poisons the whole query union.
        useLiveQuery("orders", query);
      };
      const rejectExtraField = (query: ValidQuery | ExtraFieldQuery) => {
        // @ts-expect-error one extra route field poisons the whole query union.
        useLiveQuery("orders", query);
      };
      const rejectMissingField = (query: ValidQuery | MissingFieldQuery) => {
        // @ts-expect-error one missing route field poisons the whole query union.
        useLiveQuery("orders", query);
      };
      const rejectWrongField = (query: ValidQuery | WrongFieldQuery) => {
        // @ts-expect-error one wrong route field poisons the whole query union.
        useLiveQuery("orders", query);
      };

      expectTypeOf(rejectWrongValue).toBeFunction();
      expectTypeOf(rejectExtraField).toBeFunction();
      expectTypeOf(rejectMissingField).toBeFunction();
      expectTypeOf(rejectWrongField).toBeFunction();
    };

    expectTypeOf(assertInvalidUnionQueries).toBeFunction();
  });
});
