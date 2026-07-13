import {
  defineViewServerConfig,
  type ViewServerRuntimeClient,
  viewSchema,
} from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { Effect, HashMap, Option, Schedule, Schema, Stream } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { type ViewServerGrpcHealthLedger } from "../src/grpc-health";
import type { ViewServerRuntimeTopicDefinitions } from "../src/runtime-types";

import { grpcClients, GrpcOrder, grpcTopicSources } from "./grpc-config";

import type { GrpcOrderValueMessage } from "./grpc-config";

import { makeGrpcHealth } from "./grpc-materialized";

export const leasedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      grpcSource: grpcSourceMarkers.leased({
        routeBy: ["region"],
      }),
    },
  },
});

export const PublicKeyGrpcOrder = Schema.Struct({
  id: Schema.String.pipe(Schema.check(Schema.isPattern(/^public-/))),
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const RouteEncodingOrder = Schema.Struct({
  id: Schema.String,
  text: Schema.String,
  amount: Schema.BigDecimal,
  count: Schema.BigInt,
  disabled: Schema.Boolean,
  score: Schema.Number,
  flag: Schema.Boolean,
  none: Schema.Null,
  plainScore: Schema.Number,
  tags: Schema.Array(Schema.String),
  meta: Schema.Struct({
    desk: Schema.String,
  }),
  weird: Schema.Unknown,
});

export class SemanticRouteClass extends Schema.Class<SemanticRouteClass>("SemanticRouteClass")({
  value: Schema.String,
}) {}
viewSchema.admitClass(SemanticRouteClass);

export const grpcLeasedViewServer = (input: {
  readonly streamForRegion: (
    region: string,
  ) => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
  readonly acquired?: (region: string) => void;
  readonly release?: Effect.Effect<void>;
}) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: ({ route }) => {
          input.acquired?.(route.region);
          return input.streamForRegion(route.region);
        },
        release: () => input.release ?? Effect.void,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  });

export const grpcLeasedViewServerFromCallbacks = (input: {
  readonly request?: (route: { readonly region: string }) => {
    readonly orderId?: string;
  };
  readonly acquire: (input: {
    readonly route: { readonly region: string };
  }) => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
  readonly release?: () => Effect.Effect<void, unknown, never>;
  readonly map: (input: {
    readonly value: GrpcOrderValueMessage;
    readonly route: { readonly region: string };
  }) => typeof GrpcOrder.Type;
}) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: input.request ?? (({ region }) => ({ orderId: region })),
        acquire: input.acquire,
        ...(input.release === undefined ? {} : { release: input.release }),
        map: input.map,
      }),
    },
  });

export const grpcPublicKeyLeasedViewServer = (input: {
  readonly streamForRegion: (
    region: string,
  ) => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
}) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: PublicKeyGrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: ({ route }) => input.streamForRegion(route.region),
        map: ({ value, route }) => ({
          id: `public-${route.region}-${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  });

export const grpcKeyLeasedViewServer = (input: {
  readonly streamForId: (id: string) => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
}) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: ["id"],
        request: ({ id }) => ({ orderId: id }),
        acquire: ({ route }) => input.streamForId(route.id),
        map: ({ value, route }) => ({
          id: route.id,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "key-route",
          updatedAt: value.updatedAt,
        }),
      }),
    },
  });

export const routeEncodingValues = {
  amount: BigDecimal.fromStringUnsafe("123.45"),
  count: 9007199254740993n,
  disabled: false,
  flag: true,
  meta: {
    desk: "equities",
  },
  none: null,
  plainScore: 42,
  score: -0,
  tags: ["fast", "shared"],
  text: "route",
  weird: {
    alpha: "first",
    stable: "route",
  },
};

export const grpcRouteEncodingLeasedViewServer = () =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: RouteEncodingOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: [
          "amount",
          "count",
          "disabled",
          "flag",
          "meta",
          "none",
          "plainScore",
          "score",
          "tags",
          "text",
          "weird",
        ],
        request: (route) => ({ orderId: String(route.text) }),
        acquire: () => Stream.never,
        map: () => ({
          id: "route-encoding",
          ...routeEncodingValues,
        }),
      }),
    },
  });

export const SemanticRouteOrder = Schema.Struct({
  id: Schema.String,
  routeClass: SemanticRouteClass,
  routeOption: viewSchema.Option(Schema.String),
  routeHashMap: viewSchema.HashMap(Schema.String, Schema.String),
});

export const grpcSemanticRouteLeasedViewServer = () =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: SemanticRouteOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: ["routeClass", "routeOption", "routeHashMap"],
        request: () => ({ orderId: "semantic-route" }),
        acquire: () => Stream.never,
        map: () => ({
          id: "semantic-route",
          routeClass: SemanticRouteClass.make({ value: "route" }),
          routeOption: Option.none(),
          routeHashMap: HashMap.empty(),
        }),
      }),
    },
  });

export const longRunningGrpcStream = (
  values: ReadonlyArray<GrpcOrderValueMessage>,
): Stream.Stream<GrpcOrderValueMessage, never, never> =>
  Stream.make(...values).pipe(Stream.concat(Stream.never));

export const makeLeasedGrpcHealth = makeGrpcHealth;

export const captureLeasedGrpcDegradation = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  health: ViewServerGrpcHealthLedger<Topics>,
  messages: Array<string>,
): ViewServerGrpcHealthLedger<Topics> => ({
  ...health,
  feedDegraded: (feedKey, message) =>
    Effect.sync(() => {
      messages.push(message.split("\n", 1)[0] ?? message);
    }).pipe(Effect.andThen(health.feedDegraded(feedKey, message))),
});

export type LeasedOrdersQuery = {
  readonly select: readonly ["id", "customerId", "price", "region"];
  readonly where: {
    readonly region: {
      readonly eq: string;
    };
  };
  readonly orderBy: readonly [
    {
      readonly field: "price";
      readonly direction: "asc";
    },
  ];
  readonly limit: 10;
};

export const leasedOrdersQuery = (region: string): LeasedOrdersQuery => ({
  select: ["id", "customerId", "price", "region"],
  where: {
    region: { eq: region },
  },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 10,
});

export const waitForLeasedGrpcSnapshotRows = Effect.fn(
  "ViewServerRuntime.test.grpc.leased.snapshotRows.wait",
)(function* (
  client: ViewServerRuntimeClient<typeof leasedGrpcViewServer.topics>,
  region: string,
  expectedTotalRows: number,
) {
  return yield* client.snapshot("orders", leasedOrdersQuery(region)).pipe(
    Effect.repeat({
      schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
      until: (snapshot) => snapshot.totalRows === expectedTotalRows,
    }),
  );
});
