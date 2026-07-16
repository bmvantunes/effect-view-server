import {
  defineViewServerConfig,
  type GrpcRuntimeClients,
  type RuntimeRegions,
  type ViewServerConfig,
  type ViewServerHealth,
  type ViewServerRuntimeClient,
} from "@effect-view-server/config";
import { Effect, Schedule, Stream } from "effect";
import {
  makeDefaultGrpcRuntimeSourceDependencies,
  resolveGrpcRuntimeSourceOptions as resolveViewServerRuntimeOptions,
} from "../src/grpc-runtime-source";
import { type ResolvedViewServerGrpcRuntimeOptions } from "../src/grpc-runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "../src/runtime-types";

import {
  grpcClients,
  grpcClientsWithOrphan,
  GrpcOrder,
  grpcTopicSources,
  grpcTopicSourcesWithOrphan,
} from "./grpc-config";

import type { GrpcOrderValueMessage, GrpcTopics } from "./grpc-config";

export const grpcMaterializedViewServer = (
  stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>,
) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.materialized({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => stream,
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

export const grpcMaterializedViewServerFromCallbacks = (input: {
  readonly request?: () => { readonly orderId?: string };
  readonly acquire: () => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
  readonly release?: () => Effect.Effect<void, unknown, never>;
  readonly map?: (input: { readonly value: GrpcOrderValueMessage }) => typeof GrpcOrder.Type;
}) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.materialized({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        request: input.request ?? (() => ({ orderId: "all" })),
        acquire: input.acquire,
        ...(input.release === undefined ? {} : { release: input.release }),
        map: ({ value }) =>
          input.map?.({ value }) ?? {
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          },
      }),
    },
  });

export const grpcMaterializedViewServerWithRelease = (
  stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>,
  release: Effect.Effect<void>,
) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.materialized({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => stream,
        release: () => release,
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

export const grpcMaterializedViewServerWithRequestFailure = () =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.materialized({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        request: () => {
          throw new Error("request exploded");
        },
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

export const grpcMaterializedViewServerWithAcquireFailure = () =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.materialized({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => {
          throw new Error("acquire exploded");
        },
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

export const grpcMaterializedViewServerWithMappingFailure = (
  stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>,
) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.materialized({
        schema: GrpcOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => stream,
        map: () => {
          throw new Error("mapping exploded");
        },
      }),
    },
  });

export const grpcMaterializedViewServerWithOrphanClient = () =>
  defineViewServerConfig({
    grpc: { clients: grpcClientsWithOrphan },
    topics: {
      orders: grpcTopicSourcesWithOrphan.materialized({
        schema: GrpcOrder,
        key: "id",
        client: "orphan",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
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

export const waitForGrpcSnapshotRows = Effect.fn("ViewServerRuntime.test.grpc.snapshotRows.wait")(
  function* (client: ViewServerRuntimeClient<GrpcTopics>, expectedTotalRows: number) {
    return yield* client
      .snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      })
      .pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (snapshot) => snapshot.totalRows === expectedTotalRows,
        }),
      );
  },
);

export const makeGrpcHealth = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const Clients extends GrpcRuntimeClients,
>(
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients> & {
    readonly sourceConfig: ViewServerConfig<Topics, Regions, Clients>;
  },
) =>
  makeDefaultGrpcRuntimeSourceDependencies<Topics>().makeHealthLedger(
    grpcOptions.sourceConfig,
    grpcOptions,
  );

export const grpcHealthFeed = (health: ViewServerHealth<GrpcTopics>) =>
  health.grpc?.feeds["orders"]?.materialized["orders"];

export const grpcHealthClient = (health: ViewServerHealth<GrpcTopics>) =>
  health.grpc?.clients["orders"];

export const fastGrpcMaterializedReconnect = {
  delay: "10 millis",
  maxReconnects: 3,
} satisfies ResolvedViewServerGrpcRuntimeOptions<GrpcTopics>["materializedReconnect"];

export const resolveGrpcRuntimeOptions = Effect.fn("ViewServerRuntime.test.grpc.options.resolve")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
    const Clients extends GrpcRuntimeClients,
  >(
    config: ViewServerConfig<Topics, Regions, Clients>,
    materializedReconnect: ResolvedViewServerGrpcRuntimeOptions<
      Topics,
      Clients
    >["materializedReconnect"] = fastGrpcMaterializedReconnect,
  ) {
    const options = yield* resolveViewServerRuntimeOptions(config, {
      grpc: {
        materializedReconnect,
      },
    });
    const grpcOptions = yield* Effect.fromNullishOr(options);
    return {
      ...grpcOptions,
      sourceConfig: config,
    };
  },
);

export const resolveLeasedGrpcRuntimeOptions = resolveGrpcRuntimeOptions;
