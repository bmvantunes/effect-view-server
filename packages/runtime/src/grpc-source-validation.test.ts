import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Schema, Stream } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import { ViewServerGrpcIngressError } from "./grpc-ingress";
import {
  makeDefaultGrpcRuntimeSourceDependencies,
  resolveGrpcRuntimeSourceOptions as resolveViewServerRuntimeOptions,
} from "./grpc-runtime-source";
import {
  resolveViewServerGrpcRuntimeOptions,
  validateGrpcSourceFeeds,
} from "./grpc-runtime-options";
import {
  grpcAndKafkaViewServer,
  grpcClients,
  GrpcOrder,
  grpcTopicSources,
  grpcViewServer,
} from "../test-harness/grpc-config";
import {
  grpcMaterializedViewServer,
  resolveGrpcRuntimeOptions,
  resolveLeasedGrpcRuntimeOptions,
} from "../test-harness/grpc-materialized";
import { grpcLeasedViewServer, leasedGrpcViewServer } from "../test-harness/grpc-leased";

const cloneWithMutableTopics = <
  const Config extends {
    readonly topics: object;
  },
>(
  config: Config,
) => ({
  ...config,
  topics: { ...config.topics },
});

const cloneWithMutableOrdersGrpcSource = <
  const Config extends {
    readonly topics: {
      readonly orders: {
        readonly grpcSource: object;
      };
    };
  },
>(
  config: Config,
) => ({
  ...config,
  topics: {
    ...config.topics,
    orders: {
      ...config.topics.orders,
      grpcSource: { ...config.topics.orders.grpcSource },
    },
  },
});

describe("Runtime source composition and options", () => {
  it.live("rejects topic-owned gRPC feeds without config-owned clients", () =>
    Effect.gen(function* () {
      const config = { ...grpcMaterializedViewServer(Stream.never) };
      Reflect.deleteProperty(config, "grpc");
      const error = yield* resolveViewServerRuntimeOptions(config).pipe(Effect.flip);
      const grpcError = yield* Schema.decodeUnknownEffect(ViewServerGrpcIngressError)(error);

      expect({
        _tag: grpcError._tag,
        cause: grpcError.cause,
        message: grpcError.message,
        phase: grpcError.phase,
      }).toStrictEqual({
        _tag: "ViewServerGrpcIngressError",
        cause: "missing-grpc-clients",
        message:
          "gRPC feeds are configured, but no gRPC clients were provided on config.grpc.clients.",
        phase: "configuration",
      });
    }),
  );

  it.live("rejects malformed topic-owned gRPC bindings during runtime option derivation", () =>
    Effect.gen(function* () {
      const invalidLifecycleViewServer = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          grpc: {
            clients: grpcClients,
          },
          topics: {
            orders: grpcTopicSources.materialized({
              schema: GrpcOrder,
              key: "id",
              client: "orders",
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
        }),
      );
      Object.defineProperty(invalidLifecycleViewServer.topics.orders.grpcSource, "lifecycle", {
        value: "invalid-lifecycle",
      });
      const invalidRouteByViewServer = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          grpc: {
            clients: grpcClients,
          },
          topics: {
            orders: grpcTopicSources.leased({
              schema: GrpcOrder,
              key: "id",
              client: "orders",
              method: "streamOrders",
              routeBy: ["region"],
              request: ({ region }) => ({ orderId: region }),
              acquire: () => Stream.never,
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
        }),
      );
      Object.defineProperty(invalidRouteByViewServer.topics.orders.grpcSource, "routeBy", {
        value: [],
      });
      const partialConcreteBindingViewServer = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          grpc: {
            clients: grpcClients,
          },
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(partialConcreteBindingViewServer.topics.orders.grpcSource, "client", {
        enumerable: true,
        value: "orders",
      });
      Object.defineProperty(partialConcreteBindingViewServer.topics.orders.grpcSource, "method", {
        enumerable: true,
        value: "streamOrders",
      });

      const invalidLifecycleError = yield* resolveViewServerGrpcRuntimeOptions(
        invalidLifecycleViewServer,
        {},
      ).pipe(Effect.flip);
      const invalidRouteByError = yield* resolveViewServerGrpcRuntimeOptions(
        invalidRouteByViewServer,
        {},
      ).pipe(Effect.flip);
      const partialConcreteBindingError = yield* resolveViewServerGrpcRuntimeOptions(
        partialConcreteBindingViewServer,
        {},
      ).pipe(Effect.flip);
      const invalidLifecycleGrpcError = yield* Schema.decodeUnknownEffect(
        ViewServerGrpcIngressError,
      )(invalidLifecycleError);
      const invalidRouteByGrpcError = yield* Schema.decodeUnknownEffect(ViewServerGrpcIngressError)(
        invalidRouteByError,
      );
      const partialConcreteBindingGrpcError = yield* Schema.decodeUnknownEffect(
        ViewServerGrpcIngressError,
      )(partialConcreteBindingError);

      expect({
        invalidLifecycle: {
          feedName: invalidLifecycleGrpcError.feedName,
          message: invalidLifecycleGrpcError.message,
          phase: invalidLifecycleGrpcError.phase,
          topic: invalidLifecycleGrpcError.topic,
        },
        invalidRouteBy: {
          feedName: invalidRouteByGrpcError.feedName,
          message: invalidRouteByGrpcError.message,
          phase: invalidRouteByGrpcError.phase,
          topic: invalidRouteByGrpcError.topic,
        },
        partialConcreteBinding: {
          feedName: partialConcreteBindingGrpcError.feedName,
          message: partialConcreteBindingGrpcError.message,
          phase: partialConcreteBindingGrpcError.phase,
          topic: partialConcreteBindingGrpcError.topic,
        },
      }).toStrictEqual({
        invalidLifecycle: {
          feedName: "orders",
          message: "View Server topic orders declares invalid gRPC source metadata.",
          phase: "configuration",
          topic: "orders",
        },
        invalidRouteBy: {
          feedName: "orders",
          message: "View Server topic orders declares invalid gRPC source metadata.",
          phase: "configuration",
          topic: "orders",
        },
        partialConcreteBinding: {
          feedName: "orders",
          message: "View Server topic orders declares invalid gRPC source metadata.",
          phase: "configuration",
          topic: "orders",
        },
      });
    }),
  );

  it.live("does not register gRPC health feeds for unknown topics", () =>
    Effect.gen(function* () {
      const config = grpcMaterializedViewServer(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(config);
      const ordersSourceFeed = yield* Effect.fromNullishOr(grpcOptions.feeds["orders"]);
      Object.defineProperty(ordersSourceFeed, "topic", {
        value: "missing",
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, {});
      const health = makeDefaultGrpcRuntimeSourceDependencies<
        typeof config.topics
      >().makeHealthLedger(config, grpcOptions);
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(Object.entries(currentHealth.grpc?.feeds ?? {})).toStrictEqual([]);
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects resolved gRPC feeds that reference non-server-streaming methods", () =>
    Effect.gen(function* () {
      const config = cloneWithMutableOrdersGrpcSource(grpcMaterializedViewServer(Stream.never));
      Object.defineProperty(config.topics.orders.grpcSource, "method", {
        enumerable: true,
        value: "getOrder",
      });

      const error = yield* resolveViewServerRuntimeOptions(config).pipe(Effect.flip);

      expect(error).toStrictEqual(
        new ViewServerGrpcIngressError({
          message:
            "gRPC feed orders references non-server-streaming method getOrder on client orders",
          cause: "getOrder",
          feedName: "orders",
          topic: "orders",
          phase: "configuration",
        }),
      );
    }),
  );

  it.live("rejects runtime startup when a declared leased gRPC source has no feed", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof leasedGrpcViewServer.topics>;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof leasedGrpcViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };

      const error = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        leasedGrpcViewServer,
      ).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "View Server topic orders declares gRPC leased source but no matching gRPC feed was configured.",
      );
    }),
  );

  it.live("validates topic-owned grpcSource feeds", () =>
    Effect.gen(function* () {
      const config = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const options = yield* resolveViewServerRuntimeOptions(config);
      yield* validateGrpcSourceFeeds(config, yield* Effect.fromNullishOr(options));

      expect({
        feedTopic: options?.feeds["orders"]?.topic,
        routeBy: options?.feeds["orders"]?.routeBy,
      }).toStrictEqual({
        feedTopic: "orders",
        routeBy: ["region"],
      });
    }),
  );

  it.live("rejects gRPC feeds that target unknown or non-gRPC View Server topics", () =>
    Effect.gen(function* () {
      const unknownTopicConfig = grpcMaterializedViewServer(Stream.never);
      const nonGrpcTopicConfig = grpcMaterializedViewServer(Stream.never);
      const unknownTopicOptions = yield* resolveGrpcRuntimeOptions(unknownTopicConfig);
      const nonGrpcTopicOptions = yield* resolveGrpcRuntimeOptions(nonGrpcTopicConfig);
      const unknownTopicFeed = yield* Effect.fromNullishOr(unknownTopicOptions.feeds["orders"]);
      const nonGrpcTopicFeed = yield* Effect.fromNullishOr(nonGrpcTopicOptions.feeds["orders"]);
      unknownTopicOptions.feeds["unknown"] = {
        ...unknownTopicFeed,
        topic: "unknown",
      };
      nonGrpcTopicOptions.feeds["audit"] = {
        ...nonGrpcTopicFeed,
        topic: "audit",
      };
      const unknownTopicError = yield* validateGrpcSourceFeeds(
        grpcAndKafkaViewServer,
        unknownTopicOptions,
      ).pipe(Effect.flip);
      const nonGrpcTopicError = yield* validateGrpcSourceFeeds(
        grpcAndKafkaViewServer,
        nonGrpcTopicOptions,
      ).pipe(Effect.flip);

      expect({
        unknownTopicMessage: unknownTopicError.message,
        unknownTopicFeedName: Reflect.get(unknownTopicError, "feedName"),
        nonGrpcTopicMessage: nonGrpcTopicError.message,
        nonGrpcTopicFeedName: Reflect.get(nonGrpcTopicError, "feedName"),
      }).toStrictEqual({
        unknownTopicMessage: "gRPC feed unknown references unknown View Server topic unknown.",
        unknownTopicFeedName: "unknown",
        nonGrpcTopicMessage:
          "gRPC feed audit targets View Server topic audit, but that topic does not declare a gRPC source.",
        nonGrpcTopicFeedName: "audit",
      });
    }),
  );

  it.live("rejects gRPC feed lifecycle mismatches from the feed validation boundary", () =>
    Effect.gen(function* () {
      const materializedConfig = grpcMaterializedViewServer(Stream.never);
      const leasedConfig = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const materializedTopicLeasedFeedOptions =
        yield* resolveGrpcRuntimeOptions(materializedConfig);
      const leasedTopicMaterializedFeedOptions =
        yield* resolveLeasedGrpcRuntimeOptions(leasedConfig);
      const materializedFeed = yield* Effect.fromNullishOr(
        materializedTopicLeasedFeedOptions.feeds["orders"],
      );
      const leasedFeed = yield* Effect.fromNullishOr(
        leasedTopicMaterializedFeedOptions.feeds["orders"],
      );
      Object.defineProperty(materializedFeed, "lifecycle", { value: "leased" });
      Object.defineProperty(leasedFeed, "lifecycle", { value: "materialized" });
      const materializedTopicLeasedFeedError = yield* validateGrpcSourceFeeds(
        grpcViewServer,
        materializedTopicLeasedFeedOptions,
      ).pipe(Effect.flip);
      const leasedTopicMaterializedFeedError = yield* validateGrpcSourceFeeds(
        leasedGrpcViewServer,
        leasedTopicMaterializedFeedOptions,
      ).pipe(Effect.flip);

      expect({
        materializedTopicMessage: materializedTopicLeasedFeedError.message,
        materializedTopicFeedName: Reflect.get(materializedTopicLeasedFeedError, "feedName"),
        leasedTopicMessage: leasedTopicMaterializedFeedError.message,
        leasedTopicFeedName: Reflect.get(leasedTopicMaterializedFeedError, "feedName"),
      }).toStrictEqual({
        materializedTopicMessage:
          "gRPC feed orders lifecycle leased does not match View Server topic orders source lifecycle materialized.",
        materializedTopicFeedName: "orders",
        leasedTopicMessage:
          "gRPC feed orders lifecycle materialized does not match View Server topic orders source lifecycle leased.",
        leasedTopicFeedName: "orders",
      });
    }),
  );

  it.live("rejects resolved gRPC feeds that reference missing clients or methods", () =>
    Effect.gen(function* () {
      const missingClientConfig = cloneWithMutableOrdersGrpcSource(
        grpcMaterializedViewServer(Stream.never),
      );
      const missingMethodConfig = cloneWithMutableOrdersGrpcSource(
        grpcMaterializedViewServer(Stream.never),
      );
      Object.defineProperty(missingClientConfig.topics.orders.grpcSource, "client", {
        value: "missing",
      });
      Object.defineProperty(missingMethodConfig.topics.orders.grpcSource, "method", {
        value: "missingMethod",
      });

      const missingClientError = yield* resolveViewServerRuntimeOptions(missingClientConfig).pipe(
        Effect.flip,
      );
      const missingMethodError = yield* resolveViewServerRuntimeOptions(missingMethodConfig).pipe(
        Effect.flip,
      );

      expect({
        missingClient: {
          cause: missingClientError.cause,
          feedName: Reflect.get(missingClientError, "feedName"),
          message: missingClientError.message,
          phase: Reflect.get(missingClientError, "phase"),
          topic: Reflect.get(missingClientError, "topic"),
        },
        missingMethod: {
          cause: missingMethodError.cause,
          feedName: Reflect.get(missingMethodError, "feedName"),
          message: missingMethodError.message,
          phase: Reflect.get(missingMethodError, "phase"),
          topic: Reflect.get(missingMethodError, "topic"),
        },
      }).toStrictEqual({
        missingClient: {
          cause: "missing",
          feedName: "orders",
          message: "gRPC feed orders references missing client: missing",
          phase: "configuration",
          topic: "orders",
        },
        missingMethod: {
          cause: "missingMethod",
          feedName: "orders",
          message: "gRPC feed orders references missing method missingMethod on client orders",
          phase: "configuration",
          topic: "orders",
        },
      });
    }),
  );

  it.live("rejects leased gRPC feed routeBy mismatches from the feed validation boundary", () =>
    Effect.gen(function* () {
      const makeLocalViewServer = () =>
        defineViewServerConfig({
          grpc: { clients: grpcClients },
          topics: {
            orders: grpcTopicSources.leased({
              schema: GrpcOrder,
              key: "id",
              client: "orders",
              method: "streamOrders",
              routeBy: ["region", "status"],
              request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
              acquire: () => Stream.never,
              map: ({ value, route }) => ({
                id: `${route.region}:${value.customerId}`,
                customerId: value.customerId,
                status: route.status,
                price: value.price,
                region: route.region,
                updatedAt: value.updatedAt,
              }),
            }),
          },
        });
      const localViewServer = makeLocalViewServer();
      const invalidFeedRouteByViewServer = makeLocalViewServer();
      const invalidSourceRouteByViewServer =
        cloneWithMutableOrdersGrpcSource(makeLocalViewServer());
      const resolvedGrpcOptions = yield* resolveGrpcRuntimeOptions(localViewServer);
      const invalidFeedRouteByOptions = yield* resolveGrpcRuntimeOptions(
        invalidFeedRouteByViewServer,
      );
      const feed = yield* Effect.fromNullishOr(resolvedGrpcOptions.feeds["orders"]);
      const invalidFeedRouteBy = yield* Effect.fromNullishOr(
        invalidFeedRouteByOptions.feeds["orders"],
      );
      Object.defineProperty(feed, "routeBy", { value: ["status", "region"] });
      Object.defineProperty(invalidFeedRouteBy, "routeBy", {
        value: ["region", 1],
      });
      Object.defineProperty(invalidSourceRouteByViewServer.topics.orders.grpcSource, "routeBy", {
        value: ["region", 1],
      });
      const error = yield* validateGrpcSourceFeeds(localViewServer, resolvedGrpcOptions).pipe(
        Effect.flip,
      );
      const invalidFeedRouteByError = yield* validateGrpcSourceFeeds(
        localViewServer,
        invalidFeedRouteByOptions,
      ).pipe(Effect.flip);
      const invalidSourceRouteByError = yield* validateGrpcSourceFeeds(
        invalidSourceRouteByViewServer,
        resolvedGrpcOptions,
      ).pipe(Effect.flip);

      expect({
        message: error.message,
        feedName: Reflect.get(error, "feedName"),
        invalidFeedRouteByMessage: invalidFeedRouteByError.message,
        invalidSourceRouteByMessage: invalidSourceRouteByError.message,
      }).toStrictEqual({
        message:
          "gRPC leased feed orders routeBy status, region does not match View Server topic orders source routeBy region, status.",
        feedName: "orders",
        invalidFeedRouteByMessage:
          "gRPC leased feed orders routeBy  does not match View Server topic orders source routeBy region, status.",
        invalidSourceRouteByMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
      });
    }),
  );

  it.live("rejects gRPC feeds when topic source metadata is malformed", () =>
    Effect.gen(function* () {
      const nullTopicConfig = cloneWithMutableTopics(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(nullTopicConfig.topics, "orders", { value: null });
      const nonGrpcKindConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(nonGrpcKindConfig.topics.orders.grpcSource, "kind", {
        value: "not-grpc",
      });
      const invalidLifecycleConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(invalidLifecycleConfig.topics.orders.grpcSource, "lifecycle", {
        value: "invalid-lifecycle",
      });
      const rawGrpcSourceConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(rawGrpcSourceConfig.topics.orders, "grpcSource", {
        value: {
          kind: "grpc",
          lifecycle: "materialized",
        },
      });
      const mismatchedGrpcSourceTagConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(mismatchedGrpcSourceTagConfig.topics.orders.grpcSource, "_tag", {
        value: "GrpcLeasedTopicSource",
      });
      const mismatchedGrpcLeasedSourceTagConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.leased({
                routeBy: ["region"],
              }),
            },
          },
        }),
      );
      Object.defineProperty(mismatchedGrpcLeasedSourceTagConfig.topics.orders.grpcSource, "_tag", {
        value: "GrpcMaterializedTopicSource",
      });
      const malformedGrpcSourceConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(malformedGrpcSourceConfig.topics.orders, "grpcSource", {
        value: "not-grpc",
      });
      const extraGrpcSourceConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(extraGrpcSourceConfig.topics.orders.grpcSource, "extra", {
        value: true,
      });
      const materializedRouteByGrpcSourceConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.materialized(),
            },
          },
        }),
      );
      Object.defineProperty(
        materializedRouteByGrpcSourceConfig.topics.orders.grpcSource,
        "routeBy",
        {
          value: ["region"],
        },
      );
      const emptyRouteByGrpcSourceConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.leased({
                routeBy: ["region"],
              }),
            },
          },
        }),
      );
      Object.defineProperty(emptyRouteByGrpcSourceConfig.topics.orders.grpcSource, "routeBy", {
        value: [],
      });
      const partialLeasedGrpcSourceConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.leased({
                routeBy: ["region"],
              }),
            },
          },
        }),
      );
      Object.defineProperty(partialLeasedGrpcSourceConfig.topics.orders.grpcSource, "client", {
        enumerable: true,
        value: "orders",
      });
      Object.defineProperty(partialLeasedGrpcSourceConfig.topics.orders.grpcSource, "method", {
        enumerable: true,
        value: "streamOrders",
      });
      const extraLeasedGrpcSourceConfig = cloneWithMutableOrdersGrpcSource(
        defineViewServerConfig({
          topics: {
            orders: {
              schema: GrpcOrder,
              key: "id",
              grpcSource: grpcSourceMarkers.leased({
                routeBy: ["region"],
              }),
            },
          },
        }),
      );
      Object.defineProperty(extraLeasedGrpcSourceConfig.topics.orders.grpcSource, "extra", {
        value: true,
      });
      const feed = grpcMaterializedViewServer(Stream.never);
      const leasedFeed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const leasedGrpcOptions = yield* resolveLeasedGrpcRuntimeOptions(leasedFeed);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const nullTopicError = yield* validateGrpcSourceFeeds(nullTopicConfig, grpcOptions).pipe(
        Effect.flip,
      );
      const nonGrpcKindError = yield* validateGrpcSourceFeeds(nonGrpcKindConfig, grpcOptions).pipe(
        Effect.flip,
      );
      const invalidLifecycleError = yield* validateGrpcSourceFeeds(
        invalidLifecycleConfig,
        grpcOptions,
      ).pipe(Effect.flip);
      const rawGrpcSourceError = yield* validateGrpcSourceFeeds(
        rawGrpcSourceConfig,
        grpcOptions,
      ).pipe(Effect.flip);
      const mismatchedGrpcSourceTagError = yield* validateGrpcSourceFeeds(
        mismatchedGrpcSourceTagConfig,
        grpcOptions,
      ).pipe(Effect.flip);
      const mismatchedGrpcLeasedSourceTagError = yield* validateGrpcSourceFeeds(
        mismatchedGrpcLeasedSourceTagConfig,
        leasedGrpcOptions,
      ).pipe(Effect.flip);
      const malformedGrpcSourceError = yield* validateGrpcSourceFeeds(
        malformedGrpcSourceConfig,
        undefined,
      ).pipe(Effect.flip);
      const extraGrpcSourceError = yield* validateGrpcSourceFeeds(
        extraGrpcSourceConfig,
        undefined,
      ).pipe(Effect.flip);
      const materializedRouteByGrpcSourceError = yield* validateGrpcSourceFeeds(
        materializedRouteByGrpcSourceConfig,
        undefined,
      ).pipe(Effect.flip);
      const emptyRouteByGrpcSourceError = yield* validateGrpcSourceFeeds(
        emptyRouteByGrpcSourceConfig,
        undefined,
      ).pipe(Effect.flip);
      const partialLeasedGrpcSourceError = yield* validateGrpcSourceFeeds(
        partialLeasedGrpcSourceConfig,
        undefined,
      ).pipe(Effect.flip);
      const extraLeasedGrpcSourceError = yield* validateGrpcSourceFeeds(
        extraLeasedGrpcSourceConfig,
        undefined,
      ).pipe(Effect.flip);

      expect({
        nullTopicMessage: nullTopicError.message,
        nullTopicFeedName: Reflect.get(nullTopicError, "feedName"),
        nonGrpcKindMessage: nonGrpcKindError.message,
        nonGrpcKindFeedName: Reflect.get(nonGrpcKindError, "feedName"),
        invalidLifecycleMessage: invalidLifecycleError.message,
        invalidLifecycleFeedName: Reflect.get(invalidLifecycleError, "feedName"),
        rawGrpcSourceMessage: rawGrpcSourceError.message,
        rawGrpcSourceFeedName: Reflect.get(rawGrpcSourceError, "feedName"),
        mismatchedGrpcSourceTagMessage: mismatchedGrpcSourceTagError.message,
        mismatchedGrpcSourceTagFeedName: Reflect.get(mismatchedGrpcSourceTagError, "feedName"),
        mismatchedGrpcLeasedSourceTagMessage: mismatchedGrpcLeasedSourceTagError.message,
        mismatchedGrpcLeasedSourceTagFeedName: Reflect.get(
          mismatchedGrpcLeasedSourceTagError,
          "feedName",
        ),
        malformedGrpcSourceMessage: malformedGrpcSourceError.message,
        malformedGrpcSourceFeedName: Reflect.get(malformedGrpcSourceError, "feedName"),
        extraGrpcSourceMessage: extraGrpcSourceError.message,
        extraGrpcSourceFeedName: Reflect.get(extraGrpcSourceError, "feedName"),
        materializedRouteByGrpcSourceMessage: materializedRouteByGrpcSourceError.message,
        materializedRouteByGrpcSourceFeedName: Reflect.get(
          materializedRouteByGrpcSourceError,
          "feedName",
        ),
        emptyRouteByGrpcSourceMessage: emptyRouteByGrpcSourceError.message,
        emptyRouteByGrpcSourceFeedName: Reflect.get(emptyRouteByGrpcSourceError, "feedName"),
        partialLeasedGrpcSourceMessage: partialLeasedGrpcSourceError.message,
        partialLeasedGrpcSourceFeedName: Reflect.get(partialLeasedGrpcSourceError, "feedName"),
        extraLeasedGrpcSourceMessage: extraLeasedGrpcSourceError.message,
        extraLeasedGrpcSourceFeedName: Reflect.get(extraLeasedGrpcSourceError, "feedName"),
      }).toStrictEqual({
        nullTopicMessage:
          "gRPC feed orders targets View Server topic orders, but that topic does not declare a gRPC source.",
        nullTopicFeedName: "orders",
        nonGrpcKindMessage: "View Server topic orders declares invalid gRPC source metadata.",
        nonGrpcKindFeedName: "orders",
        invalidLifecycleMessage: "View Server topic orders declares invalid gRPC source metadata.",
        invalidLifecycleFeedName: "orders",
        rawGrpcSourceMessage: "View Server topic orders declares invalid gRPC source metadata.",
        rawGrpcSourceFeedName: "orders",
        mismatchedGrpcSourceTagMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
        mismatchedGrpcSourceTagFeedName: "orders",
        mismatchedGrpcLeasedSourceTagMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
        mismatchedGrpcLeasedSourceTagFeedName: "orders",
        malformedGrpcSourceMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
        malformedGrpcSourceFeedName: "orders",
        extraGrpcSourceMessage: "View Server topic orders declares invalid gRPC source metadata.",
        extraGrpcSourceFeedName: "orders",
        materializedRouteByGrpcSourceMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
        materializedRouteByGrpcSourceFeedName: "orders",
        emptyRouteByGrpcSourceMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
        emptyRouteByGrpcSourceFeedName: "orders",
        partialLeasedGrpcSourceMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
        partialLeasedGrpcSourceFeedName: "orders",
        extraLeasedGrpcSourceMessage:
          "View Server topic orders declares invalid gRPC source metadata.",
        extraLeasedGrpcSourceFeedName: "orders",
      });
    }),
  );

  it.live(
    "rejects gRPC feed lifecycle mismatch when validation sees inconsistent resolved options",
    () =>
      Effect.gen(function* () {
        const config = grpcMaterializedViewServer(Stream.never);
        const grpcOptions = yield* resolveGrpcRuntimeOptions(config);
        const feed = yield* Effect.fromNullishOr(grpcOptions.feeds["orders"]);
        Object.defineProperty(feed, "lifecycle", { value: "leased" });

        const error = yield* validateGrpcSourceFeeds(grpcViewServer, grpcOptions).pipe(Effect.flip);

        expect({
          message: error.message,
          feedName: Reflect.get(error, "feedName"),
        }).toStrictEqual({
          message:
            "gRPC feed orders lifecycle leased does not match View Server topic orders source lifecycle materialized.",
          feedName: "orders",
        });
      }),
  );
});
