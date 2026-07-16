import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Duration, Effect, Schema, Stream } from "effect";
import { ViewServerGrpcIngressError } from "./grpc-ingress";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./grpc-runtime-options";
import {
  makeDefaultGrpcRuntimeSourceDependencies,
  makeGrpcRuntimeSourceAdapter,
  resolveGrpcRuntimeSourceOptions as resolveViewServerRuntimeOptions,
} from "./grpc-runtime-source";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import {
  grpcClients,
  GrpcOrder,
  grpcTopicOwnedSourceViewServer,
  grpcTopicSources,
} from "../test-harness/grpc-config";
import { grpcMaterializedViewServer } from "../test-harness/grpc-materialized";
import { grpcLeasedViewServer } from "../test-harness/grpc-leased";

type GrpcRuntimeOptionsSummary = {
  readonly clientBaseUrls: ReadonlyArray<readonly [string, string]>;
  readonly clientNames: ReadonlyArray<string>;
  readonly feeds: ReadonlyArray<{
    readonly client: string;
    readonly feedName: string;
    readonly lifecycle: "leased" | "materialized";
    readonly method: string;
    readonly topic: string;
  }>;
  readonly materializedReconnect: {
    readonly delay: Duration.Input;
    readonly maxReconnects: number;
  };
};

const summarizeGrpcRuntimeOptions = (
  options: ResolvedViewServerGrpcRuntimeOptions<typeof grpcTopicOwnedSourceViewServer.topics>,
): GrpcRuntimeOptionsSummary => ({
  clientBaseUrls: Object.entries(options.clientBaseUrls),
  clientNames: Object.keys(options.clients),
  feeds: Object.entries(options.feeds).map(([feedName, feed]) => ({
    client: feed.client,
    feedName,
    lifecycle: feed.lifecycle,
    method: feed.method,
    topic: feed.topic,
  })),
  materializedReconnect: options.materializedReconnect,
});

describe("gRPC runtime options", () => {
  it.live("hands resolved gRPC runtime options to the production source Adapter dependencies", () =>
    Effect.gen(function* () {
      type Topics = typeof grpcTopicOwnedSourceViewServer.topics;
      const defaults = makeDefaultRuntimeDependencies<Topics>();
      const grpcDefaults = makeDefaultGrpcRuntimeSourceDependencies<Topics>();
      let healthLedgerReceivedConfiguredTopics = false;
      let healthLedgerOptions: GrpcRuntimeOptionsSummary | undefined;
      let leaseManagerOptions: GrpcRuntimeOptionsSummary | undefined;
      let ingressOptions: GrpcRuntimeOptionsSummary | undefined;
      const dependencies: ViewServerRuntimeDependencies<Topics> = {
        ...defaults,
        sourceAdapters: [
          makeGrpcRuntimeSourceAdapter({
            ...grpcDefaults,
            makeHealthLedger: (config, options) => {
              healthLedgerReceivedConfiguredTopics =
                config.topics === grpcTopicOwnedSourceViewServer.topics;
              healthLedgerOptions = summarizeGrpcRuntimeOptions(options);
              return grpcDefaults.makeHealthLedger(config, options);
            },
            makeLeaseManager: (
              config,
              runtimeClient,
              liveClient,
              internalLiveClient,
              requestHealthRefresh,
              options,
              health,
            ) => {
              leaseManagerOptions = summarizeGrpcRuntimeOptions(options);
              return grpcDefaults.makeLeaseManager(
                config,
                runtimeClient,
                liveClient,
                internalLiveClient,
                requestHealthRefresh,
                options,
                health,
              );
            },
            makeIngress: (_config, _client, _requestHealthRefresh, options) => {
              ingressOptions = summarizeGrpcRuntimeOptions(options);
              return Effect.succeed({
                close: Effect.void,
              });
            },
          }),
        ],
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcTopicOwnedSourceViewServer,
        {
          grpc: {
            materializedReconnect: {
              delay: "100 millis",
              maxReconnects: 5,
            },
          },
        },
      );

      const expectedOptions: GrpcRuntimeOptionsSummary = {
        clientBaseUrls: [["orders", "https://orders.example.test"]],
        clientNames: ["orders"],
        feeds: [
          {
            client: "orders",
            feedName: "orders",
            lifecycle: "materialized",
            method: "streamOrders",
            topic: "orders",
          },
          {
            client: "orders",
            feedName: "routedOrders",
            lifecycle: "leased",
            method: "streamOrders",
            topic: "routedOrders",
          },
        ],
        materializedReconnect: {
          delay: "100 millis",
          maxReconnects: 5,
        },
      };
      expect({
        healthLedgerOptions,
        healthLedgerReceivedConfiguredTopics,
        ingressOptions,
        leaseManagerOptions,
      }).toStrictEqual({
        healthLedgerOptions: expectedOptions,
        healthLedgerReceivedConfiguredTopics: true,
        ingressOptions: expectedOptions,
        leaseManagerOptions: expectedOptions,
      });

      yield* runtime.close;
    }),
  );

  it.live("derives topic-owned gRPC runtime feeds without release callbacks", () =>
    Effect.gen(function* () {
      const noReleaseViewServer = defineViewServerConfig({
        grpc: {
          clients: grpcClients,
        },
        topics: {
          orders: grpcTopicSources.materialized({
            schema: GrpcOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "all-no-release-orders" }),
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
          routedOrders: grpcTopicSources.leased({
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
      });

      const options = yield* resolveViewServerRuntimeOptions(noReleaseViewServer);

      expect({
        ordersHasRelease: Object.hasOwn(options?.feeds["orders"] ?? {}, "release"),
        routedOrdersHasRelease: Object.hasOwn(options?.feeds["routedOrders"] ?? {}, "release"),
      }).toStrictEqual({
        ordersHasRelease: false,
        routedOrdersHasRelease: false,
      });
    }),
  );

  it.live("rejects explicit gRPC runtime options without clients or feeds", () =>
    Effect.gen(function* () {
      const sourceFreeViewServer = defineViewServerConfig({ topics: {} });
      const error = yield* resolveViewServerRuntimeOptions(sourceFreeViewServer, {
        grpc: {},
      }).pipe(Effect.flip);
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
          "runtime options.grpc was provided, but no gRPC clients were provided on config.grpc.clients.",
        phase: "configuration",
      });
    }),
  );

  it.live("rejects stale public gRPC runtime feed declarations", () =>
    Effect.gen(function* () {
      const clientsOptions = {
        grpc: {},
      };
      Object.defineProperty(clientsOptions.grpc, "clients", {
        enumerable: true,
        value: grpcClients,
      });
      const feedsOptions = {
        grpc: {},
      };
      Object.defineProperty(feedsOptions.grpc, "feeds", {
        enumerable: true,
        value: {
          orders: {},
        },
      });
      const unknownOptions = {
        grpc: {},
      };
      Object.defineProperty(unknownOptions.grpc, "feedz", {
        enumerable: true,
        value: {},
      });
      const unknownReconnectOptions = {
        grpc: {
          materializedReconnect: {
            delay: Duration.millis(100),
            maxReconnects: 1,
          },
        },
      };
      Object.defineProperty(unknownReconnectOptions.grpc.materializedReconnect, "maxAttempts", {
        enumerable: true,
        value: 1,
      });
      const nullReconnectOptions = {
        grpc: {},
      };
      Object.defineProperty(nullReconnectOptions.grpc, "materializedReconnect", {
        enumerable: true,
        value: null,
      });
      const nullGrpcOptions = {
        grpc: null,
      };
      const arrayGrpcOptions = {
        grpc: [],
      };

      const clientsError = yield* resolveViewServerRuntimeOptions(
        grpcTopicOwnedSourceViewServer,
        clientsOptions,
      ).pipe(Effect.flip);
      const feedsError = yield* resolveViewServerRuntimeOptions(
        grpcTopicOwnedSourceViewServer,
        feedsOptions,
      ).pipe(Effect.flip);
      const unknownError = yield* resolveViewServerRuntimeOptions(
        grpcTopicOwnedSourceViewServer,
        unknownOptions,
      ).pipe(Effect.flip);
      const unknownReconnectError = yield* resolveViewServerRuntimeOptions(
        grpcTopicOwnedSourceViewServer,
        unknownReconnectOptions,
      ).pipe(Effect.flip);
      const nullReconnectResult = yield* resolveViewServerRuntimeOptions(
        grpcTopicOwnedSourceViewServer,
        nullReconnectOptions,
      );
      const nullGrpcError = yield* resolveViewServerRuntimeOptions(
        grpcTopicOwnedSourceViewServer,
        // @ts-expect-error runtime guard rejects malformed public grpc options.
        nullGrpcOptions,
      ).pipe(Effect.flip);
      const arrayGrpcError = yield* resolveViewServerRuntimeOptions(
        grpcTopicOwnedSourceViewServer,
        // @ts-expect-error runtime guard rejects malformed public grpc options.
        arrayGrpcOptions,
      ).pipe(Effect.flip);
      const clientsGrpcError = yield* Schema.decodeUnknownEffect(ViewServerGrpcIngressError)(
        clientsError,
      );
      const feedsGrpcError = yield* Schema.decodeUnknownEffect(ViewServerGrpcIngressError)(
        feedsError,
      );
      const unknownGrpcError = yield* Schema.decodeUnknownEffect(ViewServerGrpcIngressError)(
        unknownError,
      );
      const unknownReconnectGrpcError = yield* Schema.decodeUnknownEffect(
        ViewServerGrpcIngressError,
      )(unknownReconnectError);
      const nullGrpcOptionsError = yield* Schema.decodeUnknownEffect(ViewServerGrpcIngressError)(
        nullGrpcError,
      );
      const arrayGrpcOptionsError = yield* Schema.decodeUnknownEffect(ViewServerGrpcIngressError)(
        arrayGrpcError,
      );

      expect({
        clientsError: {
          cause: clientsGrpcError.cause,
          message: clientsGrpcError.message,
          phase: clientsGrpcError.phase,
        },
        feedsError: {
          cause: feedsGrpcError.cause,
          message: feedsGrpcError.message,
          phase: feedsGrpcError.phase,
        },
        unknownError: {
          cause: unknownGrpcError.cause,
          message: unknownGrpcError.message,
          phase: unknownGrpcError.phase,
        },
        unknownReconnectError: {
          cause: unknownReconnectGrpcError.cause,
          message: unknownReconnectGrpcError.message,
          phase: unknownReconnectGrpcError.phase,
        },
        malformedGrpcOptions: {
          arrayCauseIsArray: Array.isArray(arrayGrpcOptionsError.cause),
          arrayMessage: arrayGrpcOptionsError.message,
          arrayPhase: arrayGrpcOptionsError.phase,
          nullCause: nullGrpcOptionsError.cause,
          nullMessage: nullGrpcOptionsError.message,
          nullPhase: nullGrpcOptionsError.phase,
        },
        nullReconnect: nullReconnectResult?.materializedReconnect,
      }).toStrictEqual({
        clientsError: {
          cause: "clients",
          message:
            "runtime options.grpc.clients is not supported; bind gRPC clients in defineViewServerConfig.grpc.clients.",
          phase: "configuration",
        },
        feedsError: {
          cause: "feeds",
          message:
            "runtime options.grpc.feeds is not supported; bind gRPC feeds on topic-owned grpcSource definitions.",
          phase: "configuration",
        },
        unknownError: {
          cause: "feedz",
          message: "runtime options.grpc has unsupported key: feedz",
          phase: "configuration",
        },
        unknownReconnectError: {
          cause: "maxAttempts",
          message: "runtime options.grpc.materializedReconnect has unsupported key: maxAttempts",
          phase: "configuration",
        },
        malformedGrpcOptions: {
          arrayCauseIsArray: true,
          arrayMessage: "runtime options.grpc must be an object when provided.",
          arrayPhase: "configuration",
          nullCause: null,
          nullMessage: "runtime options.grpc must be an object when provided.",
          nullPhase: "configuration",
        },
        nullReconnect: {
          delay: "1 second",
          maxReconnects: 60,
        },
      });
    }),
  );

  it.live("ignores inherited topic-owned gRPC bindings during runtime option derivation", () =>
    Effect.gen(function* () {
      const inheritedOrders = {
        schema: GrpcOrder,
        key: "id" as const,
      };
      Object.setPrototypeOf(inheritedOrders, {
        grpcSource: grpcTopicSources.materialized({
          schema: GrpcOrder,
          key: "id",
          client: "orders",
          method: "streamOrders",
          request: () => ({ orderId: "inherited" }),
          acquire: () => Stream.never,
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        }).grpcSource,
      });
      const viewServerWithInheritedSource = defineViewServerConfig({
        grpc: {
          clients: grpcClients,
        },
        topics: {
          orders: inheritedOrders,
        },
      });

      const options = yield* resolveViewServerRuntimeOptions(viewServerWithInheritedSource);

      expect(options).toBeUndefined();
    }),
  );

  it.live("accepts leased gRPC feeds for runtime lease management", () =>
    Effect.gen(function* () {
      const config = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const options = yield* resolveViewServerRuntimeOptions(config);

      expect(options?.feeds["orders"]).toMatchObject({
        client: "orders",
        lifecycle: "leased",
        method: "streamOrders",
        routeBy: ["region"],
        topic: "orders",
      });
    }),
  );

  it.live("rejects invalid materialized gRPC reconnect maxReconnects", () =>
    Effect.gen(function* () {
      const config = grpcMaterializedViewServer(Stream.never);
      const error = yield* resolveViewServerRuntimeOptions(config, {
        grpc: {
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: Infinity,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error).toStrictEqual(
        new ViewServerGrpcIngressError({
          message:
            "gRPC materialized reconnect maxReconnects must be a finite non-negative integer.",
          cause: Infinity,
          phase: "configuration",
        }),
      );
    }),
  );

  it.live("rejects invalid materialized gRPC reconnect delay", () =>
    Effect.gen(function* () {
      const config = grpcMaterializedViewServer(Stream.never);
      const error = yield* resolveViewServerRuntimeOptions(config, {
        grpc: {
          materializedReconnect: {
            delay: "Infinity",
            maxReconnects: 3,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error).toStrictEqual(
        new ViewServerGrpcIngressError({
          message: "gRPC materialized reconnect delay must be finite and positive.",
          cause: "Infinity",
          phase: "configuration",
        }),
      );
    }),
  );

  it.live("rejects zero materialized gRPC reconnect delay", () =>
    Effect.gen(function* () {
      const config = grpcMaterializedViewServer(Stream.never);
      const error = yield* resolveViewServerRuntimeOptions(config, {
        grpc: {
          materializedReconnect: {
            delay: 0,
            maxReconnects: 3,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error).toStrictEqual(
        new ViewServerGrpcIngressError({
          message: "gRPC materialized reconnect delay must be finite and positive.",
          cause: 0,
          phase: "configuration",
        }),
      );
    }),
  );
});
