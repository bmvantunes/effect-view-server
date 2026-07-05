import type { ViewServerLiveClient, ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@effect-view-server/effect-utils";
import type { ViewServerRuntimeCoreOptionsFor } from "@effect-view-server/runtime-core";
import { makeSourceOwnershipPolicy } from "@effect-view-server/runtime-core/internal";
import { Config, Effect, Exit, Layer } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  type ViewServerRuntimeDependencyConfig,
  type ViewServerRuntimeDependencies,
} from "./runtime-dependencies";
import type { ViewServerKafkaIngressError } from "./kafka-ingress";
import type { ViewServerGrpcIngressError } from "./grpc-ingress";
import type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import {
  resolveViewServerRuntimeOptions,
  resolveViewServerRuntimeOptionsWithRuntimeFeeds,
  validateGrpcSourceFeeds,
  type ResolvedViewServerRuntimeOptions,
  type ViewServerRuntimeOptionsWithRuntimeFeeds,
} from "./runtime-options";
import type {
  ViewServerRuntime,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeOptions,
  ViewServerRuntimeOptionsArgs,
  ViewServerGrpcRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

export { makeDefaultRuntimeDependencies };
export type {
  ViewServerRuntime,
  ViewServerRuntimeDependencies,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeOptions,
  ViewServerRuntimeOptionsArgs,
  ViewServerGrpcRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
};

const toPublicLiveClient = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  close: Effect.Effect<void>,
): ViewServerLiveClient<Topics> => ({
  close,
  health: liveClient.health,
  subscribe: liveClient.subscribe,
  subscribeHealth: liveClient.subscribeHealth,
  subscribeHealthSummary: liveClient.subscribeHealthSummary,
});

const ignoreRuntimeHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring runtime health refresh failure.",
);

type RuntimeCoreOptionsBuilder<Topics extends ViewServerRuntimeTopicDefinitions> = {
  groupedIncrementalAdmissionLimits?: NonNullable<
    ViewServerRuntimeCoreOptionsFor<Topics>["groupedIncrementalAdmissionLimits"]
  >;
  subscriptionQueueCapacity?: NonNullable<
    ViewServerRuntimeCoreOptionsFor<Topics>["subscriptionQueueCapacity"]
  >;
  transportHealth: NonNullable<ViewServerRuntimeCoreOptionsFor<Topics>["transportHealth"]>;
  healthOverlay?: NonNullable<ViewServerRuntimeCoreOptionsFor<Topics>["healthOverlay"]>;
};

type ViewServerRuntimeFactoryError =
  | HttpServerError.ServeError
  | Config.ConfigError
  | ViewServerRuntimeError
  | ViewServerKafkaIngressError
  | ViewServerGrpcIngressError
  | ViewServerTcpPublishIngressError;

type MakeViewServerRuntimeWithDependencies = {
  <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions = RuntimeRegions,
    const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics, Regions, GrpcClients>,
  ): Effect.Effect<ViewServerRuntime<Topics>, ViewServerRuntimeFactoryError>;
  <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions = RuntimeRegions,
    const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
    const Options extends object = ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics, Regions, GrpcClients>,
    options: Options,
  ): Effect.Effect<ViewServerRuntime<Topics, Options>, ViewServerRuntimeFactoryError>;
};

type MakeViewServerRuntimeWithRuntimeFeedsAndDependencies = {
  <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions = RuntimeRegions,
    const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
    const Options extends object = ViewServerRuntimeOptionsWithRuntimeFeeds<
      Topics,
      Regions,
      GrpcClients
    >,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics, Regions, GrpcClients>,
    options: Options,
  ): Effect.Effect<ViewServerRuntime<Topics, Options>, ViewServerRuntimeFactoryError>;
};

export const makeViewServerRuntimeWithDependencies: MakeViewServerRuntimeWithDependencies =
  Effect.fn("ViewServerRuntime.makeWithDependencies")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions = RuntimeRegions,
    const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
    const Options extends object = ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics, Regions, GrpcClients>,
    options?: Options,
  ) {
    if (options === undefined) {
      const resolvedOptions = yield* resolveViewServerRuntimeOptions(config, {});
      return yield* makeViewServerRuntimeFromResolvedOptions(dependencies, config, resolvedOptions);
    }
    const resolvedOptions = yield* resolveViewServerRuntimeOptions(config, options);
    return yield* makeViewServerRuntimeFromResolvedOptions(dependencies, config, resolvedOptions);
  });

export const makeViewServerRuntimeWithRuntimeFeedsAndDependencies: MakeViewServerRuntimeWithRuntimeFeedsAndDependencies =
  Effect.fn("ViewServerRuntime.makeWithRuntimeFeedsAndDependencies")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions = RuntimeRegions,
    const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
    const Options extends object = ViewServerRuntimeOptionsWithRuntimeFeeds<
      Topics,
      Regions,
      GrpcClients
    >,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics, Regions, GrpcClients>,
    options: Options,
  ) {
    // Private dependency-injection seam for tests/benchmarks that need already-resolved
    // gRPC feeds. Public runtime options must use topic-owned `grpcSource` bindings.
    const resolvedOptions = yield* resolveViewServerRuntimeOptionsWithRuntimeFeeds(config, options);
    return yield* makeViewServerRuntimeFromResolvedOptions(dependencies, config, resolvedOptions);
  });

const makeViewServerRuntimeFromResolvedOptions = Effect.fn(
  "ViewServerRuntime.makeFromResolvedOptions",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  resolvedOptions: ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
) {
  const dependencyConfig: ViewServerRuntimeDependencyConfig<Topics> = {
    topics: config.topics,
  };
  const kafkaOptions = resolvedOptions.kafkaOptions;
  const grpcOptions = resolvedOptions.grpcOptions;
  yield* validateGrpcSourceFeeds(dependencyConfig, grpcOptions);
  const transportHealth = makeViewServerRuntimeTransportHealth<Topics>();
  const kafkaHealth =
    kafkaOptions === undefined
      ? undefined
      : dependencies.makeKafkaHealthLedger(dependencyConfig, kafkaOptions);
  const grpcHealth =
    grpcOptions === undefined
      ? undefined
      : dependencies.makeGrpcHealthLedger(dependencyConfig, grpcOptions);
  const runtimeCoreInput: RuntimeCoreOptionsBuilder<Topics> = {
    transportHealth: transportHealth.transportHealth,
  };
  if (resolvedOptions.runtimeCoreOptions.groupedIncrementalAdmissionLimits !== undefined) {
    runtimeCoreInput.groupedIncrementalAdmissionLimits =
      resolvedOptions.runtimeCoreOptions.groupedIncrementalAdmissionLimits;
  }
  if (resolvedOptions.runtimeCoreOptions.subscriptionQueueCapacity !== undefined) {
    runtimeCoreInput.subscriptionQueueCapacity =
      resolvedOptions.runtimeCoreOptions.subscriptionQueueCapacity;
  }
  if (kafkaHealth !== undefined || grpcHealth !== undefined) {
    runtimeCoreInput.healthOverlay = (
      health: ViewServerHealth<Topics>,
      nowMillis: number,
    ): ViewServerHealth<Topics> => {
      const kafkaOverlayed =
        kafkaHealth === undefined ? health : kafkaHealth.healthOverlay(health, nowMillis);
      return grpcHealth === undefined
        ? kafkaOverlayed
        : grpcHealth.healthOverlay(kafkaOverlayed, nowMillis);
    };
  }
  const runtimeCore = yield* dependencies.makeRuntimeCore(dependencyConfig, runtimeCoreInput);
  const refreshTransportHealth = ignoreRuntimeHealthRefreshFailure(runtimeCore.refreshHealth);
  const grpcLeaseManager =
    grpcOptions === undefined || grpcHealth === undefined
      ? undefined
      : yield* makeViewServerGrpcLeaseManager(
          dependencyConfig,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          runtimeCore.requestHealthRefresh,
          grpcOptions,
          grpcHealth,
        );
  const sourceOwnership = makeSourceOwnershipPolicy(dependencyConfig);
  const runtimeLiveClient = grpcLeaseManager?.liveClient ?? runtimeCore.liveClient;
  const runtimeClient = grpcLeaseManager?.client ?? runtimeCore.client;
  const closeGrpcLeaseManager =
    grpcLeaseManager === undefined ? Effect.void : grpcLeaseManager.close;
  const tcpPublishIngress =
    resolvedOptions.tcpPublishOptions === undefined
      ? undefined
      : yield* dependencies
          .makeTcpPublishIngress(dependencyConfig, runtimeClient, {
            ...resolvedOptions.tcpPublishOptions,
            ...(resolvedOptions.auth === undefined ? {} : { auth: resolvedOptions.auth }),
            rejectedTopics: sourceOwnership.sourceOwnedTopics,
          })
          .pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? closeGrpcLeaseManager.pipe(Effect.ensuring(runtimeCore.close))
                : Effect.void,
            ),
          );
  const server = yield* dependencies
    .makeServer(
      dependencyConfig,
      {
        ...(resolvedOptions.auth === undefined ? {} : { auth: resolvedOptions.auth }),
        liveClient: runtimeLiveClient,
        runtime: runtimeClient,
        transport: {
          clientOpened: transportHealth.clientOpened.pipe(Effect.andThen(refreshTransportHealth)),
          clientClosed: transportHealth.clientClosed.pipe(Effect.andThen(refreshTransportHealth)),
          streamOpened: transportHealth.streamOpened.pipe(Effect.andThen(refreshTransportHealth)),
          streamClosed: transportHealth.streamClosed.pipe(Effect.andThen(refreshTransportHealth)),
        },
      },
      resolvedOptions.serverOptions,
    )
    .pipe(
      Effect.onExit((exit) =>
        Exit.isFailure(exit)
          ? (tcpPublishIngress?.close ?? Effect.void).pipe(
              Effect.ensuring(closeGrpcLeaseManager),
              Effect.ensuring(runtimeCore.close),
            )
          : Effect.void,
      ),
    );
  const kafkaIngress =
    kafkaOptions === undefined || kafkaHealth === undefined
      ? undefined
      : yield* dependencies
          .makeKafkaIngress(
            dependencyConfig,
            runtimeCore.internalClient,
            runtimeCore.requestHealthRefresh,
            kafkaOptions,
            kafkaHealth,
          )
          .pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? (tcpPublishIngress?.close ?? Effect.void).pipe(
                    Effect.ensuring(server.close),
                    Effect.ensuring(closeGrpcLeaseManager),
                    Effect.ensuring(runtimeCore.close),
                  )
                : Effect.void,
            ),
          );
  const grpcIngress =
    grpcOptions === undefined || grpcHealth === undefined
      ? undefined
      : yield* dependencies
          .makeGrpcIngress(
            dependencyConfig,
            runtimeCore.internalClient,
            runtimeCore.requestHealthRefresh,
            grpcOptions,
            grpcHealth,
          )
          .pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? (tcpPublishIngress?.close ?? Effect.void).pipe(
                    Effect.ensuring(kafkaIngress?.close ?? Effect.void),
                    Effect.ensuring(closeGrpcLeaseManager),
                    Effect.ensuring(server.close),
                    Effect.ensuring(runtimeCore.close),
                  )
                : Effect.void,
            ),
          );
  const closeGrpcIngress: Effect.Effect<void> =
    grpcIngress === undefined ? Effect.void : grpcIngress.close;
  const closeKafkaIngress: Effect.Effect<void> =
    kafkaIngress === undefined ? Effect.void : kafkaIngress.close;
  const closeTcpPublishIngress: Effect.Effect<void> =
    tcpPublishIngress === undefined ? Effect.void : tcpPublishIngress.close;
  const close: Effect.Effect<void> = closeTcpPublishIngress.pipe(
    Effect.ensuring(closeGrpcIngress),
    Effect.ensuring(closeGrpcLeaseManager),
    Effect.ensuring(closeKafkaIngress),
    Effect.ensuring(server.close),
    Effect.ensuring(runtimeCore.close),
  );
  const publicLiveClient = toPublicLiveClient(runtimeLiveClient, close);
  return {
    url: server.url,
    healthUrl: server.healthUrl,
    metricsUrl: server.metricsUrl,
    ...(tcpPublishIngress === undefined ? {} : { tcpPublishUrl: tcpPublishIngress.url }),
    client: runtimeClient,
    liveClient: publicLiveClient,
    health: runtimeClient.health,
    close,
  };
});

const logRuntimeStarted = Effect.fn("ViewServerRuntime.logStarted")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(runtime: ViewServerRuntime<Topics>) {
  yield* Effect.logInfo(`View Server WebSocket listening at ${runtime.url}`);
  yield* Effect.logInfo(`View Server health endpoint listening at ${runtime.healthUrl}`);
  yield* Effect.logInfo(`View Server metrics endpoint listening at ${runtime.metricsUrl}`);
  if (runtime.tcpPublishUrl !== undefined) {
    yield* Effect.logInfo(`View Server TCP publish endpoint listening at ${runtime.tcpPublishUrl}`);
  }
});

const makeViewServerRuntimeLaunchLayer = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
  const Options extends object,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options?: ViewServerRuntimeOptionsInput<Topics, Regions, GrpcClients, Options>,
) =>
  Layer.effectDiscard(
    Effect.acquireRelease(
      (options === undefined
        ? makeViewServerRuntimeWithDependencies(dependencies, config)
        : makeViewServerRuntimeWithDependencies(dependencies, config, options)
      ).pipe(Effect.tap(logRuntimeStarted)),
      (runtime) => runtime.close,
    ),
  );

export const runViewServerRuntimeWithDependencies: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions = RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  const Options extends object = ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options?: ViewServerRuntimeOptionsInput<Topics, Regions, GrpcClients, Options>,
) => Effect.Effect<never, ViewServerRuntimeFactoryError> = Effect.fn(
  "ViewServerRuntime.runWithDependencies",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
  const Options extends object,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options?: ViewServerRuntimeOptionsInput<Topics, Regions, GrpcClients, Options>,
) {
  return yield* makeViewServerRuntimeLaunchLayer(dependencies, config, options).pipe(Layer.launch);
});
