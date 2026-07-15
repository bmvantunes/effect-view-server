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
import { Config, Effect, Exit, Layer, Scope } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  type ViewServerRuntimeDependencyConfig,
  type ViewServerRuntimeDependencies,
} from "./runtime-dependencies";
import type { ViewServerKafkaIngressError } from "./kafka-ingress";
import type { ViewServerGrpcIngressError } from "./grpc-ingress";
import type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";
import {
  resolveViewServerRuntimeOptions,
  validateGrpcSourceFeeds,
  type ResolvedViewServerRuntimeOptions,
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

const ignoreRuntimeStartupCleanupFailure = <R>(
  cleanup: Effect.Effect<void, never, R>,
): Effect.Effect<void, never, R> =>
  cleanup.pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Ignoring runtime startup cleanup failure.", cause),
    ),
    Effect.uninterruptible,
  );

const acquireRuntimeResource = Effect.fn("ViewServerRuntime.acquireResource")(function* <A, E, R>(
  scope: Scope.Scope,
  acquire: Effect.Effect<A, E, R>,
  release: (resource: A) => Effect.Effect<void>,
) {
  return yield* Effect.acquireRelease(acquire, release, { interruptible: true }).pipe(
    Scope.provide(scope),
  );
});

const acquireRuntimeResourceUninterruptibly = Effect.fn(
  "ViewServerRuntime.acquireResourceUninterruptibly",
)(function* <A, E, R>(
  scope: Scope.Scope,
  acquire: Effect.Effect<A, E, R>,
  release: (resource: A) => Effect.Effect<void>,
) {
  return yield* Effect.acquireRelease(acquire, release, { interruptible: false }).pipe(
    Scope.provide(scope),
  );
});

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
  const runtimeScope = yield* Scope.make("sequential");
  const startup = Effect.gen(function* () {
    const runtimeCore = yield* acquireRuntimeResource(
      runtimeScope,
      dependencies.makeRuntimeCore(dependencyConfig, runtimeCoreInput),
      (resource) => resource.close,
    );
    const kafkaHealthObserver =
      kafkaHealth === undefined
        ? undefined
        : yield* acquireRuntimeResourceUninterruptibly(
            runtimeScope,
            dependencies.makeKafkaHealthObserver(kafkaHealth, runtimeCore.requestHealthRefresh),
            (resource) => resource.close,
          );
    const refreshTransportHealth = ignoreRuntimeHealthRefreshFailure(runtimeCore.refreshHealth);
    const grpcLeaseManager =
      grpcOptions === undefined || grpcHealth === undefined
        ? undefined
        : yield* acquireRuntimeResource(
            runtimeScope,
            dependencies.makeGrpcLeaseManager(
              dependencyConfig,
              runtimeCore.internalClient,
              runtimeCore.liveClient,
              runtimeCore.internalLiveClient,
              runtimeCore.requestHealthRefresh,
              grpcOptions,
              grpcHealth,
            ),
            (resource) => resource.close,
          );
    const runtimeLiveClient = grpcLeaseManager?.liveClient ?? runtimeCore.liveClient;
    const runtimeClient = grpcLeaseManager?.client ?? runtimeCore.client;
    const server = yield* acquireRuntimeResource(
      runtimeScope,
      dependencies.makeServer(
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
      ),
      (resource) => resource.close,
    );
    if (kafkaOptions !== undefined && kafkaHealthObserver !== undefined) {
      yield* acquireRuntimeResource(
        runtimeScope,
        dependencies.makeKafkaIngress(
          dependencyConfig,
          runtimeCore.internalClient,
          kafkaOptions,
          kafkaHealthObserver,
        ),
        (resource) => resource.close,
      );
    }
    if (grpcOptions !== undefined && grpcHealth !== undefined) {
      yield* acquireRuntimeResource(
        runtimeScope,
        dependencies.makeGrpcIngress(
          dependencyConfig,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          grpcOptions,
          grpcHealth,
        ),
        (resource) => resource.close,
      );
    }
    const tcpPublishIngress =
      resolvedOptions.tcpPublishOptions === undefined
        ? undefined
        : yield* acquireRuntimeResource(
            runtimeScope,
            dependencies.makeTcpPublishIngress(dependencyConfig, runtimeCore.internalClient, {
              ...resolvedOptions.tcpPublishOptions,
              ...(resolvedOptions.auth === undefined ? {} : { auth: resolvedOptions.auth }),
            }),
            (resource) => resource.close,
          );
    const close: Effect.Effect<void> = (yield* Effect.cached(
      Scope.close(runtimeScope, Exit.void),
    )).pipe(Effect.uninterruptible);
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
  return yield* startup.pipe(
    Effect.onExit((exit) =>
      Exit.isFailure(exit)
        ? ignoreRuntimeStartupCleanupFailure(Scope.close(runtimeScope, exit))
        : Effect.void,
    ),
  );
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
      options === undefined
        ? makeViewServerRuntimeWithDependencies(dependencies, config)
        : makeViewServerRuntimeWithDependencies(dependencies, config, options),
      (runtime) => runtime.close,
      { interruptible: true },
    ).pipe(Effect.tap(logRuntimeStarted)),
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
