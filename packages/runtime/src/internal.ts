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
import type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";
import {
  resolveViewServerRuntimeBaseOptions,
  type ResolvedViewServerRuntimeBaseOptions,
} from "./runtime-options";
import type { ViewServerRuntimeSourceError } from "./runtime-source-adapters";
import {
  validateRuntimeSourceOwnership,
  type ViewServerRuntimePreparedSource,
  type ViewServerRuntimeSourceModule,
} from "./runtime-source";
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
  | ViewServerRuntimeSourceError
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
      const runtimeOptions: ViewServerRuntimeOptions<Topics, Regions, GrpcClients> = {};
      return yield* makeViewServerRuntimeFromResolvedOptions(
        dependencies,
        config,
        runtimeOptions,
        resolveViewServerRuntimeBaseOptions(runtimeOptions),
      );
    }
    return yield* makeViewServerRuntimeFromResolvedOptions(
      dependencies,
      config,
      options,
      resolveViewServerRuntimeBaseOptions(options),
    );
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
  runtimeOptions: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  resolvedOptions: ResolvedViewServerRuntimeBaseOptions<Topics, Regions, GrpcClients>,
) {
  const dependencyConfig: ViewServerRuntimeDependencyConfig<Topics> = {
    topics: config.topics,
  };
  const sourceModules: Array<ViewServerRuntimeSourceModule<Topics, ViewServerRuntimeSourceError>> =
    [];
  for (const sourceAdapter of dependencies.sourceAdapters) {
    const sourceModule = yield* sourceAdapter.make(config, runtimeOptions);
    if (sourceModule !== undefined) {
      sourceModules.push(sourceModule);
    }
  }
  yield* validateRuntimeSourceOwnership(sourceModules);
  const transportHealth = makeViewServerRuntimeTransportHealth<Topics>();
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
  if (sourceModules.length > 0) {
    runtimeCoreInput.healthOverlay = (
      health: ViewServerHealth<Topics>,
      nowMillis: number,
    ): ViewServerHealth<Topics> => {
      let overlayed = health;
      for (const sourceModule of sourceModules) {
        overlayed = sourceModule.healthOverlay(overlayed, nowMillis);
      }
      return overlayed;
    };
  }
  const runtimeScope = yield* Scope.make("sequential");
  const startup = Effect.gen(function* () {
    const runtimeCore = yield* acquireRuntimeResource(
      runtimeScope,
      dependencies.makeRuntimeCore(dependencyConfig, runtimeCoreInput),
      (resource) => resource.close,
    );
    const refreshRuntimeHealth = ignoreRuntimeHealthRefreshFailure(runtimeCore.refreshHealth);
    const preparedSources: Array<
      ViewServerRuntimePreparedSource<Topics, ViewServerRuntimeSourceError>
    > = [];
    let runtimeLiveClient = runtimeCore.liveClient;
    let runtimeClient = runtimeCore.client;
    for (const sourceModule of sourceModules) {
      const preparedSource = yield* sourceModule
        .prepare({
          client: runtimeClient,
          internalClient: runtimeCore.internalClient,
          internalLiveClient: runtimeCore.internalLiveClient,
          liveClient: runtimeLiveClient,
          refreshHealth: refreshRuntimeHealth,
          requestHealthRefresh: runtimeCore.requestHealthRefresh,
        })
        .pipe(Scope.provide(runtimeScope));
      preparedSources.push(preparedSource);
      runtimeClient = preparedSource.client;
      runtimeLiveClient = preparedSource.liveClient;
    }
    const server = yield* acquireRuntimeResource(
      runtimeScope,
      dependencies.makeServer(
        dependencyConfig,
        {
          ...(resolvedOptions.auth === undefined ? {} : { auth: resolvedOptions.auth }),
          liveClient: runtimeLiveClient,
          runtime: runtimeClient,
          transport: {
            clientOpened: transportHealth.clientOpened.pipe(Effect.andThen(refreshRuntimeHealth)),
            clientClosed: transportHealth.clientClosed.pipe(Effect.andThen(refreshRuntimeHealth)),
            streamOpened: transportHealth.streamOpened.pipe(Effect.andThen(refreshRuntimeHealth)),
            streamClosed: transportHealth.streamClosed.pipe(Effect.andThen(refreshRuntimeHealth)),
          },
        },
        resolvedOptions.serverOptions,
      ),
      (resource) => resource.close,
    );
    for (const preparedSource of preparedSources) {
      yield* preparedSource.start.pipe(Scope.provide(runtimeScope));
    }
    const tcpPublishIngress =
      resolvedOptions.tcpPublishOptions === undefined
        ? undefined
        : yield* acquireRuntimeResource(
            runtimeScope,
            dependencies.makeTcpPublishIngress(
              dependencyConfig,
              runtimeCore.decodedMutationClient,
              {
                ...resolvedOptions.tcpPublishOptions,
                ...(resolvedOptions.auth === undefined ? {} : { auth: resolvedOptions.auth }),
              },
            ),
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
