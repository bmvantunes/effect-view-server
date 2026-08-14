import { definedFields } from "@effect-view-server/effect-utils";
import type { ViewServerLiveClient, ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type { ViewServerConfig, ViewServerRuntimeError } from "@effect-view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@effect-view-server/effect-utils";
import type {
  ViewServerRuntimeCoreOptionsFor,
  ViewServerSourceRequirements,
} from "@effect-view-server/runtime-core";
import { Effect, Exit, Fiber, Layer, Option, Ref, Scope } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  type ViewServerRuntimeDependencyConfig,
  type ViewServerRuntimeDependencies,
} from "./runtime-dependencies";
import {
  resolveViewServerRuntimeBaseOptions,
  type ResolvedViewServerRuntimeBaseOptions,
  validateViewServerRuntimeOptions,
} from "./runtime-options";
import type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";
import type {
  ViewServerRuntime,
  ViewServerRuntimeOptions,
  ViewServerRuntimeOptionsArgs,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeReportingOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";
import { makeRuntimeReporting, startingHeartbeat, stoppingHeartbeat } from "./runtime-reporting";

export { makeDefaultRuntimeDependencies };
export type {
  ViewServerRuntime,
  ViewServerRuntimeDependencies,
  ViewServerRuntimeOptions,
  ViewServerRuntimeOptionsArgs,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeReportingOptions,
  ViewServerRuntimeTopicDefinitions,
};
export type {
  RuntimeDependency,
  RuntimeDependencyIssue,
  RuntimeHeartbeat,
} from "./runtime-reporting";

const toPublicLiveClient = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  close: Effect.Effect<void>,
): ViewServerLiveClient<Topics> => ({
  close,
  health: liveClient.health,
  subscribe: liveClient.subscribe,
  subscribeHealth: liveClient.subscribeHealth,
  subscribeHealthSummary: liveClient.subscribeHealthSummary,
  subscribeSourceHealth: liveClient.subscribeSourceHealth,
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
};

type ViewServerRuntimeFactoryError =
  | HttpServerError.ServeError
  | ViewServerRuntimeError
  | ViewServerTcpPublishIngressError;

const runtimeFatalSignals = new WeakMap<object, Effect.Effect<never, ViewServerRuntimeError>>();

type MakeViewServerRuntimeWithDependencies = {
  <const Topics extends ViewServerRuntimeTopicDefinitions>(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics>,
  ): Effect.Effect<
    ViewServerRuntime<Topics>,
    ViewServerRuntimeFactoryError,
    ViewServerSourceRequirements<Topics>
  >;
  <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Options extends object = ViewServerRuntimeOptions<Topics>,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics>,
    options: ViewServerRuntimeOptionsInput<Topics, Options>,
  ): Effect.Effect<
    ViewServerRuntime<Topics>,
    ViewServerRuntimeFactoryError,
    ViewServerSourceRequirements<Topics>
  >;
};

export const makeViewServerRuntimeWithDependencies: MakeViewServerRuntimeWithDependencies =
  Effect.fn("ViewServerRuntime.makeWithDependencies")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Options extends object = ViewServerRuntimeOptions<Topics>,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics>,
    options?: ViewServerRuntimeOptionsInput<Topics, Options>,
  ) {
    const runtimeOptions = yield* validateViewServerRuntimeOptions<Topics>(options ?? {});
    return yield* makeViewServerRuntimeFromResolvedOptions(
      dependencies,
      config,
      resolveViewServerRuntimeBaseOptions(runtimeOptions),
    );
  });

const makeViewServerRuntimeFromResolvedOptions = Effect.fn(
  "ViewServerRuntime.makeFromResolvedOptions",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  resolvedOptions: ResolvedViewServerRuntimeBaseOptions<Topics>,
) {
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const runtimeScope = yield* Scope.make("sequential");
      const reportingLifecycle =
        resolvedOptions.reporting === undefined
          ? undefined
          : {
              options: resolvedOptions.reporting,
              startingFiber: yield* startingHeartbeat(runtimeScope, resolvedOptions.reporting),
            };
      const startupStopping = yield* Ref.make(
        reportingLifecycle === undefined
          ? Effect.void
          : stoppingHeartbeat(
              runtimeScope,
              reportingLifecycle.options,
              reportingLifecycle.startingFiber,
            ),
      );
      const dependencyConfig: ViewServerRuntimeDependencyConfig<Topics> = {
        topics: config.topics,
      };
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
      const startup = Effect.gen(function* () {
        const runtimeCore = yield* acquireRuntimeResource(
          runtimeScope,
          dependencies.makeRuntimeCore(dependencyConfig, runtimeCoreInput),
          (resource) => resource.close,
        );
        const closeRuntimeScope = Scope.close(runtimeScope, Exit.void).pipe(Effect.uninterruptible);
        const cachedScopeCloseFiber = yield* Effect.cached(
          closeRuntimeScope.pipe(
            Effect.forkDetach({
              startImmediately: true,
            }),
          ),
        );
        const awaitRuntimeScopeClose = cachedScopeCloseFiber.pipe(
          Effect.flatMap(Fiber.join),
          Effect.asVoid,
        );
        const refreshRuntimeHealth = ignoreRuntimeHealthRefreshFailure(runtimeCore.refreshHealth);
        const transports = yield* Effect.raceFirst(
          Effect.gen(function* () {
            const server = yield* acquireRuntimeResource(
              runtimeScope,
              dependencies.makeServer(
                dependencyConfig,
                {
                  ...definedFields(resolvedOptions.auth, (auth) => ({ auth })),
                  liveClient: {
                    subscribeHealth: runtimeCore.liveClient.subscribeHealth,
                    subscribeHealthSummary: runtimeCore.liveClient.subscribeHealthSummary,
                    subscribeProtocolSourceHealth:
                      runtimeCore.serverLiveClient.subscribeProtocolSourceHealth,
                    subscribeProtocolQuery:
                      runtimeCore.protocolQuerySubscriber.subscribeProtocolQuery,
                  },
                  runtime: runtimeCore.client,
                  transport: {
                    clientOpened: transportHealth.clientOpened.pipe(
                      Effect.andThen(refreshRuntimeHealth),
                    ),
                    clientClosed: transportHealth.clientClosed.pipe(
                      Effect.andThen(refreshRuntimeHealth),
                    ),
                    streamOpened: transportHealth.streamOpened.pipe(
                      Effect.andThen(refreshRuntimeHealth),
                    ),
                    streamClosed: transportHealth.streamClosed.pipe(
                      Effect.andThen(refreshRuntimeHealth),
                    ),
                  },
                },
                resolvedOptions.serverOptions,
              ),
              (resource) => resource.close,
            );
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
                        ...definedFields(resolvedOptions.auth, (auth) => ({ auth })),
                      },
                    ),
                    (resource) => resource.close,
                  );
            return {
              server,
              tcpPublishIngress,
            };
          }),
          runtimeCore.fatal,
        );
        const reporting =
          reportingLifecycle === undefined
            ? undefined
            : yield* Effect.gen(function* () {
                yield* Fiber.interrupt(reportingLifecycle.startingFiber);
                return yield* makeRuntimeReporting(
                  runtimeScope,
                  reportingLifecycle.options,
                  runtimeCore.reporting,
                ).pipe(
                  Effect.tap((reporter) => Ref.set(startupStopping, reporter.stopping)),
                  Effect.uninterruptible,
                );
              });
        const cachedShutdownFiber = yield* Effect.cached(
          (reporting === undefined ? Effect.void : reporting.stopping).pipe(
            Effect.ensuring(awaitRuntimeScopeClose),
            Effect.forkDetach({ startImmediately: true }),
          ),
        );
        const awaitShutdown = cachedShutdownFiber.pipe(Effect.flatMap(Fiber.join), Effect.asVoid);
        const fatalWatcher = yield* Effect.forkDetach(
          runtimeCore.fatal.pipe(Effect.catchCause(() => awaitShutdown)),
          { startImmediately: true },
        );
        const close: Effect.Effect<void> = Fiber.interrupt(fatalWatcher).pipe(
          Effect.asVoid,
          Effect.andThen(awaitShutdown),
        );
        const publicLiveClient = toPublicLiveClient(runtimeCore.liveClient, close);
        const runtime: ViewServerRuntime<Topics> = {
          url: transports.server.url,
          healthUrl: transports.server.healthUrl,
          metricsUrl: transports.server.metricsUrl,
          ...definedFields(transports.tcpPublishIngress, (tcpPublishIngress) => ({
            tcpPublishUrl: tcpPublishIngress.url,
          })),
          client: runtimeCore.client,
          liveClient: publicLiveClient,
          health: runtimeCore.client.health,
          close,
        };
        runtimeFatalSignals.set(runtime, runtimeCore.fatal);
        return runtime;
      });
      return yield* restore(startup).pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? ignoreRuntimeStartupCleanupFailure(
                Ref.get(startupStopping).pipe(
                  Effect.flatMap((reportStopping) => reportStopping),
                  Effect.ensuring(Scope.close(runtimeScope, exit)),
                ),
              )
            : Effect.void,
        ),
      );
    }),
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
  const Options extends object,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) =>
  Layer.effectDiscard(
    Effect.acquireRelease(
      options === undefined
        ? makeViewServerRuntimeWithDependencies(dependencies, config)
        : makeViewServerRuntimeWithDependencies(dependencies, config, options),
      (runtime) => runtime.close,
      { interruptible: true },
    ).pipe(
      Effect.tap(logRuntimeStarted),
      Effect.flatMap((runtime) =>
        Option.getOrThrow(Option.fromNullishOr(runtimeFatalSignals.get(runtime))),
      ),
    ),
  );

export const runViewServerRuntimeWithDependencies: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object = ViewServerRuntimeOptions<Topics>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) => Effect.Effect<never, ViewServerRuntimeFactoryError, ViewServerSourceRequirements<Topics>> =
  Effect.fn("ViewServerRuntime.runWithDependencies")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Options extends object,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics>,
    options?: ViewServerRuntimeOptionsInput<Topics, Options>,
  ) {
    return yield* makeViewServerRuntimeLaunchLayer(dependencies, config, options).pipe(
      Layer.launch,
    );
  });
