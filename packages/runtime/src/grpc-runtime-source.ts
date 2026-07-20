import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
} from "@effect-view-server/config";
import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type {
  ViewServerRuntimeCoreInternalClient,
  ViewServerRuntimeCoreInternalLiveClient,
} from "@effect-view-server/runtime-core/internal";
import type { Effect } from "effect";
import { Effect as RuntimeEffect } from "effect";
import { makeViewServerGrpcHealthLedger, type ViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcIngress, type ViewServerGrpcIngress } from "./grpc-ingress";
import {
  makeViewServerGrpcLeaseManager,
  type ViewServerGrpcLeaseManager,
} from "./grpc-lease-manager";
import { ViewServerGrpcIngressError } from "./grpc-source-lifecycle";
import {
  grpcFeedsFromConfig,
  hasGrpcSourceDeclarations,
  resolveViewServerGrpcRuntimeOptions,
  validateGrpcSourceFeeds,
  validatePublicGrpcRuntimeOptions,
  type ResolvedViewServerGrpcRuntimeOptions,
} from "./grpc-runtime-options";
import type {
  ViewServerRuntimeSourceAdapter,
  ViewServerRuntimeSourceModule,
} from "./runtime-source";
import type { ViewServerRuntimeOptions, ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export const resolveGrpcRuntimeSourceOptions = RuntimeEffect.fn(
  "ViewServerRuntime.source.grpc.options.resolve",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, Clients>,
  runtimeOptions: ViewServerRuntimeOptions<Topics, Regions, Clients> = {},
) {
  yield* validatePublicGrpcRuntimeOptions(runtimeOptions.grpc);
  if (runtimeOptions.grpc === undefined) {
    if (!hasGrpcSourceDeclarations(config)) {
      return undefined;
    }
    if (Object.keys(grpcFeedsFromConfig(config)).length === 0) {
      yield* validateGrpcSourceFeeds(config, undefined);
    }
  }
  const options = yield* resolveViewServerGrpcRuntimeOptions(config, runtimeOptions.grpc ?? {});
  yield* validateGrpcSourceFeeds(config, options);
  return options;
});

export type ViewServerGrpcRuntimeSourceDependencies<
  Topics extends ViewServerRuntimeTopicDefinitions,
> = {
  readonly makeHealthLedger: <const Clients extends GrpcRuntimeClients>(
    config: { readonly topics: Topics },
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  ) => ViewServerGrpcHealthLedger<Topics>;
  readonly makeLeaseManager: <const Clients extends GrpcRuntimeClients>(
    config: { readonly topics: Topics },
    runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
    liveClient: ViewServerRuntimeLiveClient<Topics>,
    internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
    health: ViewServerGrpcHealthLedger<Topics>,
  ) => Effect.Effect<ViewServerGrpcLeaseManager<Topics>>;
  readonly makeIngress: <const Clients extends GrpcRuntimeClients>(
    config: { readonly topics: Topics },
    client: ViewServerRuntimeCoreInternalClient<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
    health: ViewServerGrpcHealthLedger<Topics>,
  ) => Effect.Effect<ViewServerGrpcIngress, ViewServerGrpcIngressError>;
};

export const makeDefaultGrpcRuntimeSourceDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerGrpcRuntimeSourceDependencies<Topics> => ({
  makeHealthLedger: (config, options) => {
    const hasConfiguredTopic = (topic: string): topic is Extract<keyof Topics, string> =>
      Object.hasOwn(config.topics, topic);
    const feeds: Record<
      string,
      {
        readonly client: string;
        readonly lifecycle: "materialized" | "leased";
        readonly topic: Extract<keyof Topics, string>;
      }
    > = Object.create(null);
    for (const [feedName, feed] of Object.entries(options.feeds)) {
      if (hasConfiguredTopic(feed.topic)) {
        feeds[feedName] = {
          client: feed.client,
          lifecycle: feed.lifecycle,
          topic: feed.topic,
        };
      }
    }
    return makeViewServerGrpcHealthLedger({
      clients: options.clientBaseUrls,
      feeds,
    });
  },
  makeLeaseManager: makeViewServerGrpcLeaseManager,
  makeIngress: makeViewServerGrpcIngress,
});

export const makeGrpcRuntimeSourceAdapter = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  dependencies: ViewServerGrpcRuntimeSourceDependencies<Topics> = makeDefaultGrpcRuntimeSourceDependencies<Topics>(),
): ViewServerRuntimeSourceAdapter<Topics, ViewServerGrpcIngressError> => ({
  make: (config, runtimeOptions) =>
    RuntimeEffect.gen(function* () {
      const options = yield* resolveGrpcRuntimeSourceOptions(config, runtimeOptions);
      if (options === undefined) {
        return undefined;
      }
      const dependencyConfig = { topics: config.topics };
      const health = dependencies.makeHealthLedger(dependencyConfig, options);
      const module: ViewServerRuntimeSourceModule<Topics, ViewServerGrpcIngressError> = {
        healthOverlay: health.healthOverlay,
        ownedTopics: Object.entries(options.feeds).map(([feedName, feed]) => ({
          topic: feed.topic,
          owner: `gRPC feed ${feedName}`,
          conflict: (existingOwner) =>
            new ViewServerGrpcIngressError({
              message: `View Server topic ${feed.topic} cannot be owned by both ${existingOwner} and gRPC feed ${feedName}.`,
              cause: feed.topic,
              feedName,
              topic: feed.topic,
            }),
        })),
        prepare: (input) =>
          RuntimeEffect.gen(function* () {
            const manager = yield* RuntimeEffect.acquireRelease(
              dependencies.makeLeaseManager(
                dependencyConfig,
                input.internalClient,
                input.liveClient,
                input.internalLiveClient,
                input.requestHealthRefresh,
                options,
                health,
              ),
              (resource) => resource.close,
              { interruptible: true },
            );
            return {
              client: manager.client,
              liveClient: manager.liveClient,
              protocolQuerySubscriber: manager.protocolQuerySubscriber,
              start: RuntimeEffect.acquireRelease(
                dependencies.makeIngress(
                  dependencyConfig,
                  input.internalClient,
                  input.requestHealthRefresh,
                  options,
                  health,
                ),
                (resource) => resource.close,
                { interruptible: true },
              ).pipe(RuntimeEffect.asVoid),
            };
          }),
      };
      return module;
    }),
});
