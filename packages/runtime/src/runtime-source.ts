import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeClient,
} from "@effect-view-server/config";
import type {
  ViewServerRuntimeCoreInternalClient,
  ViewServerRuntimeCoreInternalLiveClient,
} from "@effect-view-server/runtime-core/internal";
import { Effect, type Config, type Scope } from "effect";
import type { ViewServerRuntimeOptions, ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerRuntimeSourceClients<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
};

export type ViewServerRuntimeSourcePrepareInput<Topics extends ViewServerRuntimeTopicDefinitions> =
  ViewServerRuntimeSourceClients<Topics> & {
    readonly internalClient: ViewServerRuntimeCoreInternalClient<Topics>;
    readonly internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>;
    readonly refreshHealth: Effect.Effect<void>;
    readonly requestHealthRefresh: Effect.Effect<void>;
  };

export type ViewServerRuntimePreparedSource<
  Topics extends ViewServerRuntimeTopicDefinitions,
  SourceError,
> = ViewServerRuntimeSourceClients<Topics> & {
  readonly start: Effect.Effect<void, SourceError, Scope.Scope>;
};

export type ViewServerRuntimeSourceOwnership<SourceError> = {
  readonly topic: string;
  readonly owner: string;
  readonly conflict: (existingOwner: string) => SourceError;
};

export type ViewServerRuntimeSourceModule<
  Topics extends ViewServerRuntimeTopicDefinitions,
  SourceError,
> = {
  readonly healthOverlay: (
    health: ViewServerHealth<Topics>,
    nowMillis: number,
  ) => ViewServerHealth<Topics>;
  readonly ownedTopics: ReadonlyArray<ViewServerRuntimeSourceOwnership<SourceError>>;
  readonly prepare: (
    input: ViewServerRuntimeSourcePrepareInput<Topics>,
  ) => Effect.Effect<
    ViewServerRuntimePreparedSource<Topics, SourceError>,
    SourceError,
    Scope.Scope
  >;
};

export type ViewServerRuntimeSourceAdapter<
  Topics extends ViewServerRuntimeTopicDefinitions,
  SourceError = never,
> = {
  readonly make: <
    const Regions extends RuntimeRegions,
    const GrpcClients extends GrpcRuntimeClients,
  >(
    config: ViewServerConfig<Topics, Regions, GrpcClients>,
    options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  ) => Effect.Effect<
    ViewServerRuntimeSourceModule<Topics, SourceError> | undefined,
    Config.ConfigError | SourceError
  >;
};

export const validateRuntimeSourceOwnership = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  SourceError,
>(
  modules: ReadonlyArray<ViewServerRuntimeSourceModule<Topics, SourceError>>,
): Effect.Effect<void, SourceError> => {
  const owners = new Map<string, { readonly moduleIndex: number; readonly owner: string }>();
  for (const [moduleIndex, module] of modules.entries()) {
    for (const ownership of module.ownedTopics) {
      const existing = owners.get(ownership.topic);
      if (existing === undefined) {
        owners.set(ownership.topic, { moduleIndex, owner: ownership.owner });
      } else if (existing.moduleIndex !== moduleIndex) {
        return Effect.fail(ownership.conflict(existing.owner));
      }
    }
  }
  return Effect.void;
};
