import { definedFields } from "./optional-fields";
import type { ViewServerConfig, ViewServerRuntimeError } from "@effect-view-server/config";
import {
  createViewServerRuntimeCore,
  makeViewServerRuntimeCore,
  type TopicDefinitions,
  type ViewServerRuntimeCoreInstance,
  type ViewServerRuntimeCoreOptionsFor,
  type ViewServerRuntimeCorePublicClient,
  type ViewServerRuntimeCorePublicLiveClient,
  type ViewServerSourceRequirements,
} from "@effect-view-server/runtime-core";
import { Effect } from "effect";

export type {
  TopicDefinitions,
  ViewServerSourceRequirements,
} from "@effect-view-server/runtime-core";

export type ViewServerInMemoryTopicDefinitions = TopicDefinitions;

export type ViewServerInMemoryInstance<Topics extends ViewServerInMemoryTopicDefinitions> = {
  readonly client: ViewServerRuntimeCorePublicClient<Topics>;
  readonly liveClient: ViewServerRuntimeCorePublicLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

export type ViewServerInMemoryOptions<
  Topics extends ViewServerInMemoryTopicDefinitions = ViewServerInMemoryTopicDefinitions,
> = Omit<ViewServerRuntimeCoreOptionsFor<Topics>, "transportHealth">;

type SynchronousInMemoryConfig<Topics extends ViewServerInMemoryTopicDefinitions> =
  ViewServerConfig<Topics> &
    ([ViewServerSourceRequirements<NoInfer<Topics>>] extends [never] ? unknown : never);

const toRuntimeCoreOptions = <const Topics extends ViewServerInMemoryTopicDefinitions>(
  input: ViewServerInMemoryOptions<Topics>,
): ViewServerRuntimeCoreOptionsFor<Topics> => ({
  ...definedFields(
    input.groupedIncrementalAdmissionLimits,
    (groupedIncrementalAdmissionLimits) => ({
      groupedIncrementalAdmissionLimits,
    }),
  ),
  ...definedFields(input.subscriptionQueueCapacity, (subscriptionQueueCapacity) => ({
    subscriptionQueueCapacity,
  })),
  ...definedFields(input.healthRefreshCadence, (healthRefreshCadence) => ({
    healthRefreshCadence,
  })),
});

const toInMemoryInstance = <const Topics extends ViewServerInMemoryTopicDefinitions>(
  runtimeCore: ViewServerRuntimeCoreInstance<Topics>,
): ViewServerInMemoryInstance<Topics> => {
  return {
    client: runtimeCore.client,
    close: runtimeCore.close,
    liveClient: runtimeCore.liveClient,
  };
};

export const makeInMemoryViewServer: <const Topics extends ViewServerInMemoryTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerInMemoryOptions<Topics>,
) => Effect.Effect<
  ViewServerInMemoryInstance<Topics>,
  ViewServerRuntimeError,
  ViewServerSourceRequirements<Topics>
> = Effect.fn("ViewServerInMemory.make")(
  <const Topics extends ViewServerInMemoryTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    input: ViewServerInMemoryOptions<Topics>,
  ) =>
    makeViewServerRuntimeCore(config, toRuntimeCoreOptions(input)).pipe(
      Effect.map(toInMemoryInstance),
    ),
);

export const createInMemoryViewServer = <const Topics extends ViewServerInMemoryTopicDefinitions>(
  config: SynchronousInMemoryConfig<Topics>,
  options: ViewServerInMemoryOptions<Topics> = {},
): ViewServerInMemoryInstance<Topics> =>
  toInMemoryInstance(createViewServerRuntimeCore(config, toRuntimeCoreOptions(options)));
