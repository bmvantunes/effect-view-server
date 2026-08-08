import type {
  TopicDefinitions,
  ViewServerTopicConfig,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { Effect } from "effect";
import { makeViewServerRuntimeCoreInternalWithConstructionOptions } from "./runtime-core-construction";
import type {
  ViewServerRuntimeCoreInternalInstance,
  ViewServerRuntimeCoreInternalOptionsFor,
} from "./runtime-core-types";
import type { ViewServerSourceRequirements } from "./source-runtime";
export { makeSourceOwnershipPolicy } from "./source-ownership-policy";
export { makeTopicSourceBindings } from "./source-binding-resolution";
export { makeRuntimeCoreMutationPipeline } from "./source-mutation-pipeline";
export { engineQueryWithoutRoute } from "./engine-query";
export { adaptRuntimeQuerySubscriber } from "./runtime-query-subscriber";
export type {
  TopicDefinitionHasRequiredDefinedObjectProperty,
  TopicDefinitionHasSourceOwner,
  TopicSourceBinding,
  TopicSourceOwner,
} from "./source-binding-resolution";
export type {
  RuntimeCoreDecodedRowWithStorageKey,
  RuntimeCoreMutationPipeline,
  ViewServerRuntimeCoreCheckedMutations,
  ViewServerRuntimeCoreInternalMutations,
} from "./source-mutation-pipeline";
export type {
  SourceOwnershipDecision,
  SourceOwnershipOwner,
  SourceOwnershipPolicy,
  SourceOwnershipTopic,
} from "./source-ownership-policy";

export type {
  ViewServerRuntimeCoreInternalLiveClient,
  ViewServerRuntimeCoreQueryPartition,
  ViewServerRuntimeCoreTerminalObserver,
} from "./live-client-contract";
export type { ViewServerRuntimeCoreProtocolQuerySubscriber } from "./protocol-query-subscriber";
export type { ViewServerRuntimeCoreInternalClient } from "./runtime-client";
export type {
  ViewServerRuntimeCoreInternalInstance,
  ViewServerRuntimeCoreInternalOptionsFor,
} from "./runtime-core-types";
export type { ViewServerSourceRequirements } from "./source-runtime";
export type { RuntimeSourceReportingSnapshot } from "./source-reporting";

export const makeViewServerRuntimeCoreInternal: <const Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerRuntimeCoreInternalOptionsFor<Topics>,
) => Effect.Effect<
  ViewServerRuntimeCoreInternalInstance<Topics>,
  ViewServerRuntimeError,
  ViewServerSourceRequirements<Topics>
> = (config, input) => makeViewServerRuntimeCoreInternalWithConstructionOptions(config, input);
