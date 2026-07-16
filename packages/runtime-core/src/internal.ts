import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ViewServerTopicConfig, ViewServerRuntimeError } from "@effect-view-server/config";
import type { Effect } from "effect";
import { makeViewServerRuntimeCoreInternalWithConstructionOptions } from "./runtime-core-construction";
import type {
  ViewServerRuntimeCoreInternalInstance,
  ViewServerRuntimeCoreInternalOptionsFor,
} from "./runtime-core-types";
export {
  collectSourceOwnershipConflicts,
  makeSourceOwnershipPolicy,
} from "./source-ownership-policy";
export {
  makeTopicSourceBindings,
  topicGrpcSourceMetadataFromUnknown,
} from "./source-binding-resolution";
export { makeRuntimeCoreMutationPipeline } from "./source-mutation-pipeline";
export type {
  TopicDefinitionHasRequiredDefinedObjectProperty,
  TopicDefinitionHasSourceOwner,
  TopicGrpcSourceLifecycle,
  TopicGrpcSourceMetadata,
  TopicGrpcSourceValidMetadata,
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
  SourceOwnershipAccessProfile,
  SourceOwnershipConflict,
  SourceOwnershipDecision,
  SourceOwnershipGrpcLifecycle,
  SourceOwnershipGrpcOptions,
  SourceOwnershipKafkaOptions,
  SourceOwnershipOwner,
  SourceOwnershipPolicy,
  SourceOwnershipTopic,
} from "./source-ownership-policy";

export type {
  ViewServerRuntimeCoreInternalLiveClient,
  ViewServerRuntimeCoreTerminalObserver,
} from "./live-client";
export type { ViewServerRuntimeCoreInternalClient } from "./runtime-client";
export type {
  ViewServerRuntimeCoreInternalInstance,
  ViewServerRuntimeCoreInternalOptionsFor,
} from "./runtime-core-types";

export const makeViewServerRuntimeCoreInternal: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerRuntimeCoreInternalOptionsFor<Topics>,
) => Effect.Effect<ViewServerRuntimeCoreInternalInstance<Topics>, ViewServerRuntimeError> = (
  config,
  input,
) => makeViewServerRuntimeCoreInternalWithConstructionOptions(config, input);
