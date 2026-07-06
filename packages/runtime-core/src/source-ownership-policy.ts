import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ViewServerRuntimeError, ViewServerTopicConfig } from "@effect-view-server/config";
import { Effect } from "effect";
import {
  leasedManagedRuntimeAccessError,
  leasedManagedRuntimeResetError,
  leasedRuntimeAccessError,
  sourceOwnedRuntimeMutationError,
  sourceOwnedRuntimeResetError,
} from "./runtime-error";

export type SourceOwnershipAccessProfile = "managedRuntime" | "runtimeCore";

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const topicGrpcSource = (definition: object): unknown => {
  if (hasDefinedOwnProperty(definition, "grpcSource")) {
    return Reflect.get(definition, "grpcSource");
  }
  return undefined;
};

const isGrpcLeasedSource = (source: unknown): boolean =>
  typeof source === "object" &&
  source !== null &&
  Reflect.get(source, "kind") === "grpc" &&
  Reflect.get(source, "lifecycle") === "leased";

export type SourceOwnershipPolicy = {
  readonly grpcLeasedTopics: ReadonlySet<string>;
  readonly sourceOwnedTopics: ReadonlySet<string>;
  readonly requirePublicMutationAllowed: (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly requirePublicReadAllowed: (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly requirePublicResetAllowed: (
    profile: SourceOwnershipAccessProfile,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly isGrpcLeasedTopic: (topic: string) => boolean;
  readonly isSourceOwnedTopic: (topic: string) => boolean;
  readonly hasSourceOwnedTopics: boolean;
};

const leasedRuntimeAccessErrorFor = (
  topic: string,
  profile: SourceOwnershipAccessProfile,
): ViewServerRuntimeError =>
  profile === "managedRuntime"
    ? leasedManagedRuntimeAccessError(topic)
    : leasedRuntimeAccessError(topic);

export const makeSourceOwnershipPolicy = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
): SourceOwnershipPolicy => {
  const grpcLeasedTopics = new Set<string>();
  const sourceOwnedTopics = new Set<string>();
  for (const [topic, definition] of Object.entries(config.topics)) {
    const grpcSource = topicGrpcSource(definition);
    if (hasDefinedOwnProperty(definition, "kafkaSource") || grpcSource !== undefined) {
      sourceOwnedTopics.add(topic);
    }
    if (isGrpcLeasedSource(grpcSource)) {
      grpcLeasedTopics.add(topic);
    }
  }
  const sortedGrpcLeasedTopics = new Set([...grpcLeasedTopics].sort());
  const sortedSourceOwnedTopics = new Set([...sourceOwnedTopics].sort());
  return {
    grpcLeasedTopics: sortedGrpcLeasedTopics,
    sourceOwnedTopics: sortedSourceOwnedTopics,
    requirePublicMutationAllowed: (topic, profile) => {
      if (!sortedSourceOwnedTopics.has(topic)) {
        return Effect.void;
      }
      return Effect.fail(
        profile === "managedRuntime" && sortedGrpcLeasedTopics.has(topic)
          ? leasedRuntimeAccessErrorFor(topic, profile)
          : sourceOwnedRuntimeMutationError(topic),
      );
    },
    requirePublicReadAllowed: (topic, profile) =>
      sortedGrpcLeasedTopics.has(topic)
        ? Effect.fail(leasedRuntimeAccessErrorFor(topic, profile))
        : Effect.void,
    requirePublicResetAllowed: (profile) => {
      if (sortedSourceOwnedTopics.size === 0) {
        return Effect.void;
      }
      return Effect.fail(
        profile === "managedRuntime" && sortedGrpcLeasedTopics.size > 0
          ? leasedManagedRuntimeResetError
          : sourceOwnedRuntimeResetError,
      );
    },
    isGrpcLeasedTopic: (topic) => sortedGrpcLeasedTopics.has(topic),
    isSourceOwnedTopic: (topic) => sortedSourceOwnedTopics.has(topic),
    hasSourceOwnedTopics: sortedSourceOwnedTopics.size > 0,
  };
};
