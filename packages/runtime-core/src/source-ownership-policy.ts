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
export type SourceOwnershipGrpcLifecycle = "leased" | "materialized" | "unknown";
export type SourceOwnershipOwner =
  | {
      readonly _tag: "kafka";
    }
  | {
      readonly _tag: "grpc";
      readonly lifecycle: SourceOwnershipGrpcLifecycle;
    };

export type SourceOwnershipTopic = {
  readonly grpcLeased: boolean;
  readonly owners: ReadonlyArray<SourceOwnershipOwner>;
  readonly sourceOwned: boolean;
  readonly topic: string;
};

export type SourceOwnershipDecision =
  | {
      readonly _tag: "allowed";
    }
  | {
      readonly _tag: "rejected";
      readonly error: ViewServerRuntimeError;
    };

export type SourceOwnershipConflict = {
  readonly grpcFeed: string;
  readonly kafkaSource: string;
  readonly topic: string;
};

export type SourceOwnershipKafkaOptions = {
  readonly topics: Readonly<Record<string, { readonly viewServerTopic: string }>>;
};

export type SourceOwnershipGrpcOptions = {
  readonly feeds: Readonly<Record<string, { readonly topic: string }>>;
};

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const topicGrpcSource = (definition: object): unknown => {
  if (hasDefinedOwnProperty(definition, "grpcSource")) {
    return Reflect.get(definition, "grpcSource");
  }
  return undefined;
};

const grpcLifecycle = (source: unknown): SourceOwnershipGrpcLifecycle => {
  if (typeof source !== "object" || source === null || Reflect.get(source, "kind") !== "grpc") {
    return "unknown";
  }
  const lifecycle = Reflect.get(source, "lifecycle");
  if (lifecycle === "leased" || lifecycle === "materialized") {
    return lifecycle;
  }
  return "unknown";
};

const topicOwners = (definition: object): ReadonlyArray<SourceOwnershipOwner> => {
  const owners: Array<SourceOwnershipOwner> = [];
  if (hasDefinedOwnProperty(definition, "kafkaSource")) {
    owners.push({ _tag: "kafka" });
  }
  const grpcSource = topicGrpcSource(definition);
  if (grpcSource !== undefined) {
    owners.push({
      _tag: "grpc",
      lifecycle: grpcLifecycle(grpcSource),
    });
  }
  return owners;
};

const topicOwnership = (topic: string, definition: object): SourceOwnershipTopic => {
  const owners = topicOwners(definition);
  return {
    grpcLeased: owners.some((owner) => owner._tag === "grpc" && owner.lifecycle === "leased"),
    owners,
    sourceOwned: owners.length > 0,
    topic,
  };
};

export type SourceOwnershipPolicy = {
  readonly grpcLeasedTopics: ReadonlySet<string>;
  readonly sourceOwnedTopics: ReadonlySet<string>;
  readonly topics: ReadonlyMap<string, SourceOwnershipTopic>;
  readonly publicMutationDecision: (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ) => SourceOwnershipDecision;
  readonly publicReadDecision: (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ) => SourceOwnershipDecision;
  readonly publicResetDecision: (profile: SourceOwnershipAccessProfile) => SourceOwnershipDecision;
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

const allowedDecision: SourceOwnershipDecision = {
  _tag: "allowed",
};

const rejectedDecision = (error: ViewServerRuntimeError): SourceOwnershipDecision => ({
  _tag: "rejected",
  error,
});

const decisionEffect = (
  decision: SourceOwnershipDecision,
): Effect.Effect<void, ViewServerRuntimeError> =>
  decision._tag === "allowed" ? Effect.void : Effect.fail(decision.error);

export const makeSourceOwnershipPolicy = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
): SourceOwnershipPolicy => {
  const grpcLeasedTopics = new Set<string>();
  const sourceOwnedTopics = new Set<string>();
  const topics = new Map<string, SourceOwnershipTopic>();
  for (const [topic, definition] of Object.entries(config.topics)) {
    const ownership = topicOwnership(topic, definition);
    topics.set(topic, ownership);
    if (ownership.sourceOwned) {
      sourceOwnedTopics.add(topic);
    }
    if (ownership.grpcLeased) {
      grpcLeasedTopics.add(topic);
    }
  }
  const sortedGrpcLeasedTopics = new Set([...grpcLeasedTopics].sort());
  const sortedSourceOwnedTopics = new Set([...sourceOwnedTopics].sort());
  const sortedTopics = new Map(
    [...topics.entries()].sort(([left], [right]) => left.localeCompare(right)),
  );
  const publicMutationDecision = (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ): SourceOwnershipDecision => {
    if (!sortedSourceOwnedTopics.has(topic)) {
      return allowedDecision;
    }
    return rejectedDecision(
      profile === "managedRuntime" && sortedGrpcLeasedTopics.has(topic)
        ? leasedRuntimeAccessErrorFor(topic, profile)
        : sourceOwnedRuntimeMutationError(topic),
    );
  };
  const publicReadDecision = (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ): SourceOwnershipDecision =>
    sortedGrpcLeasedTopics.has(topic)
      ? rejectedDecision(leasedRuntimeAccessErrorFor(topic, profile))
      : allowedDecision;
  const publicResetDecision = (profile: SourceOwnershipAccessProfile): SourceOwnershipDecision => {
    if (sortedSourceOwnedTopics.size === 0) {
      return allowedDecision;
    }
    return rejectedDecision(
      profile === "managedRuntime" && sortedGrpcLeasedTopics.size > 0
        ? leasedManagedRuntimeResetError
        : sourceOwnedRuntimeResetError,
    );
  };
  return {
    grpcLeasedTopics: sortedGrpcLeasedTopics,
    sourceOwnedTopics: sortedSourceOwnedTopics,
    topics: sortedTopics,
    publicMutationDecision,
    publicReadDecision,
    publicResetDecision,
    requirePublicMutationAllowed: (topic, profile) =>
      decisionEffect(publicMutationDecision(topic, profile)),
    requirePublicReadAllowed: (topic, profile) =>
      decisionEffect(publicReadDecision(topic, profile)),
    requirePublicResetAllowed: (profile) => decisionEffect(publicResetDecision(profile)),
    isGrpcLeasedTopic: (topic) => sortedGrpcLeasedTopics.has(topic),
    isSourceOwnedTopic: (topic) => sortedSourceOwnedTopics.has(topic),
    hasSourceOwnedTopics: sortedSourceOwnedTopics.size > 0,
  };
};

export const collectSourceOwnershipConflicts = (
  kafkaOptions: SourceOwnershipKafkaOptions | undefined,
  grpcOptions: SourceOwnershipGrpcOptions | undefined,
): ReadonlyArray<SourceOwnershipConflict> => {
  if (kafkaOptions === undefined || grpcOptions === undefined) {
    return [];
  }
  const grpcFeedByTopic = new Map<string, string>();
  for (const [feedName, feed] of Object.entries(grpcOptions.feeds)) {
    grpcFeedByTopic.set(feed.topic, feedName);
  }
  const conflicts: Array<SourceOwnershipConflict> = [];
  for (const [sourceTopic, kafkaTopic] of Object.entries(kafkaOptions.topics)) {
    const grpcFeedName = grpcFeedByTopic.get(kafkaTopic.viewServerTopic);
    if (grpcFeedName !== undefined) {
      conflicts.push({
        grpcFeed: grpcFeedName,
        kafkaSource: sourceTopic,
        topic: kafkaTopic.viewServerTopic,
      });
    }
  }
  return conflicts;
};
