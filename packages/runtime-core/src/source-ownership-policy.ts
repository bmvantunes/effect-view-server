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
import {
  makeTopicSourceBindings,
  type TopicGrpcSourceLifecycle,
  type TopicSourceOwner,
} from "./source-binding-resolution";

export type SourceOwnershipAccessProfile = "managedRuntime" | "runtimeCore";
export type SourceOwnershipGrpcLifecycle = TopicGrpcSourceLifecycle;
export type SourceOwnershipOwner = TopicSourceOwner;

export type SourceOwnershipTopic = {
  readonly sourceLeased: boolean;
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

export type SourceOwnershipPolicy = {
  readonly leasedTopics: ReadonlySet<string>;
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
  readonly publicSubscriptionDecision: (
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
  readonly requirePublicSubscriptionAllowed: (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly requirePublicResetAllowed: (
    profile: SourceOwnershipAccessProfile,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly isGrpcLeasedTopic: (topic: string) => boolean;
  readonly isLeasedTopic: (topic: string) => boolean;
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
  const leasedTopics = new Set<string>();
  const sourceOwnedTopics = new Set<string>();
  const topics = new Map<string, SourceOwnershipTopic>();
  for (const [topic, binding] of makeTopicSourceBindings(config)) {
    const ownership = {
      sourceLeased: binding.sourceLeased,
      grpcLeased: binding.grpcLeased,
      owners: binding.owners,
      sourceOwned: binding.sourceOwned,
      topic: binding.topic,
    };
    topics.set(topic, ownership);
    if (ownership.sourceOwned) {
      sourceOwnedTopics.add(topic);
    }
    if (ownership.grpcLeased) {
      grpcLeasedTopics.add(topic);
    }
    if (ownership.grpcLeased || ownership.sourceLeased) {
      leasedTopics.add(topic);
    }
  }
  const sortedLeasedTopics = new Set([...leasedTopics].sort());
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
    sortedLeasedTopics.has(topic)
      ? rejectedDecision(leasedRuntimeAccessErrorFor(topic, profile))
      : allowedDecision;
  const publicSubscriptionDecision = (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ): SourceOwnershipDecision => {
    const ownership = sortedTopics.get(topic);
    return ownership?.sourceLeased === true ||
      (profile === "managedRuntime" && ownership?.grpcLeased === true)
      ? allowedDecision
      : publicReadDecision(topic, profile);
  };
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
    leasedTopics: sortedLeasedTopics,
    sourceOwnedTopics: sortedSourceOwnedTopics,
    topics: sortedTopics,
    publicMutationDecision,
    publicReadDecision,
    publicSubscriptionDecision,
    publicResetDecision,
    requirePublicMutationAllowed: (topic, profile) =>
      decisionEffect(publicMutationDecision(topic, profile)),
    requirePublicReadAllowed: (topic, profile) =>
      decisionEffect(publicReadDecision(topic, profile)),
    requirePublicSubscriptionAllowed: (topic, profile) =>
      decisionEffect(publicSubscriptionDecision(topic, profile)),
    requirePublicResetAllowed: (profile) => decisionEffect(publicResetDecision(profile)),
    isGrpcLeasedTopic: (topic) => sortedGrpcLeasedTopics.has(topic),
    isLeasedTopic: (topic) => sortedLeasedTopics.has(topic),
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
