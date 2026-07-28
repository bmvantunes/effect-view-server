import type {
  TopicDefinitions,
  ViewServerRuntimeError,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import { Effect } from "effect";
import {
  sourceLeasedRuntimeReadError,
  sourceOwnedRuntimeMutationError,
  sourceOwnedRuntimeResetError,
} from "./runtime-error";
import { makeTopicSourceBindings, type TopicSourceOwner } from "./source-binding-resolution";

export type SourceOwnershipAccessProfile = "managedRuntime" | "runtimeCore";
export type SourceOwnershipOwner = TopicSourceOwner;

export type SourceOwnershipTopic = {
  readonly sourceLeased: boolean;
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

export type SourceOwnershipPolicy = {
  readonly leasedTopics: ReadonlySet<string>;
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
  readonly isLeasedTopic: (topic: string) => boolean;
  readonly isSourceOwnedTopic: (topic: string) => boolean;
  readonly hasSourceOwnedTopics: boolean;
};

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

export const makeSourceOwnershipPolicy = <const Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
): SourceOwnershipPolicy => {
  const leasedTopics = new Set<string>();
  const sourceOwnedTopics = new Set<string>();
  const topics = new Map<string, SourceOwnershipTopic>();
  for (const [topic, binding] of makeTopicSourceBindings(config)) {
    const ownership = {
      sourceLeased: binding.sourceLeased,
      owners: binding.owners,
      sourceOwned: binding.sourceOwned,
      topic: binding.topic,
    };
    topics.set(topic, ownership);
    if (ownership.sourceOwned) {
      sourceOwnedTopics.add(topic);
    }
    if (ownership.sourceLeased) {
      leasedTopics.add(topic);
    }
  }
  const sortedLeasedTopics = new Set([...leasedTopics].sort());
  const sortedSourceOwnedTopics = new Set([...sourceOwnedTopics].sort());
  const sortedTopics = new Map(
    [...topics.entries()].sort(([left], [right]) => left.localeCompare(right)),
  );
  const publicMutationDecision = (
    topic: string,
    _profile: SourceOwnershipAccessProfile,
  ): SourceOwnershipDecision =>
    sortedSourceOwnedTopics.has(topic)
      ? rejectedDecision(sourceOwnedRuntimeMutationError(topic))
      : allowedDecision;
  const publicReadDecision = (
    topic: string,
    _profile: SourceOwnershipAccessProfile,
  ): SourceOwnershipDecision =>
    sortedLeasedTopics.has(topic)
      ? rejectedDecision(sourceLeasedRuntimeReadError(topic))
      : allowedDecision;
  const publicSubscriptionDecision = (
    topic: string,
    profile: SourceOwnershipAccessProfile,
  ): SourceOwnershipDecision =>
    sortedLeasedTopics.has(topic) ? allowedDecision : publicReadDecision(topic, profile);
  const publicResetDecision = (_profile: SourceOwnershipAccessProfile): SourceOwnershipDecision =>
    sortedSourceOwnedTopics.size === 0
      ? allowedDecision
      : rejectedDecision(sourceOwnedRuntimeResetError);
  return {
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
    isLeasedTopic: (topic) => sortedLeasedTopics.has(topic),
    isSourceOwnedTopic: (topic) => sortedSourceOwnedTopics.has(topic),
    hasSourceOwnedTopics: sortedSourceOwnedTopics.size > 0,
  };
};
