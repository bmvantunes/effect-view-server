import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ViewServerConfig } from "@effect-view-server/config";

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
  readonly isGrpcLeasedTopic: (topic: string) => boolean;
  readonly isSourceOwnedTopic: (topic: string) => boolean;
  readonly hasSourceOwnedTopics: boolean;
};

export const makeSourceOwnershipPolicy = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
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
    isGrpcLeasedTopic: (topic) => sortedGrpcLeasedTopics.has(topic),
    isSourceOwnedTopic: (topic) => sortedSourceOwnedTopics.has(topic),
    hasSourceOwnedTopics: sortedSourceOwnedTopics.size > 0,
  };
};
