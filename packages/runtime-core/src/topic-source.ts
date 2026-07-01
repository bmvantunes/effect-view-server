import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ViewServerConfig } from "@effect-view-server/config";

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const topicGrpcSource = (definition: object): unknown => {
  if (hasDefinedOwnProperty(definition, "grpcSource")) {
    return Reflect.get(definition, "grpcSource");
  }
  if (hasDefinedOwnProperty(definition, "source")) {
    return Reflect.get(definition, "source");
  }
  return undefined;
};

const isGrpcLeasedSource = (source: unknown): boolean =>
  typeof source === "object" &&
  source !== null &&
  Reflect.get(source, "kind") === "grpc" &&
  Reflect.get(source, "lifecycle") === "leased";

export const grpcLeasedSourceTopics = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
): Set<string> => {
  const topics = new Set<string>();
  for (const [topic, definition] of Object.entries(config.topics)) {
    const source = topicGrpcSource(definition);
    if (isGrpcLeasedSource(source)) {
      topics.add(topic);
    }
  }
  return topics;
};

export const sourceOwnedTopics = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
): Set<string> => {
  const topics = new Set<string>();
  for (const [topic, definition] of Object.entries(config.topics)) {
    if (
      hasDefinedOwnProperty(definition, "kafkaSource") ||
      hasDefinedOwnProperty(definition, "grpcSource") ||
      hasDefinedOwnProperty(definition, "source")
    ) {
      topics.add(topic);
    }
  }
  return topics;
};
