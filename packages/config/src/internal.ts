import {
  decodeKafkaTopicMessage,
  isKafkaTopicSourceDefinition,
  makeKafkaResolvedSourceTopics,
  type KafkaResolvedSourceTopicDefinition,
  type RuntimeRegions,
} from "./kafka-contract";
import type { RowSchema } from "./topic-contract";

type KafkaSourceTopicRegistry = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key: string;
    readonly kafkaSource?: object | undefined;
  }
>;

export const makeKafkaSourceTopicsForConfig = <
  const Topics extends KafkaSourceTopicRegistry,
  const Regions extends RuntimeRegions,
>(config: {
  readonly topics: Topics;
}): ReadonlyArray<
  KafkaResolvedSourceTopicDefinition<Topics, Regions, Extract<keyof Topics, string>>
> => makeKafkaResolvedSourceTopics<Topics, Regions>(config.topics);

export { decodeKafkaTopicMessage, isKafkaTopicSourceDefinition };
export type { KafkaResolvedSourceTopicDefinition } from "./kafka-contract";

export { defineGrpcFeed, grpcSourceMarkers } from "./grpc-contract";
export type { GrpcFeedDefinition } from "./grpc-contract";
