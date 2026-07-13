import {
  decodeKafkaTopicMessage,
  isKafkaTopicSourceDefinition,
  makeKafkaResolvedSourceTopics,
  type KafkaResolvedSourceTopicDefinition,
  type RuntimeRegions,
} from "./kafka-contract";
import type { RowSchema } from "./topic-contract";

export { validateDecodedRow } from "./decoded-row-validation";

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
export type {
  RuntimeKafkaSourceOwnershipConstraint,
  RuntimeKafkaSourceRegionConstraint,
  TopicOwnedKafkaSourceRegion,
  TopicOwnedKafkaSourceTopic,
} from "./kafka-contract";

export { grpcSourceMarkers } from "./grpc-contract";
export {
  isRawQueryFilterOperatorKey,
  isRawQueryRangeFilterOperatorKey,
  rawQueryFilterOperatorKeys,
  rawQueryRangeFilterOperatorKeys,
} from "./raw-query-filter-operators";
export { schemaAstChildren } from "./schema-ast-children";
export {
  snapshotViewServerRowSchema,
  snapshotViewServerTopics,
  viewServerRowSchemaFieldsMatchAst,
  viewServerRowSchemasShareOrigin,
} from "./config-ownership";
