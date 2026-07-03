import { type ViewServerSystemTopicName, viewServerTopicNameIsReserved } from "./health-contract";
import {
  type ExactRuntimeOptions,
  type RuntimeRegions,
  type ValidateKafkaTopicSource,
  type RuntimeOptionsCandidate,
  type RuntimeOptionsDefinition,
} from "./kafka-contract";
import type {
  GrpcFeedHelper,
  GrpcMaterializedTopicSource,
  GrpcLeasedTopicSource,
  GrpcRuntimeClients,
} from "./grpc-contract";
import { defineGrpcFeed } from "./grpc-contract";
import type { RejectExtraKeys } from "./query-exact";
import type { TopicSourceDefinition } from "./source-contract";
import type {
  FieldKey,
  RowFromSchema,
  RowSchema,
  StringFieldKey,
  TopicDefinition,
  TopicDefinitions,
} from "./topic-contract";
import { viewServerUnsupportedRuntimeFieldDomain } from "./schema-field-metadata";

export type {
  Aggregate,
  AggregateOrderByField,
  Aggregates,
  AggregateKind,
  AverageAggregate,
  ComparableAggregate,
  CountAggregate,
  CountDistinctAggregate,
  EqualityFilter,
  ExactGroupedQuery,
  ExactLiveQuery,
  ExactLiveQueryInput,
  ExactPatch,
  ExactRawQuery,
  FieldFilter,
  FieldKey,
  GroupedOrderBy,
  GroupedQuery,
  GroupedResult,
  LiveQuery,
  LiveQueryResult,
  LiveQueryRow,
  NumericFieldKey,
  OrderBy,
  OrderByField,
  PickRawFields,
  RangeFilter,
  RawQuery,
  RowFromSchema,
  RowSchema,
  SchemaType,
  SortDirection,
  StringFieldKey,
  StringFilter,
  SumAggregate,
  TopicDefinition,
  TopicDefinitions,
  TopicName,
  TopicRow,
  TopicSchema,
  ValidateLiveQuery,
  Where,
} from "./topic-contract";
export type {
  GrpcClientHealth,
  GrpcClientStatus,
  GrpcFeedHealth,
  GrpcFeedLifecycle,
  GrpcFeedStatus,
  GrpcRuntimeHealth,
  GrpcTopicFeedsHealth,
  KafkaRegionHealth,
  KafkaRegionStatus,
  KafkaStartFromHealth,
  KafkaTopicHealth,
  KafkaTopicRegionHealth,
  KafkaTopicStatus,
  RuntimeStatus,
  TopicHealthStatus,
  TopicRuntimeHealth,
  TransportHealth,
  ViewServerHealth,
  ViewServerHealthConnectionStatus,
  ViewServerHealthDetails,
  ViewServerHealthStatus,
  ViewServerHealthSummary,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
} from "./health-contract";
export {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerReservedTopicNames,
  viewServerTopicNameIsReserved,
  viewServerHealthSummaryFromHealth,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
} from "./health-contract";
export type { ViewServerSystemTopicName } from "./health-contract";
export type {
  ViewServerBackpressureError,
  RuntimeEnvironmentConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "./runtime-contract";
export {
  viewServerSchemaFieldMetadata,
  viewServerUnsupportedRuntimeFieldDomain,
  type ViewServerSchemaFieldMetadata,
} from "./schema-field-metadata";
export type {
  DeltaEvent,
  DeltaOperation,
  LiveSubscription,
  LiveTransportAdapter,
  SnapshotEvent,
  StatusEvent,
  StatusEventCode,
} from "./live-protocol";
export { decodeKafkaCodec, kafka, kafkaErrorIsMapping } from "./kafka";
export { grpc } from "./grpc-contract";
export type {
  AnyGrpcLeasedFeedDefinition,
  AnyGrpcMaterializedFeedDefinition,
  GrpcConnectClientDefinition,
  GrpcClientDefinitionService,
  GrpcClientValue,
  GrpcFeedAcquireInput,
  GrpcFeedDefinition,
  GrpcFeedHelper,
  GrpcFeedMapInput,
  GrpcFeedReleaseInput,
  GrpcFeedSession,
  GrpcHelper,
  GrpcLeasedTopic,
  GrpcLeasedFeedDefinition,
  GrpcLeasedTopicSource,
  GrpcMaterializedFeedDefinition,
  GrpcMaterializedTopic,
  GrpcMaterializedTopicSource,
  GrpcMethodRequest,
  GrpcMethodValue,
  GrpcRuntimeClients,
  GrpcRuntimeValue,
  GrpcServerStreamingMethodName,
  GrpcTopicSource,
  GrpcTopicSourceLifecycle,
} from "./grpc-contract";
export { defineGrpcFeed } from "./grpc-contract";
export type {
  ExactLeasedRouteQuery,
  ExactLiveQueryInputForTopic,
  TopicRouteBy,
} from "./source-query-contract";
export { validateLiveQuerySourceRoute } from "./source-query-contract";
export type { TopicSourceDefinition } from "./source-contract";
export type {
  KafkaBytesCodec,
  ExactRuntimeOptions,
  KafkaCodec,
  KafkaCodecDecodeInput,
  KafkaCodecError,
  KafkaCodecType,
  KafkaCustomCodec,
  KafkaDecodeError,
  KafkaJsonCodec,
  KafkaMappingError,
  KafkaMessageMetadata,
  KafkaProtobufCodec,
  KafkaSourceCodec,
  KafkaStringCodec,
  KafkaProtobufType,
  KafkaTopicSourceDefinition,
  KafkaTopicSourceMapInput,
  NonEmptyReadonlyArray,
  RuntimeOptions,
  RuntimeOptionsCandidate,
  RuntimeOptionsDefinition,
  RuntimeRegions,
  RuntimeValue,
  ValidateKafkaTopicSource,
  ValidateRuntimeOptions,
  ViewServerKafkaCommittedStartFrom,
  ViewServerKafkaStartFrom,
} from "./kafka";

type ViewServerConfigTopicShape = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key: string;
    readonly kafkaSource?: object | undefined;
    readonly grpcSource?: TopicSourceDefinition | undefined;
    readonly source?: never;
  }
>;

type ViewServerConfigBody<
  Topics extends ViewServerConfigTopicShape,
  KafkaRegions extends RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients,
> = {
  readonly kafka?: KafkaRegions;
  readonly grpc?: {
    readonly clients: GrpcClients;
  };
  readonly topics: Topics;
  readonly defineRuntimeOptions: <const Options extends RuntimeOptionsCandidate>(
    options: ExactRuntimeOptions<Topics, KafkaRegions, Options>,
  ) => RuntimeOptionsDefinition<Topics, KafkaRegions, Options>;
  readonly grpcFeed: <const Clients extends GrpcRuntimeClients = GrpcClients>() => GrpcFeedHelper<
    Topics,
    Clients
  >;
};

export type ViewServerConfig<
  Topics extends ViewServerConfigTopicShape,
  KafkaRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> =
  ViewServerConfigTopicsAreValid<Topics, KafkaRegions, GrpcClients> extends true
    ? ViewServerConfigBody<Topics, KafkaRegions, GrpcClients>
    : never;

type ViewServerConfigTopicsAreValid<
  Topics extends ViewServerConfigTopicShape,
  KafkaRegions extends RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients,
> = string extends keyof Topics
  ? true
  : [Topics] extends [ValidateTopicDefinitions<Topics, KafkaRegions, GrpcClients>]
    ? true
    : false;

type TopicHasSource<Topic, Key extends "kafkaSource" | "grpcSource"> = Key extends keyof Topic
  ? undefined extends Topic[Key]
    ? false
    : true
  : false;

type TopicSourceConflict<Topic> =
  TopicHasSource<Topic, "kafkaSource"> extends true
    ? TopicHasSource<Topic, "grpcSource"> extends true
      ? never
      : unknown
    : unknown;

type ValidateTopicDefinitions<
  Topics extends TopicDefinitions,
  KafkaRegions extends RuntimeRegions = RuntimeRegions,
  _GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly [Topic in keyof Topics]: Topic extends ViewServerSystemTopicName
    ? never
    : Topics[Topic] extends {
          readonly schema: infer S extends RowSchema;
          readonly key: infer Key extends string;
        }
      ? TopicSourceConflict<Topics[Topic]> extends never
        ? never
        : Topics[Topic] extends { readonly kafkaSource: infer KafkaSource }
          ? TopicDefinition<S, Key & StringFieldKey<RowFromSchema<S>>> & {
              readonly kafkaSource: ValidateKafkaTopicSource<
                Topics,
                KafkaRegions,
                Extract<Topic, string>,
                Key,
                KafkaSource
              >;
            }
          : Topics[Topic] extends { readonly grpcSource: infer GrpcSource }
            ? TopicDefinition<S, Key & StringFieldKey<RowFromSchema<S>>> & {
                readonly grpcSource: ValidateTopicSource<RowFromSchema<S>, GrpcSource>;
              }
            : TopicDefinition<S, Key & StringFieldKey<RowFromSchema<S>>>
      : never;
};

type TopicOwnedKafkaSourceTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly kafkaSource: object;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

type RuntimeRegionsAreBroad<Regions extends RuntimeRegions> = string extends keyof Regions
  ? true
  : false;

type ConfigKafkaSourceRegionConstraint<
  Topics extends object,
  KafkaRegions extends RuntimeRegions,
> = [TopicOwnedKafkaSourceTopic<Topics>] extends [never]
  ? unknown
  : RuntimeRegionsAreBroad<KafkaRegions> extends true
    ? never
    : unknown;

type ValidateGrpcLeasedRouteBy<Row, Source> =
  Source extends GrpcLeasedTopicSource<infer RouteBy>
    ? GrpcLeasedTopicSource<{
        readonly [Index in keyof RouteBy]: RouteBy[Index] extends FieldKey<Row>
          ? RouteBy[Index]
          : never;
      }>
    : Source;

type ValidateTopicSource<Row, Source> = Source extends GrpcMaterializedTopicSource
  ? Source & RejectExtraKeys<Source, GrpcMaterializedTopicSource>
  : Source extends GrpcLeasedTopicSource<infer RouteBy>
    ? ValidateGrpcLeasedRouteBy<Row, Source> &
        RejectExtraKeys<Source, GrpcLeasedTopicSource<RouteBy>>
    : Source extends undefined
      ? undefined
      : never;

export type DefineViewServerConfigInput<
  Topics extends ViewServerConfigTopicShape,
  KafkaRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> =
  ConfigKafkaSourceRegionConstraint<Topics, KafkaRegions> extends never
    ? {
        readonly kafka?: KafkaRegions;
        readonly grpc?: {
          readonly clients: GrpcClients;
        };
        readonly topics: never;
      }
    : {
        readonly kafka?: KafkaRegions;
        readonly grpc?: {
          readonly clients: GrpcClients;
        };
        readonly topics: Topics & ValidateTopicDefinitions<Topics, KafkaRegions, GrpcClients>;
      };

type DefineViewServerConfigValidationArguments<
  Topics extends ViewServerConfigTopicShape,
  KafkaRegions extends RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients,
> =
  ViewServerConfigTopicsAreValid<Topics, KafkaRegions, GrpcClients> extends true
    ? ConfigKafkaSourceRegionConstraint<Topics, KafkaRegions> extends never
      ? readonly [
          invalid: {
            readonly __viewServerKafkaSourceRegionsInvalid: never;
          },
        ]
      : readonly []
    : readonly [
        invalid: {
          readonly __viewServerTopicDefinitionsInvalid: never;
        },
      ];

export function defineViewServerConfig<
  const Topics extends Record<
    string,
    {
      readonly schema: RowSchema;
      readonly key: string;
      readonly kafkaSource?: object | undefined;
      readonly grpcSource?: TopicSourceDefinition | undefined;
      readonly source?: never;
    }
  >,
  const KafkaRegions extends RuntimeRegions = RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  input: {
    readonly kafka?: KafkaRegions;
    readonly grpc?: {
      readonly clients: GrpcClients;
    };
    readonly topics: Topics;
  },
  ..._validation: DefineViewServerConfigValidationArguments<Topics, KafkaRegions, GrpcClients>
): ViewServerConfig<Topics, KafkaRegions, GrpcClients>;
export function defineViewServerConfig(
  input: {
    readonly kafka?: RuntimeRegions;
    readonly grpc?: {
      readonly clients: GrpcRuntimeClients;
    };
    readonly topics: ViewServerConfigTopicShape;
  },
  ..._validation: ReadonlyArray<unknown>
) {
  for (const topic of Object.keys(input.topics)) {
    if (viewServerTopicNameIsReserved(topic)) {
      throw new Error(`View Server topic name is reserved for system health streams: ${topic}`);
    }
    const schema = input.topics[topic]!.schema;
    for (const field of Object.keys(schema.fields)) {
      if (field === "__proto__" || field === "prototype" || field === "constructor") {
        throw new Error(`View Server topic ${topic} uses a reserved row field name: ${field}`);
      }
      const unsupportedRuntimeDomain = viewServerUnsupportedRuntimeFieldDomain(
        schema.fields[field],
      );
      if (unsupportedRuntimeDomain !== undefined) {
        throw new Error(
          `View Server topic ${topic} field ${field} uses unsupported runtime domain: ${unsupportedRuntimeDomain}`,
        );
      }
    }
    const topicDefinition = input.topics[topic]!;
    if (Object.prototype.hasOwnProperty.call(topicDefinition, "source")) {
      throw new Error(
        `View Server topic ${topic} cannot declare source; use kafkaSource or grpcSource.`,
      );
    }
    let sourceCount = 0;
    if (topicDefinition.kafkaSource !== undefined) {
      sourceCount += 1;
    }
    if (topicDefinition.grpcSource !== undefined) {
      sourceCount += 1;
    }
    if (sourceCount > 1) {
      throw new Error(
        `View Server topic ${topic} cannot declare more than one source owner: kafkaSource, grpcSource.`,
      );
    }
  }
  const config = {
    ...(input.kafka === undefined ? {} : { kafka: input.kafka }),
    ...(input.grpc === undefined ? {} : { grpc: input.grpc }),
    topics: input.topics,
    defineRuntimeOptions: <const Options extends RuntimeOptionsCandidate>(options: Options) =>
      options,
    grpcFeed: () => defineGrpcFeed(),
  };
  return config;
}
