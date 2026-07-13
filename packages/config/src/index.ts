import { type ViewServerSystemTopicName, viewServerTopicNameIsReserved } from "./health-contract";
import {
  type ExactRuntimeOptions,
  type RuntimeRegions,
  type ValidateKafkaTopicSource,
  type RuntimeOptionsCandidate,
  type RuntimeOptionsDefinition,
} from "./kafka-contract";
import { grpcTopicSourceDefinitionKey, grpcTopicSourceDefinitionSchema } from "./grpc-contract";
import {
  isViewServerRowSchema,
  snapshotViewServerGrpcClients,
  snapshotViewServerTopics,
  viewServerRowSchemaFieldsMatchAst,
  viewServerRowSchemasShareOrigin,
} from "./config-ownership";
import type {
  GrpcLeasedTopicSourceDefinition,
  GrpcMaterializedTopicSource,
  GrpcMaterializedTopicSourceDefinition,
  GrpcLeasedTopicSource,
  GrpcRuntimeClients,
  GrpcTopicSourceHasKey,
  GrpcTopicSourceIsBoundToClients,
} from "./grpc-contract";
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
import { Schema } from "effect";
export { viewSchema } from "./view-schema";

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
  Simplify,
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
  GrpcConnectClientDefinition,
  GrpcClientDefinitionService,
  GrpcClientValue,
  GrpcFeedAcquireInput,
  GrpcFeedMapInput,
  GrpcFeedReleaseInput,
  GrpcFeedSession,
  GrpcHelper,
  GrpcLeasedTopicSource,
  GrpcLeasedTopicSourceDefinition,
  GrpcMaterializedTopicSource,
  GrpcMaterializedTopicSourceDefinition,
  GrpcMethodRequest,
  GrpcMethodValue,
  GrpcRuntimeClients,
  GrpcRuntimeValue,
  GrpcServerStreamingMethodName,
  GrpcTopicSourceHelper,
  GrpcTopicSource,
  GrpcTopicSourceLifecycle,
} from "./grpc-contract";
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

export type ViewServerConfigTopicShape = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key: string;
    readonly kafkaSource?: object | undefined;
    readonly grpcSource?: TopicSourceDefinition | undefined;
    readonly source?: never;
  }
>;

export type ViewServerTopicConfig<Topics extends ViewServerConfigTopicShape> = {
  readonly topics: Topics;
};

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
};

type NoGrpcClients = Record<never, never>;

export type ViewServerConfig<
  Topics extends ViewServerConfigTopicShape,
  KafkaRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = NoGrpcClients,
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

type TypeEquals<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? (<T>() => T extends B ? 1 : 2) extends <T>() => T extends A ? 1 : 2
      ? true
      : false
    : false;

type ValidateTopicDefinitions<
  Topics extends TopicDefinitions,
  KafkaRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = NoGrpcClients,
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
                readonly grpcSource: ValidateTopicSource<S, Key, GrpcSource, GrpcClients>;
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

type TopicOwnedConcreteGrpcSourceTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends { readonly grpcSource: infer Source }
      ? Source extends {
          readonly client: string;
          readonly method: string;
          readonly map: unknown;
        }
        ? Topic
        : never
      : never;
  }[keyof Topics],
  string
>;

type RuntimeRegionsAreBroad<Regions extends RuntimeRegions> = string extends keyof Regions
  ? true
  : false;

type SourceHasConcreteGrpcBinding<Source> = Source extends {
  readonly client: string;
  readonly method: string;
  readonly map: unknown;
}
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

type ConfigGrpcSourceClientsConstraint<
  Topics extends object,
  GrpcClients extends GrpcRuntimeClients,
> = [TopicOwnedConcreteGrpcSourceTopic<Topics>] extends [never]
  ? unknown
  : [keyof GrpcClients] extends [never]
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

type ValidateGrpcMaterializedTopicSource<
  SchemaValue extends RowSchema,
  Key extends string,
  Source,
  GrpcClients extends GrpcRuntimeClients,
> =
  SourceHasConcreteGrpcBinding<Source> extends true
    ? Source extends GrpcMaterializedTopicSourceDefinition<
        infer SourceClients,
        infer SourceSchema,
        infer SourceKey,
        infer ClientName,
        infer MethodName,
        infer Request,
        infer Mapping
      >
      ? TypeEquals<SourceSchema, SchemaValue> extends true
        ? TypeEquals<SourceKey, Key> extends true
          ? GrpcTopicSourceIsBoundToClients<Source, GrpcClients> extends true
            ? GrpcTopicSourceHasKey<Source, Key> extends true
              ? Source &
                  RejectExtraKeys<
                    Source,
                    GrpcMaterializedTopicSourceDefinition<
                      SourceClients,
                      SourceSchema,
                      SourceKey,
                      ClientName,
                      MethodName,
                      Request,
                      Mapping
                    >
                  >
              : never
            : never
          : never
        : never
      : never
    : Source extends GrpcMaterializedTopicSource
      ? Source & RejectExtraKeys<Source, GrpcMaterializedTopicSource>
      : never;

type ValidateGrpcLeasedTopicSource<
  SchemaValue extends RowSchema,
  Key extends string,
  Source,
  GrpcClients extends GrpcRuntimeClients,
> =
  SourceHasConcreteGrpcBinding<Source> extends true
    ? Source extends GrpcLeasedTopicSourceDefinition<
        infer SourceClients,
        infer SourceSchema,
        infer SourceKey,
        infer RouteBy,
        infer ClientName,
        infer MethodName,
        infer Request,
        infer Mapping
      >
      ? TypeEquals<SourceSchema, SchemaValue> extends true
        ? TypeEquals<SourceKey, Key> extends true
          ? GrpcTopicSourceIsBoundToClients<Source, GrpcClients> extends true
            ? GrpcTopicSourceHasKey<Source, Key> extends true
              ? ValidateGrpcLeasedRouteBy<RowFromSchema<SchemaValue>, Source> &
                  RejectExtraKeys<
                    Source,
                    GrpcLeasedTopicSourceDefinition<
                      SourceClients,
                      SourceSchema,
                      SourceKey,
                      RouteBy,
                      ClientName,
                      MethodName,
                      Request,
                      Mapping
                    >
                  >
              : never
            : never
          : never
        : never
      : never
    : Source extends GrpcLeasedTopicSource<infer _RouteBy>
      ? ValidateGrpcLeasedRouteBy<RowFromSchema<SchemaValue>, Source> &
          RejectExtraKeys<Source, GrpcLeasedTopicSource>
      : never;

type ValidateTopicSource<
  SchemaValue extends RowSchema,
  Key extends string,
  Source,
  GrpcClients extends GrpcRuntimeClients,
> = Source extends GrpcMaterializedTopicSource
  ? ValidateGrpcMaterializedTopicSource<SchemaValue, Key, Source, GrpcClients>
  : Source extends GrpcLeasedTopicSource<infer _RouteBy>
    ? ValidateGrpcLeasedTopicSource<SchemaValue, Key, Source, GrpcClients>
    : Source extends undefined
      ? undefined
      : never;

export type DefineViewServerConfigInput<
  Topics extends ViewServerConfigTopicShape,
  KafkaRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = NoGrpcClients,
> =
  ConfigKafkaSourceRegionConstraint<Topics, KafkaRegions> extends never
    ? {
        readonly kafka?: KafkaRegions;
        readonly grpc?: {
          readonly clients: GrpcClients;
        };
        readonly topics: never;
      }
    : ConfigGrpcSourceClientsConstraint<Topics, GrpcClients> extends never
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
      : ConfigGrpcSourceClientsConstraint<Topics, GrpcClients> extends never
        ? readonly [
            invalid: {
              readonly __viewServerGrpcSourceClientsInvalid: never;
            },
          ]
        : readonly []
    : readonly [
        invalid: {
          readonly __viewServerTopicDefinitionsInvalid: never;
        },
      ];

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const concreteGrpcBinding = (value: object): object | undefined => {
  if (!hasDefinedOwnProperty(value, "grpcSource")) {
    return undefined;
  }
  const source = Reflect.get(value, "grpcSource");
  if (
    typeof source === "object" &&
    source !== null &&
    hasDefinedOwnProperty(source, "client") &&
    hasDefinedOwnProperty(source, "method")
  ) {
    return source;
  }
  return undefined;
};

const concreteGrpcBindingKeyNames = ["client", "method", "request", "acquire", "release", "map"];

const hasConcreteGrpcBindingField = (source: object): boolean =>
  concreteGrpcBindingKeyNames.some((key) => hasDefinedOwnProperty(source, key));

const concreteGrpcBindingIsComplete = (source: object): boolean => {
  const request = Reflect.get(source, "request");
  const acquire = Reflect.get(source, "acquire");
  const release = Reflect.get(source, "release");
  const map = Reflect.get(source, "map");
  return (
    hasDefinedOwnProperty(source, "client") &&
    hasDefinedOwnProperty(source, "method") &&
    typeof request === "function" &&
    typeof acquire === "function" &&
    typeof map === "function" &&
    (release === undefined || typeof release === "function")
  );
};

const grpcMethodIsServerStreaming = (method: unknown): boolean =>
  typeof method === "object" &&
  method !== null &&
  Reflect.get(method, "methodKind") === "server_streaming";

const validateConcreteGrpcBinding = (
  topic: string,
  topicDefinition: object,
  clients: GrpcRuntimeClients | undefined,
): void => {
  if (hasDefinedOwnProperty(topicDefinition, "grpcSource")) {
    const grpcSource = Reflect.get(topicDefinition, "grpcSource");
    if (
      typeof grpcSource === "object" &&
      grpcSource !== null &&
      hasConcreteGrpcBindingField(grpcSource) &&
      !concreteGrpcBindingIsComplete(grpcSource)
    ) {
      throw new Error(`View Server topic ${topic} declares invalid gRPC source metadata.`);
    }
  }
  const source = concreteGrpcBinding(topicDefinition);
  if (source === undefined) {
    return;
  }
  if (clients === undefined) {
    throw new Error(
      `View Server topic ${topic} declares grpcSource, but defineViewServerConfig.grpc.clients was not provided.`,
    );
  }
  const clientName = Reflect.get(source, "client");
  if (
    typeof clientName !== "string" ||
    !Object.prototype.hasOwnProperty.call(clients, clientName)
  ) {
    throw new Error(
      `View Server topic ${topic} declares grpcSource client ${String(clientName)}, but defineViewServerConfig.grpc.clients does not define it.`,
    );
  }
  const methodName = Reflect.get(source, "method");
  const clientDefinition = clients[clientName];
  const method =
    typeof methodName === "string" && clientDefinition !== undefined
      ? Reflect.get(clientDefinition.service.method, methodName)
      : undefined;
  if (
    typeof methodName !== "string" ||
    clientDefinition === undefined ||
    !Object.prototype.hasOwnProperty.call(clientDefinition.service.method, methodName)
  ) {
    throw new Error(
      `View Server topic ${topic} declares grpcSource method ${String(methodName)}, but grpc client ${clientName} does not define it.`,
    );
  }
  if (!grpcMethodIsServerStreaming(method)) {
    throw new Error(
      `View Server topic ${topic} declares grpcSource method ${methodName}, but grpc client ${clientName} method is not server-streaming.`,
    );
  }
  const sourceKey = grpcTopicSourceDefinitionKey(source);
  const topicKey = Reflect.get(topicDefinition, "key");
  if (sourceKey !== undefined && sourceKey !== topicKey) {
    throw new Error(
      `View Server topic ${topic} declares grpcSource for row key ${sourceKey}, but topic key is ${String(topicKey)}.`,
    );
  }
  const sourceSchema = grpcTopicSourceDefinitionSchema(source);
  const topicSchema = Reflect.get(topicDefinition, "schema");
  if (
    sourceSchema !== undefined &&
    (!isViewServerRowSchema(topicSchema) ||
      !viewServerRowSchemasShareOrigin(sourceSchema, topicSchema))
  ) {
    throw new Error(
      `View Server topic ${topic} declares grpcSource for a different schema than the topic schema.`,
    );
  }
};

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
  const GrpcClients extends GrpcRuntimeClients = NoGrpcClients,
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
  const topics = snapshotViewServerTopics(input.topics);
  const inputKafka = input.kafka;
  const kafka = inputKafka === undefined ? undefined : Object.freeze({ ...inputKafka });
  const inputGrpc = input.grpc;
  const grpc =
    inputGrpc === undefined
      ? undefined
      : Object.freeze({
          clients: snapshotViewServerGrpcClients(inputGrpc.clients),
        });

  for (const topic of Object.keys(topics)) {
    if (viewServerTopicNameIsReserved(topic)) {
      throw new Error(`View Server topic name is reserved for system health streams: ${topic}`);
    }
    const schema = topics[topic]!.schema;
    if (!isViewServerRowSchema(schema)) {
      throw new Error(`View Server topic ${topic} row schema must be an Effect Schema Struct.`);
    }
    for (const field of Object.keys(schema.fields)) {
      if (field === "__proto__" || field === "prototype" || field === "constructor") {
        throw new Error(`View Server topic ${topic} uses a reserved row field name: ${field}`);
      }
      const fieldSchema = schema.fields[field];
      if (!Schema.isSchema(fieldSchema)) {
        throw new Error(`View Server topic ${topic} field ${field} must be an Effect Schema.`);
      }
      const unsupportedRuntimeDomain = viewServerUnsupportedRuntimeFieldDomain(fieldSchema);
      if (unsupportedRuntimeDomain !== undefined) {
        throw new Error(
          `View Server topic ${topic} field ${field} uses unsupported runtime domain: ${unsupportedRuntimeDomain}`,
        );
      }
    }
    const unsupportedRowRuntimeDomain = viewServerUnsupportedRuntimeFieldDomain(schema);
    if (unsupportedRowRuntimeDomain !== undefined) {
      throw new Error(
        `View Server topic ${topic} row schema uses unsupported runtime domain: ${unsupportedRowRuntimeDomain}`,
      );
    }
    if (!viewServerRowSchemaFieldsMatchAst(schema)) {
      throw new Error(
        `View Server topic ${topic} exposed row fields do not match the row schema AST.`,
      );
    }
    const topicDefinition = topics[topic]!;
    if (Object.prototype.hasOwnProperty.call(topicDefinition, "source")) {
      throw new Error(
        `View Server topic ${topic} cannot declare source; use kafkaSource or grpcSource.`,
      );
    }
    let sourceCount = 0;
    if (hasDefinedOwnProperty(topicDefinition, "kafkaSource")) {
      sourceCount += 1;
    }
    if (hasDefinedOwnProperty(topicDefinition, "grpcSource")) {
      sourceCount += 1;
    }
    if (sourceCount > 1) {
      throw new Error(
        `View Server topic ${topic} cannot declare more than one source owner: kafkaSource, grpcSource.`,
      );
    }
    validateConcreteGrpcBinding(topic, topicDefinition, grpc?.clients);
  }
  const config = Object.freeze({
    ...(kafka === undefined ? {} : { kafka }),
    ...(grpc === undefined ? {} : { grpc }),
    topics,
    defineRuntimeOptions: <const Options extends RuntimeOptionsCandidate>(options: Options) =>
      options,
  });
  return config;
}
