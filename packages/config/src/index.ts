import { isSourceDefinition } from "@effect-view-server/source-adapter/definition";
import type {
  SourceDefinitionAny,
  SourceDefinitionLifecycle,
  SourceDefinitionRow,
  SourceDefinitionRouteFields,
} from "@effect-view-server/source-adapter";
import { Schema } from "effect";
import {
  isViewServerRowSchema,
  snapshotViewServerTopics,
  viewServerRowSchemaFieldsMatchAst,
} from "./config-ownership";
import { type ViewServerSystemTopicName, viewServerTopicNameIsReserved } from "./health-contract";
import type { RejectExtraKeys } from "./query-exact";
import type { RouteFieldKey } from "./query-filter";
import type { RowFromSchema, RowSchema } from "./topic-contract";
import { viewServerRouteFieldSchemaHasCompleteScalarDomain } from "./route-field-contract";
import { viewServerUnsupportedRuntimeFieldDomain } from "./schema-field-metadata";
import { isViewServerIdSchema, ViewServerId } from "./view-server-id";

export { viewSchema } from "./view-schema";
export { ViewServerId, type ViewServerIdSchema } from "./view-server-id";

export type {
  Aggregate,
  AggregateKind,
  AggregateOrderByField,
  Aggregates,
  AverageAggregate,
  BlankCondition,
  ComparableAggregate,
  CountAggregate,
  CountDistinctAggregate,
  EqualsCondition,
  FalseExpression,
  ExactGroupedQuery,
  ExactLiveQuery,
  ExactLiveQueryInput,
  ExactPatch,
  ExactRawQuery,
  FieldCondition,
  FieldConditionForPath,
  FieldKey,
  FilterExpression,
  FilterGroup,
  FilterableFieldPath,
  FilterableFieldValue,
  FilterableScalar,
  GroupedOrderBy,
  GroupedQuery,
  GroupedResult,
  InCondition,
  InRangeCondition,
  LiveQuery,
  LiveQueryResult,
  LiveQueryRow,
  NegationExpression,
  NotEqualCondition,
  NumericComparisonCondition,
  NumericFieldKey,
  OrderBy,
  OrderByField,
  PickRawFields,
  RawQuery,
  RouteFieldKey,
  RouteFieldValue,
  RowFromSchema,
  RowSchema,
  SchemaType,
  Simplify,
  SortDirection,
  StringFieldKey,
  SumAggregate,
  TextMatchingOptions,
  TextSearchCondition,
  TopicDefinition,
  TopicDefinitions,
  TopicName,
  TopicRow,
  TopicSchema,
  ValidateLiveQuery,
  Where,
} from "./topic-contract";
export type {
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
  ViewServerSourceHealth,
  ViewServerHealthTopicRow,
} from "./health-contract";
export {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerHealthSummaryFromHealth,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
  viewServerReservedTopicNames,
  viewServerTopicNameIsReserved,
} from "./health-contract";
export type { ViewServerSystemTopicName } from "./health-contract";
export type {
  RuntimeEnvironmentConfig,
  ViewServerBackpressureError,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "./runtime-contract";
export type { ValidatedRuntimeQuery } from "./validated-runtime-query";
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
export type {
  ExactLeasedRouteQuery,
  ExactLiveQueryInputForTopic,
  TopicRouteBy,
} from "./source-query-contract";
export { validateLiveQuerySourceRoute } from "./source-query-contract";
export type { SourceDefinitionAny } from "./source-contract";

type ViewServerTopicShape = {
  readonly schema: RowSchema;
  readonly source?: SourceDefinitionAny | undefined;
};

export type ViewServerConfigTopicShape = Record<string, ViewServerTopicShape>;
export type ViewServerConfigTopicInputShape = Record<string, ViewServerTopicShape>;
export type NormalizeViewServerTopicDefinitions<Topics> = Topics;

export type ViewServerTopicConfig<Topics extends ViewServerConfigTopicShape> = {
  readonly topics: Topics;
};

type NormalizeRowMutability<Value> = Value extends (...arguments_: never[]) => unknown
  ? Value
  : Value extends object
    ? {
        -readonly [Key in keyof Value]: NormalizeRowMutability<Value[Key]>;
      }
    : Value;

type TypeEquals<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? (<T>() => T extends B ? 1 : 2) extends <T>() => T extends A ? 1 : 2
      ? true
      : false
    : false;

type HasCanonicalId<SchemaValue extends RowSchema> = SchemaValue extends {
  readonly fields: {
    readonly id: infer Id;
  };
}
  ? TypeEquals<Id, typeof ViewServerId>
  : false;

type ViewServerConfigValidationError<Topic extends PropertyKey, Reason extends string, Details> = {
  readonly __viewServerConfigError: {
    readonly __invalid: never;
    readonly topic: Topic;
    readonly reason: Reason;
    readonly details: Details;
  };
};

type RowFieldDifference<ExpectedRow extends object, ReceivedRow extends object> = {
  readonly [Field in keyof ExpectedRow | keyof ReceivedRow]: Field extends keyof ExpectedRow
    ? Field extends keyof ReceivedRow
      ? TypeEquals<
          NormalizeRowMutability<ExpectedRow[Field]>,
          NormalizeRowMutability<ReceivedRow[Field]>
        > extends true
        ? never
        : {
            readonly field: Field;
            readonly expected: ExpectedRow[Field];
            readonly received: ReceivedRow[Field];
          }
      : {
          readonly field: Field;
          readonly expected: ExpectedRow[Field];
          readonly received: "missing";
        }
    : Field extends keyof ReceivedRow
      ? {
          readonly field: Field;
          readonly expected: "absent";
          readonly received: ReceivedRow[Field];
        }
      : never;
}[keyof ExpectedRow | keyof ReceivedRow];

type CanonicalIdDetails<SchemaValue extends RowSchema> = SchemaValue extends {
  readonly fields: {
    readonly id: infer Id;
  };
}
  ? {
      readonly field: "id";
      readonly expected: typeof ViewServerId;
      readonly received: Id;
    }
  : {
      readonly field: "id";
      readonly expected: typeof ViewServerId;
      readonly received: "missing";
    };

type ValidateSourceRoute<Topic extends PropertyKey, Row, Source extends SourceDefinitionAny> =
  SourceDefinitionLifecycle<Source> extends "leased"
    ? Exclude<
        SourceDefinitionRouteFields<Source>[number],
        RouteFieldKey<Row>
      > extends infer InvalidRoute
      ? [InvalidRoute] extends [never]
        ? Source
        : Source &
            ViewServerConfigValidationError<
              Topic,
              "leased source routeBy field is not a scalar topic row field",
              { readonly field: InvalidRoute }
            >
      : never
    : Source;

type ValidateSource<
  Topic extends PropertyKey,
  Row extends object,
  Source,
> = Source extends SourceDefinitionAny
  ? TypeEquals<SourceDefinitionRow<Source>, object> extends true
    ? ValidateSourceRoute<Topic, Row, Source>
    : [SourceDefinitionRow<Source>] extends [never]
      ? Source &
          ViewServerConfigValidationError<
            Topic,
            "source row type must not be any or unknown",
            { readonly received: SourceDefinitionRow<Source> }
          >
      : TypeEquals<
            NormalizeRowMutability<SourceDefinitionRow<Source>>,
            NormalizeRowMutability<Row>
          > extends true
        ? ValidateSourceRoute<Topic, Row, Source>
        : Source &
            ViewServerConfigValidationError<
              Topic,
              "source row does not match topic schema row",
              RowFieldDifference<Row, SourceDefinitionRow<Source>>
            >
  : Source &
      ViewServerConfigValidationError<
        Topic,
        "source must be created by SourceAdapter.make(...)",
        { readonly received: Source }
      >;

type ValidateTopic<TopicName extends PropertyKey, Topic> = Topic extends {
  readonly schema: infer TopicSchema extends RowSchema;
}
  ? HasCanonicalId<TopicSchema> extends true
    ? Topic extends { readonly source: infer Source }
      ? Topic &
          RejectExtraKeys<Topic, ViewServerTopicShape> & {
            readonly schema: TopicSchema;
            readonly source: ValidateSource<TopicName, RowFromSchema<TopicSchema>, Source>;
          }
      : Topic &
          RejectExtraKeys<Topic, ViewServerTopicShape> & {
            readonly schema: TopicSchema;
          }
    : Topic &
        ViewServerConfigValidationError<
          TopicName,
          "topic schema must define id as ViewServerId",
          CanonicalIdDetails<TopicSchema>
        >
  : Topic &
      ViewServerConfigValidationError<
        TopicName,
        "topic schema must expose concrete struct fields",
        { readonly received: Topic }
      >;

type ValidateTopicDefinitions<Topics extends ViewServerConfigTopicInputShape> = {
  readonly [Topic in keyof Topics]: Topic extends ViewServerSystemTopicName
    ? Topics[Topic] &
        ViewServerConfigValidationError<
          Topic,
          "topic name is reserved for system health streams",
          { readonly received: Topic }
        >
    : ValidateTopic<Topic, Topics[Topic]>;
};

type ViewServerConfigTopicsAreValid<Topics extends ViewServerConfigTopicShape> =
  string extends keyof Topics
    ? true
    : [Topics] extends [ValidateTopicDefinitions<Topics>]
      ? true
      : false;

export type ViewServerConfig<Topics extends ViewServerConfigTopicShape> =
  ViewServerConfigTopicsAreValid<Topics> extends true
    ? {
        readonly topics: Topics;
      }
    : never;

export type DefineViewServerConfigInput<Topics extends ViewServerConfigTopicInputShape> = {
  readonly topics: Topics & ValidateTopicDefinitions<Topics>;
};

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const ownPropertyNamesAreExact = (
  value: object,
  allowed: ReadonlySet<PropertyKey>,
): PropertyKey | undefined => Reflect.ownKeys(value).find((property) => !allowed.has(property));

const validateLeasedSourceRouteFields = (
  topic: string,
  source: SourceDefinitionAny | undefined,
  schema: RowSchema,
): void => {
  if (source === undefined || source.lifecycle !== "leased") {
    return;
  }
  for (const field of source.routeBy) {
    const fieldSchema = Object.hasOwn(schema.fields, field) ? schema.fields[field] : undefined;
    if (!viewServerRouteFieldSchemaHasCompleteScalarDomain(fieldSchema)) {
      throw new Error(
        `View Server topic ${topic} leased source route field ${field} must have a complete supported scalar schema domain.`,
      );
    }
  }
};

export function defineViewServerConfig<const Topics extends ViewServerConfigTopicInputShape>(
  input: DefineViewServerConfigInput<Topics>,
): ViewServerConfig<Topics>;
export function defineViewServerConfig(input: {
  readonly topics: ViewServerConfigTopicInputShape;
}) {
  const unsupportedConfigProperty = ownPropertyNamesAreExact(input, new Set(["topics"]));
  if (unsupportedConfigProperty !== undefined) {
    throw new Error(
      `View Server config contains unsupported property: ${String(unsupportedConfigProperty)}.`,
    );
  }
  const topics = snapshotViewServerTopics(input.topics);
  for (const topic of Object.keys(topics)) {
    if (viewServerTopicNameIsReserved(topic)) {
      throw new Error(`View Server topic name is reserved for system health streams: ${topic}`);
    }
    const topicDefinition = topics[topic]!;
    const unsupportedTopicProperty = ownPropertyNamesAreExact(
      topicDefinition,
      new Set(["schema", "source"]),
    );
    if (unsupportedTopicProperty !== undefined) {
      throw new Error(
        `View Server topic ${topic} contains unsupported property: ${String(unsupportedTopicProperty)}.`,
      );
    }
    const schema = topicDefinition.schema;
    if (!isViewServerRowSchema(schema)) {
      throw new Error(`View Server topic ${topic} row schema must be an Effect Schema Struct.`);
    }
    for (const field of Object.keys(schema.fields)) {
      if (
        field === "__proto__" ||
        field === "prototype" ||
        field === "constructor" ||
        field.includes(".")
      ) {
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
    if (!isViewServerIdSchema(schema.fields["id"])) {
      throw new Error(
        `View Server topic ${topic} row schema must define canonical id as ViewServerId.`,
      );
    }
    const source = hasDefinedOwnProperty(topicDefinition, "source")
      ? Reflect.get(topicDefinition, "source")
      : undefined;
    if (source !== undefined && !isSourceDefinition(source)) {
      throw new Error(
        `View Server topic ${topic} source must be created by SourceAdapter.make(...).`,
      );
    }
    validateLeasedSourceRouteFields(topic, source, schema);
  }
  return Object.freeze({ topics });
}
