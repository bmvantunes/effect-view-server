import { isSourceDefinition } from "@effect-view-server/source-adapter/definition";
import type {
  SourceDefinition,
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
import type { ViewServerSystemTopicName } from "./health-contract";
import type { RejectExtraKeys } from "./query-exact";
import type { FilterableScalar, RouteFieldKey } from "./query-filter";
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
type ViewServerConfigTopicCandidateShape = Record<string, unknown>;
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

type IsUnion<Value, Whole = Value> = Value extends unknown
  ? [Whole] extends [Value]
    ? false
    : true
  : never;

type IsAny<Value> = 0 extends 1 & Value ? true : false;
type IsUnknown<Value> = IsAny<Value> extends true ? false : unknown extends Value ? true : false;

type SourceDefinitionDeclaredRow<Definition> =
  Definition extends SourceDefinition<
    infer _Adapter,
    infer _Lifecycle,
    infer _Options,
    infer _RouteFields,
    infer _RetryServices,
    infer Row
  >
    ? Row
    : never;

type UnsafeSourceRowDetails<Source> =
  IsAny<SourceDefinitionDeclaredRow<Source>> extends true
    ? { readonly received: "any" }
    : IsUnknown<SourceDefinitionDeclaredRow<Source>> extends true
      ? { readonly received: "unknown" }
      : { readonly received: SourceDefinitionDeclaredRow<Source> };

type UnsafeSourceRowReason<Source> =
  IsAny<SourceDefinitionDeclaredRow<Source>> extends true
    ? "source row type must not be any or unknown"
    : IsUnknown<SourceDefinitionDeclaredRow<Source>> extends true
      ? "source row type must not be any or unknown"
      : [SourceDefinitionDeclaredRow<Source>] extends [never]
        ? "source row type must not be never"
        : "source row type must not be any or unknown";

type HasCanonicalId<SchemaValue extends RowSchema> = SchemaValue extends {
  readonly fields: {
    readonly id: infer Id;
  };
}
  ? TypeEquals<Id, typeof ViewServerId>
  : false;

declare const ViewServerConfigValidationErrorTypeId: unique symbol;

type ViewServerConfigValidationError<Topic extends PropertyKey, Reason extends string, Details> = {
  readonly [ViewServerConfigValidationErrorTypeId]: never;
  readonly __viewServerConfigError: {
    readonly __invalid: never;
    readonly topic: Topic;
    readonly reason: Reason;
    readonly details: Details;
  };
};

type WithViewServerConfigValidationError<
  Value,
  Topic extends PropertyKey,
  Reason extends string,
  Details,
> = Value extends object
  ? Value & ViewServerConfigValidationError<Topic, Reason, Details>
  : ViewServerConfigValidationError<Topic, Reason, Details>;

type IsOptionalField<Row extends object, Field extends keyof Row> =
  {} extends Pick<Row, Field> ? true : false;

type FieldPresentValue<Row extends object, Field extends keyof Row> = Required<
  Pick<Row, Field>
>[Field];

type FunctionValue = Function;

type ExpectedRowFieldDifference<ExpectedRow extends object, ReceivedRow extends object> = {
  readonly [Field in keyof ExpectedRow]-?: Field extends keyof ReceivedRow
    ? TypeEquals<
        IsOptionalField<ExpectedRow, Field>,
        IsOptionalField<ReceivedRow, Field>
      > extends true
      ? TypeEquals<
          NormalizeRowMutability<FieldPresentValue<ExpectedRow, Field>>,
          NormalizeRowMutability<FieldPresentValue<ReceivedRow, Field>>
        > extends true
        ? never
        : {
            readonly field: Field;
            readonly expected: FieldPresentValue<ExpectedRow, Field>;
            readonly received: FieldPresentValue<ReceivedRow, Field>;
          }
      : {
          readonly field: Field;
          readonly expected: FieldPresentValue<ExpectedRow, Field>;
          readonly received: FieldPresentValue<ReceivedRow, Field>;
          readonly expectedOptional: IsOptionalField<ExpectedRow, Field>;
          readonly receivedOptional: IsOptionalField<ReceivedRow, Field>;
        }
    : {
        readonly field: Field;
        readonly expected: FieldPresentValue<ExpectedRow, Field>;
        readonly received: "missing";
        readonly receivedPresent: false;
      };
}[keyof ExpectedRow];

type UnexpectedReceivedRowFieldDifference<
  ExpectedRow extends object,
  ReceivedRow extends object,
> = {
  readonly [Field in Exclude<keyof ReceivedRow, keyof ExpectedRow>]-?: {
    readonly field: Field;
    readonly expected: "absent";
    readonly expectedPresent: false;
    readonly received: FieldPresentValue<ReceivedRow, Field>;
  };
}[Exclude<keyof ReceivedRow, keyof ExpectedRow>];

type RowFieldDifferenceMember<ExpectedRow extends object, ReceivedRow extends object> =
  | ExpectedRowFieldDifference<ExpectedRow, ReceivedRow>
  | UnexpectedReceivedRowFieldDifference<ExpectedRow, ReceivedRow>;

type UnionMemberHasExactMatch<
  Member extends object,
  Candidates extends object,
> = Candidates extends unknown
  ? TypeEquals<NormalizeRowMutability<Member>, NormalizeRowMutability<Candidates>> extends true
    ? true
    : never
  : never;

type UnmatchedUnionMembers<
  Members extends object,
  Candidates extends object,
> = Members extends unknown
  ? [UnionMemberHasExactMatch<Members, Candidates>] extends [never]
    ? Members
    : never
  : never;

type DistributedRowFieldDifference<
  ExpectedRow extends object,
  ReceivedRow extends object,
> = ExpectedRow extends unknown
  ? ReceivedRow extends unknown
    ? RowFieldDifferenceMember<ExpectedRow, ReceivedRow>
    : never
  : never;

type UnionMembersWithFieldValue<
  Members extends object,
  Field extends PropertyKey,
  Value,
> = Members extends unknown
  ? Field extends keyof Members
    ? TypeEquals<FieldPresentValue<Members, Field>, Value> extends true
      ? Members
      : never
    : never
  : never;

type NonUniqueCorrelationMembers<
  Members extends object,
  Field extends PropertyKey,
  AllMembers extends object = Members,
> = Members extends unknown
  ? Field extends keyof Members
    ? TypeEquals<
        UnionMembersWithFieldValue<AllMembers, Field, FieldPresentValue<Members, Field>>,
        Members
      > extends true
      ? never
      : Members
    : Members
  : never;

type UniqueCorrelationFields<
  ExpectedUnion extends object,
  ReceivedUnion extends object,
  SharedField extends PropertyKey = keyof ExpectedUnion & keyof ReceivedUnion,
> = {
  readonly [Field in SharedField]: [
    NonUniqueCorrelationMembers<ExpectedUnion, Field>,
    NonUniqueCorrelationMembers<ReceivedUnion, Field>,
  ] extends [never, never]
    ? Field
    : never;
}[SharedField];

type CorrelationFieldValues<
  Members extends object,
  Field extends PropertyKey,
> = Members extends unknown
  ? Field extends keyof Members
    ? FieldPresentValue<Members, Field>
    : never
  : never;

type MatchingCorrelationFields<
  ExpectedUnion extends object,
  ReceivedUnion extends object,
  Field extends PropertyKey = UniqueCorrelationFields<ExpectedUnion, ReceivedUnion>,
> = Field extends unknown
  ? Field extends "_tag" | "kind" | "type" | "tag"
    ? [
        | Extract<
            CorrelationFieldValues<ExpectedUnion, Field>,
            CorrelationFieldValues<ReceivedUnion, Field>
          >
        | Extract<
            CorrelationFieldValues<ReceivedUnion, Field>,
            CorrelationFieldValues<ExpectedUnion, Field>
          >,
      ] extends [never]
      ? never
      : Field
    : TypeEquals<
          CorrelationFieldValues<ExpectedUnion, Field>,
          CorrelationFieldValues<ReceivedUnion, Field>
        > extends true
      ? Field
      : never
  : never;

type UnionToIntersection<Union> = (Union extends unknown ? (value: Union) => void : never) extends (
  value: infer Intersection,
) => void
  ? Intersection
  : never;

type LastUnionMember<Union> =
  UnionToIntersection<Union extends unknown ? () => Union : never> extends () => infer Last
    ? Last
    : never;

type PreferredCorrelationField<Fields extends PropertyKey> = "_tag" extends Fields
  ? "_tag"
  : "kind" extends Fields
    ? "kind"
    : "type" extends Fields
      ? "type"
      : "tag" extends Fields
        ? "tag"
        : Extract<LastUnionMember<Fields>, PropertyKey>;

type CorrelationDiscriminator<
  ExpectedUnion extends object,
  ReceivedUnion extends object,
> = PreferredCorrelationField<MatchingCorrelationFields<ExpectedUnion, ReceivedUnion>>;

type CorrelationField<
  ExpectedMember extends object,
  ReceivedMember extends object,
  ExpectedUnion extends object,
  ReceivedUnion extends object,
  Field extends PropertyKey = CorrelationDiscriminator<ExpectedUnion, ReceivedUnion>,
> = Field extends keyof ExpectedMember & keyof ReceivedMember
  ? TypeEquals<
      FieldPresentValue<ExpectedMember, Field>,
      FieldPresentValue<ReceivedMember, Field>
    > extends true
    ? Field
    : never
  : never;

type CorrelatedReceivedMembers<
  ExpectedMember extends object,
  ReceivedMembers extends object,
  ExpectedUnion extends object,
  ReceivedUnion extends object,
> = ReceivedMembers extends unknown
  ? [CorrelationField<ExpectedMember, ReceivedMembers, ExpectedUnion, ReceivedUnion>] extends [
      never,
    ]
    ? never
    : ReceivedMembers
  : never;

type CorrelatedExpectedMembers<
  ReceivedMember extends object,
  ExpectedMembers extends object,
  ExpectedUnion extends object,
  ReceivedUnion extends object,
> = ExpectedMembers extends unknown
  ? [CorrelationField<ExpectedMembers, ReceivedMember, ExpectedUnion, ReceivedUnion>] extends [
      never,
    ]
    ? never
    : ExpectedMembers
  : never;

type UnmatchedExpectedCorrelationDifference<
  ExpectedMember extends object,
  ReceivedUnion extends object,
  Field extends PropertyKey,
> = Field extends keyof ExpectedMember
  ? {
      readonly field: Field;
      readonly expected: FieldPresentValue<ExpectedMember, Field>;
      readonly received: CorrelationFieldValues<ReceivedUnion, Field>;
    }
  : never;

type UnmatchedReceivedCorrelationDifference<
  ExpectedUnion extends object,
  ReceivedMember extends object,
  Field extends PropertyKey,
> = Field extends keyof ReceivedMember
  ? {
      readonly field: Field;
      readonly expected: CorrelationFieldValues<ExpectedUnion, Field>;
      readonly received: FieldPresentValue<ReceivedMember, Field>;
    }
  : never;

type CorrelatedDifferencesFromExpected<
  ExpectedMembers extends object,
  ReceivedMembers extends object,
  ExpectedUnion extends object = ExpectedMembers,
  ReceivedUnion extends object = ReceivedMembers,
> = ExpectedMembers extends unknown
  ? CorrelatedReceivedMembers<
      ExpectedMembers,
      ReceivedMembers,
      ExpectedUnion,
      ReceivedUnion
    > extends infer Correlated extends object
    ? [Correlated] extends [never]
      ? [CorrelationDiscriminator<ExpectedUnion, ReceivedUnion>] extends [never]
        ? DistributedRowFieldDifference<ExpectedMembers, ReceivedMembers>
        : UnmatchedExpectedCorrelationDifference<
            ExpectedMembers,
            ReceivedUnion,
            CorrelationDiscriminator<ExpectedUnion, ReceivedUnion>
          >
      : DistributedRowFieldDifference<ExpectedMembers, Correlated>
    : never
  : never;

type CorrelatedDifferencesFromReceived<
  ExpectedMembers extends object,
  ReceivedMembers extends object,
  ExpectedUnion extends object = ExpectedMembers,
  ReceivedUnion extends object = ReceivedMembers,
> = ReceivedMembers extends unknown
  ? CorrelatedExpectedMembers<
      ReceivedMembers,
      ExpectedMembers,
      ExpectedUnion,
      ReceivedUnion
    > extends infer Correlated extends object
    ? [Correlated] extends [never]
      ? [CorrelationDiscriminator<ExpectedUnion, ReceivedUnion>] extends [never]
        ? DistributedRowFieldDifference<ExpectedMembers, ReceivedMembers>
        : UnmatchedReceivedCorrelationDifference<
            ExpectedUnion,
            ReceivedMembers,
            CorrelationDiscriminator<ExpectedUnion, ReceivedUnion>
          >
      : DistributedRowFieldDifference<Correlated, ReceivedMembers>
    : never
  : never;

type CorrelatedRowFieldDifference<ExpectedRow extends object, ReceivedRow extends object> =
  | CorrelatedDifferencesFromExpected<ExpectedRow, ReceivedRow>
  | CorrelatedDifferencesFromReceived<ExpectedRow, ReceivedRow>;

type RowFieldDifferenceMembers<
  ExpectedRow extends object,
  ReceivedRow extends object,
  UnmatchedExpected extends object = UnmatchedUnionMembers<ExpectedRow, ReceivedRow>,
  UnmatchedReceived extends object = UnmatchedUnionMembers<ReceivedRow, ExpectedRow>,
> = [UnmatchedExpected] extends [never]
  ? DistributedRowFieldDifference<ExpectedRow, UnmatchedReceived>
  : [UnmatchedReceived] extends [never]
    ? DistributedRowFieldDifference<UnmatchedExpected, ReceivedRow>
    : CorrelatedRowFieldDifference<UnmatchedExpected, UnmatchedReceived>;

type RowFieldDifference<
  ExpectedRow extends object,
  ReceivedRow extends object,
> = RowFieldDifferenceMembers<ExpectedRow, ReceivedRow>;

type CanonicalIdDetails<SchemaValue extends RowSchema> = SchemaValue extends {
  readonly fields: {
    readonly id: infer Id;
  };
}
  ? TypeEquals<Id, typeof ViewServerId> extends true
    ? never
    : {
        readonly field: "id";
        readonly expected: typeof ViewServerId;
        readonly received: Id;
      }
  : {
      readonly field: "id";
      readonly expected: typeof ViewServerId;
      readonly received: "missing";
      readonly receivedPresent: false;
    };

type InvalidRouteDetails<InvalidRoute, Row> = InvalidRoute extends PropertyKey
  ? Row extends unknown
    ? InvalidRoute extends keyof Row
      ? {
          readonly field: InvalidRoute;
          readonly expected: FilterableScalar;
          readonly received: Row[InvalidRoute];
        }
      : {
          readonly field: InvalidRoute;
          readonly expected: FilterableScalar;
          readonly received: "missing";
          readonly receivedPresent: false;
        }
    : never
  : never;

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
              InvalidRouteDetails<InvalidRoute, Row>
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
            UnsafeSourceRowReason<Source>,
            UnsafeSourceRowDetails<Source>
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
  : WithViewServerConfigValidationError<
      Source,
      Topic,
      "source must be created by SourceAdapter.make(...)",
      { readonly received: Source }
    >;

type ValidateOptionalSource<
  Topic extends PropertyKey,
  Row extends object,
  Source,
> = Source extends undefined ? undefined : ValidateSource<Topic, Row, Source>;

type ValidateTopicSource<
  TopicName extends PropertyKey,
  Topic,
  Row extends object,
> = "source" extends keyof Topic
  ? {} extends Pick<Topic, "source">
    ? {
        readonly source?: ValidateOptionalSource<TopicName, Row, Topic["source"]>;
      }
    : {
        readonly source: ValidateOptionalSource<TopicName, Row, Topic["source"]>;
      }
  : {};

type ValidateTopicMember<TopicName extends PropertyKey, Topic> = Topic extends {
  (...arguments_: infer _Arguments): unknown;
}
  ? WithViewServerConfigValidationError<
      Topic,
      TopicName,
      "topic definition must not be a function value",
      { readonly received: Topic }
    >
  : Topic extends abstract new (...arguments_: infer _Arguments) => unknown
    ? WithViewServerConfigValidationError<
        Topic,
        TopicName,
        "topic definition must not be a function value",
        { readonly received: Topic }
      >
    : Topic extends FunctionValue
      ? WithViewServerConfigValidationError<
          Topic,
          TopicName,
          "topic definition must not be a function value",
          { readonly received: Topic }
        >
      : Topic extends {
            readonly schema: infer TopicSchema extends RowSchema;
          }
        ? HasCanonicalId<TopicSchema> extends true
          ? Omit<Topic, "source"> &
              RejectExtraKeys<Topic, ViewServerTopicShape> &
              ValidateTopicSource<TopicName, Topic, RowFromSchema<TopicSchema>> & {
                readonly schema: TopicSchema;
              }
          : Topic &
              ViewServerConfigValidationError<
                TopicName,
                "topic schema must define id as ViewServerId",
                CanonicalIdDetails<TopicSchema>
              >
        : WithViewServerConfigValidationError<
            Topic,
            TopicName,
            "topic schema must expose concrete struct fields",
            { readonly received: Topic }
          >;

type InvalidTopicMembers<TopicName extends PropertyKey, Topic> = Topic extends unknown
  ? Topic extends ValidateTopicMember<TopicName, Topic>
    ? never
    : Topic
  : never;

type ValidateTopicMembers<TopicName extends PropertyKey, Topic> = [
  InvalidTopicMembers<TopicName, Topic>,
] extends [never]
  ? unknown
  : never;

type ValidateTopic<TopicName extends PropertyKey, Topic> = ValidateTopicMember<TopicName, Topic> &
  (true extends IsUnion<Topic> ? ValidateTopicMembers<TopicName, Topic> : unknown);

type UnionKeys<Union> = Union extends unknown ? keyof Union : never;

type ValidateTopicDefinitions<Topics, AllTopics = Topics> = Topics extends unknown
  ? {
      readonly [Topic in keyof Topics]: Topic extends ViewServerSystemTopicName
        ? WithViewServerConfigValidationError<
            Topics[Topic],
            Topic,
            "topic name is reserved for system health streams",
            { readonly received: Topic }
          >
        : ValidateTopic<Topic, Topics[Topic]>;
    } & {
      readonly [Topic in Exclude<UnionKeys<AllTopics>, keyof Topics>]?: never;
    }
  : never;

type InvalidSourceDefinitions<
  TopicName extends PropertyKey,
  Row extends object,
  Source,
> = Source extends unknown
  ? Source extends ValidateSource<TopicName, Row, Source>
    ? never
    : Source
  : never;

type InvalidSourceUnionTopic<TopicName extends PropertyKey, Topic> = Topic extends {
  readonly schema: infer TopicSchema extends RowSchema;
}
  ? "source" extends keyof Topic
    ? Exclude<Topic["source"], undefined> extends infer Source
      ? TypeEquals<IsUnion<Source>, true> extends true
        ? [InvalidSourceDefinitions<TopicName, RowFromSchema<TopicSchema>, Source>] extends [never]
          ? never
          : Topic
        : never
      : never
    : never
  : never;

type InvalidSourceUnionTopics<Topics> = Topics extends unknown
  ? {
      readonly [TopicName in keyof Topics]: InvalidSourceUnionTopic<TopicName, Topics[TopicName]>;
    }[keyof Topics]
  : never;

type ValidateSourceUnions<Topics> = [InvalidSourceUnionTopics<Topics>] extends [never]
  ? unknown
  : never;

type InvalidTopicDefinitionRegistries<Topics> = Topics extends unknown
  ? Topics extends ValidateTopicDefinitions<Topics>
    ? never
    : Topics
  : never;

type ValidateTopicDefinitionRegistries<Topics> = [
  InvalidTopicDefinitionRegistries<Topics>,
] extends [never]
  ? unknown
  : never;

type ValidateTopicDefinitionRegistryUnion<Topics> =
  true extends IsUnion<Topics> ? ValidateTopicDefinitionRegistries<Topics> : unknown;

type ViewServerConfigTopicsAreValid<Topics extends ViewServerConfigTopicShape> = [
  InvalidTopicDefinitionRegistries<Topics>,
  InvalidSourceUnionTopics<Topics>,
] extends [never, never]
  ? true
  : false;

export type ViewServerConfig<Topics extends ViewServerConfigTopicShape> =
  ViewServerConfigTopicsAreValid<Topics> extends true
    ? {
        readonly topics: Topics;
      }
    : never;

export type DefineViewServerConfigInput<Topics extends ViewServerConfigTopicCandidateShape> = {
  readonly topics: ValidateTopicDefinitions<Topics>;
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

export function defineViewServerConfig<const Topics extends ViewServerConfigTopicShape>(
  input: { readonly topics: Topics } & (ViewServerConfigTopicsAreValid<Topics> extends true
    ? unknown
    : never),
): ViewServerConfig<Topics>;
export function defineViewServerConfig<const Topics extends ViewServerConfigTopicCandidateShape>(
  input: {
    readonly topics: Topics &
      ValidateSourceUnions<Topics> &
      ValidateTopicDefinitionRegistryUnion<Topics>;
  } & DefineViewServerConfigInput<Topics>,
): Topics extends ViewServerConfigTopicShape ? ViewServerConfig<Topics> : never;
export function defineViewServerConfig(input: { readonly topics: ViewServerConfigTopicShape }) {
  const unsupportedConfigProperty = ownPropertyNamesAreExact(input, new Set(["topics"]));
  if (unsupportedConfigProperty !== undefined) {
    throw new Error(
      `View Server config contains unsupported property: ${String(unsupportedConfigProperty)}.`,
    );
  }
  const topics = snapshotViewServerTopics(input.topics);
  for (const topic of Object.keys(topics)) {
    const topicDefinition = topics[topic]!;
    const allowedTopicProperties = new Set<PropertyKey>(["schema", "source"]);
    const unsupportedTopicProperty = Reflect.ownKeys(topicDefinition).find(
      (property) => !allowedTopicProperties.has(property),
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
