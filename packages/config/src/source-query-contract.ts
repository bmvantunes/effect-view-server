import { Result, Schema } from "effect";
import { isBigDecimal, type BigDecimal } from "effect/BigDecimal";
import type { TopicDefinitions, TopicRow } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { RouteFieldKey, RouteFieldValue } from "./query-filter";
import type { ExactLiveQuery, ValidateLiveQuery } from "./query-result-contract";
import type {
  NonEmptyRouteBy,
  TopicLeasedSourceDefinition,
  TopicSourceDefinition,
} from "./source-contract";

type RouteShape<Row, RouteBy extends string> = {
  readonly [Field in Extract<RouteBy, RouteFieldKey<Row>>]-?: RouteFieldValue<Row, Field>;
};

type ExactRouteObject<Row, RouteBy extends string, Candidate> =
  Candidate extends RouteShape<Row, RouteBy>
    ? Candidate & RejectExtraKeys<Candidate, RouteShape<Row, RouteBy>>
    : never;

type UnionToIntersection<Union> = (Union extends unknown ? (value: Union) => void : never) extends (
  value: infer Intersection,
) => void
  ? Intersection
  : never;

export type TopicRouteBy<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly grpcSource: TopicLeasedSourceDefinition<infer RouteBy>;
}
  ? Extract<RouteBy[number], string>
  : never;

export type TopicRouteByTuple<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly grpcSource: TopicLeasedSourceDefinition<infer RouteBy>;
}
  ? RouteBy extends NonEmptyRouteBy
    ? RouteBy
    : never
  : never;

export type ExactLeasedRouteQuery<Row, RouteBy extends string, Query> = [RouteBy] extends [never]
  ? { readonly routeBy?: never }
  : Query extends { readonly routeBy: infer Candidate }
    ? { readonly routeBy: ExactRouteObject<Row, RouteBy, Candidate> }
    : { readonly routeBy: RouteShape<Row, RouteBy> };

type ExactLeasedRouteQueryForTopic<
  Topics,
  Topic extends keyof Topics,
  Query,
> = Topic extends keyof Topics
  ? [TopicRouteBy<Topics, Topic>] extends [never]
    ? never
    : ExactLeasedRouteQuery<TopicRow<Topics, Topic>, TopicRouteBy<Topics, Topic>, Query>
  : never;

type LeasedRouteFieldsForTopicUnion<Topics, Topic extends keyof Topics> = Topic extends keyof Topics
  ? TopicRouteBy<Topics, Topic>
  : never;

type LeasedRouteShapeForTopic<Topics, Topic extends keyof Topics> = Topic extends keyof Topics
  ? [TopicRouteBy<Topics, Topic>] extends [never]
    ? never
    : RouteShape<TopicRow<Topics, Topic>, TopicRouteBy<Topics, Topic>>
  : never;

type IsMutuallyAssignable<Left, Right> = [Left] extends [Right]
  ? [Right] extends [Left]
    ? true
    : false
  : false;

type TopicsWithDifferentLeasedRouteContracts<
  Topics,
  Topic extends keyof Topics,
  AllContracts,
> = Topic extends keyof Topics
  ? [TopicRouteBy<Topics, Topic>] extends [never]
    ? never
    : IsMutuallyAssignable<LeasedRouteShapeForTopic<Topics, Topic>, AllContracts> extends true
      ? never
      : Topic
  : never;

type OrdinaryTopicsForTopicUnion<Topics, Topic extends keyof Topics> = Topic extends keyof Topics
  ? [TopicRouteBy<Topics, Topic>] extends [never]
    ? Topic
    : never
  : never;

type ExactSourceRouteQuery<
  Topics,
  Topic extends keyof Topics,
  Query,
  AllContracts = LeasedRouteShapeForTopic<Topics, Topic>,
> = [LeasedRouteFieldsForTopicUnion<Topics, Topic>] extends [never]
  ? { readonly routeBy?: never }
  : [OrdinaryTopicsForTopicUnion<Topics, Topic>] extends [never]
    ? [TopicsWithDifferentLeasedRouteContracts<Topics, Topic, AllContracts>] extends [never]
      ? UnionToIntersection<ExactLeasedRouteQueryForTopic<Topics, Topic, Query>>
      : never
    : never;

type QueryWithoutRoute<Query> = Omit<Query, "routeBy">;

type ExactLiveQueryForTopic<Topics, Topic extends keyof Topics, Query> = Topic extends keyof Topics
  ? ExactLiveQuery<TopicRow<Topics, Topic>, QueryWithoutRoute<Query>>
  : never;

type TopicsRejectingLiveQuery<
  Topics,
  Topic extends keyof Topics,
  Query,
> = Topic extends keyof Topics
  ? QueryWithoutRoute<Query> extends ExactLiveQuery<
      TopicRow<Topics, Topic>,
      QueryWithoutRoute<Query>
    >
    ? never
    : Topic
  : never;

type IsUnion<Value> = [Value] extends [UnionToIntersection<Value>] ? false : true;

type RejectInvalidTopicUnionQuery<Topics, Topic extends keyof Topics, Query> =
  true extends IsUnion<Topic>
    ? [TopicsRejectingLiveQuery<Topics, Topic, Query>] extends [never]
      ? unknown
      : never
    : unknown;

export type ExactLiveQueryInputForTopic<Topics, Topic extends keyof Topics, Query> = Query &
  UnionToIntersection<ExactLiveQueryForTopic<Topics, Topic, Query>> &
  RejectInvalidTopicUnionQuery<Topics, Topic, Query> &
  ValidateLiveQuery<QueryWithoutRoute<Query>> &
  ExactSourceRouteQuery<Topics, Topic, Query>;

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isWireSafeBigDecimal = (value: unknown): value is BigDecimal =>
  isBigDecimal(value) && typeof value.value === "bigint" && Number.isSafeInteger(value.scale);

const isPlainRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  isRecord(value) &&
  !isWireSafeBigDecimal(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

const sourceLeasedRouteBy = (
  source: TopicSourceDefinition | undefined,
): ReadonlyArray<string> | "invalid" | undefined => {
  const candidate: unknown = source;
  if (!isRecord(candidate) || candidate["lifecycle"] !== "leased") {
    return undefined;
  }
  const routeBy = candidate["routeBy"];
  if (
    !Array.isArray(routeBy) ||
    routeBy.length === 0 ||
    !routeBy.every((field) => typeof field === "string") ||
    new Set(routeBy).size !== routeBy.length
  ) {
    return "invalid";
  }
  return routeBy;
};

const routeScalarIsSupported = (value: unknown): boolean =>
  value === null ||
  typeof value === "string" ||
  typeof value === "bigint" ||
  typeof value === "boolean" ||
  isWireSafeBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

const ownEnumerableDataProperty = (
  value: Readonly<Record<string, unknown>>,
  property: string,
): PropertyDescriptor | undefined => {
  const descriptor = Object.getOwnPropertyDescriptor(value, property);
  return descriptor !== undefined && descriptor.enumerable && "value" in descriptor
    ? descriptor
    : undefined;
};

export const validateLiveQuerySourceRoute = <Topics extends TopicDefinitions>(
  topics: Topics,
  topic: string,
  query: unknown,
): string | undefined => {
  const topicDefinition = topics[topic];
  if (topicDefinition === undefined) {
    return undefined;
  }
  const sourceAwareTopic: {
    readonly grpcSource?: TopicSourceDefinition | undefined;
  } = topicDefinition;
  const configuredRouteBy = sourceLeasedRouteBy(sourceAwareTopic.grpcSource);
  if (configuredRouteBy === undefined) {
    if (isRecord(query) && Object.hasOwn(query, "routeBy")) {
      return `Topic ${topic} does not accept routeBy.`;
    }
    return undefined;
  }
  if (configuredRouteBy === "invalid") {
    return `Leased topic ${topic} has invalid route metadata.`;
  }
  if (!isRecord(query)) {
    return `Leased topic ${topic} requires a query object.`;
  }
  const routeByDescriptor = ownEnumerableDataProperty(query, "routeBy");
  if (routeByDescriptor === undefined || !isPlainRecord(routeByDescriptor.value)) {
    return `Leased topic ${topic} requires routeBy fields: ${configuredRouteBy.join(", ")}.`;
  }
  const routeBy = routeByDescriptor.value;
  if (Object.getOwnPropertySymbols(routeBy).length > 0) {
    return `Leased topic ${topic} routeBy contains unsupported symbol properties.`;
  }
  const actualFields = Object.getOwnPropertyNames(routeBy);
  const configuredRouteFields = new Set(configuredRouteBy);
  if (
    actualFields.length !== configuredRouteBy.length ||
    actualFields.some((field) => !configuredRouteFields.has(field))
  ) {
    return `Leased topic ${topic} routeBy must contain all and only: ${configuredRouteBy.join(", ")}.`;
  }
  for (const field of configuredRouteBy) {
    const descriptor = ownEnumerableDataProperty(routeBy, field);
    if (descriptor === undefined || !routeScalarIsSupported(descriptor.value)) {
      return `Leased topic ${topic} routeBy field ${field} must be a supported scalar value.`;
    }
    const fieldSchema = topicDefinition.schema.fields[field];
    const matchesFieldSchema =
      fieldSchema === undefined
        ? undefined
        : Result.try(() => Schema.is(fieldSchema)(descriptor.value));
    if (
      matchesFieldSchema === undefined ||
      Result.isFailure(matchesFieldSchema) ||
      !matchesFieldSchema.success
    ) {
      return `Leased topic ${topic} routeBy field ${field} does not satisfy its configured schema.`;
    }
  }
  return undefined;
};
