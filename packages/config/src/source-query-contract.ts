import { Result, Schema } from "effect";
import { make as makeBigDecimal } from "effect/BigDecimal";
import type {
  SourceDefinitionLifecycle,
  SourceDefinitionRouteFields,
} from "@effect-view-server/source-adapter";
import type { TopicDefinitions, TopicRow } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { RouteFieldKey, RouteFieldValue } from "./query-filter";
import type { ExactLiveQuery, ValidateLiveQuery } from "./query-result-contract";
import type { NonEmptyRouteBy } from "./source-contract";

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

type LeasedRouteFields<Source> = Source extends unknown
  ? SourceDefinitionLifecycle<Source> extends "leased"
    ? Extract<SourceDefinitionRouteFields<Source>[number], string>
    : never
  : never;

type LeasedRouteTuple<Source> = Source extends unknown
  ? SourceDefinitionLifecycle<Source> extends "leased"
    ? SourceDefinitionRouteFields<Source> extends NonEmptyRouteBy
      ? SourceDefinitionRouteFields<Source>
      : never
    : never
  : never;

export type TopicRouteBy<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly source: infer Source;
}
  ? LeasedRouteFields<Source>
  : never;

export type TopicRouteByTuple<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly source: infer Source;
}
  ? LeasedRouteTuple<Source>
  : never;

export type ExactLeasedRouteQuery<Row, RouteBy extends string, Query> = [RouteBy] extends [never]
  ? { readonly routeBy?: never }
  : Query extends { readonly routeBy: infer Candidate }
    ? { readonly routeBy: ExactRouteObject<Row, RouteBy, Candidate> }
    : { readonly routeBy: RouteShape<Row, RouteBy> };

type ExactSourceRouteQueryForDefinition<Row, Source, Query> = Source extends unknown
  ? SourceDefinitionLifecycle<Source> extends "leased"
    ? SourceDefinitionRouteFields<Source> extends NonEmptyRouteBy
      ? ExactLeasedRouteQuery<
          Row,
          Extract<SourceDefinitionRouteFields<Source>[number], string>,
          Query
        >
      : never
    : { readonly routeBy?: never }
  : never;

type ExactSourceRouteQueryForTopic<
  Topics,
  Topic extends keyof Topics,
  Query,
> = Topic extends keyof Topics
  ? {
      readonly exact: Topics[Topic] extends { readonly source: infer Source }
        ? ExactSourceRouteQueryForDefinition<TopicRow<Topics, Topic>, Source, Query>
        : { readonly routeBy?: never };
    }
  : never;

type ExactSourceRouteQueryMember<Topics, Topic extends keyof Topics, Query> =
  UnionToIntersection<ExactSourceRouteQueryForTopic<Topics, Topic, Query>> extends {
    readonly exact: infer Exact;
  }
    ? Exact
    : never;

type InvalidExactSourceRouteQueryMember<
  Topics,
  Topic extends keyof Topics,
  Query,
> = Query extends unknown
  ? Query extends Query & ExactSourceRouteQueryMember<Topics, Topic, Query>
    ? never
    : Query
  : never;

type ExactSourceRouteQueryMembers<Topics, Topic extends keyof Topics, Query> = Query extends unknown
  ? ExactSourceRouteQueryMember<Topics, Topic, Query>
  : never;

type ExactSourceRouteQuery<Topics, Topic extends keyof Topics, Query> = [
  InvalidExactSourceRouteQueryMember<Topics, Topic, Query>,
] extends [never]
  ? ExactSourceRouteQueryMembers<Topics, Topic, Query>
  : never;

type QueryWithoutRoute<Query> = "routeBy" extends keyof Query
  ? Query extends unknown
    ? Omit<Query, "routeBy">
    : never
  : Query;

type ExactLiveQueryForTopic<Topics, Topic extends keyof Topics, Query> = Topic extends keyof Topics
  ? {
      readonly exact: ExactLiveQuery<TopicRow<Topics, Topic>, QueryWithoutRoute<Query>>;
    }
  : never;

type ExactLiveQueryForAllTopics<Topics, Topic extends keyof Topics, Query> =
  UnionToIntersection<ExactLiveQueryForTopic<Topics, Topic, Query>> extends {
    readonly exact: infer Exact;
  }
    ? Exact
    : never;

export type ExactLiveQueryInputForTopic<Topics, Topic extends keyof Topics, Query> = Query &
  NoInfer<
    ExactLiveQueryForAllTopics<Topics, Topic, Query> &
      ValidateLiveQuery<QueryWithoutRoute<Query>> &
      ExactSourceRouteQuery<Topics, Topic, Query>
  >;

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isPlainRecord = (value: unknown): value is Readonly<Record<string, unknown>> => {
  if (!isRecord(value)) {
    return false;
  }
  try {
    return Object.getPrototypeOf(value) === Object.prototype;
  } catch {
    return false;
  }
};

const bigDecimalTypeId = "~effect/BigDecimal";
const bigDecimalJsonCodec = Schema.toCodecJson(Schema.BigDecimal);

const routeBigDecimalIsWireRoundtrippable = (value: unknown): boolean => {
  try {
    if (typeof value !== "object" || value === null) {
      return false;
    }
    const prototype = Object.getPrototypeOf(value);
    if (typeof prototype !== "object" || prototype === null) {
      return false;
    }
    const brand = Object.getOwnPropertyDescriptor(prototype, bigDecimalTypeId);
    if (brand === undefined || !("value" in brand) || brand.value !== bigDecimalTypeId) {
      return false;
    }
    const coefficient = Object.getOwnPropertyDescriptor(value, "value");
    const scale = Object.getOwnPropertyDescriptor(value, "scale");
    if (
      coefficient === undefined ||
      !coefficient.enumerable ||
      !("value" in coefficient) ||
      typeof coefficient.value !== "bigint" ||
      scale === undefined ||
      !scale.enumerable ||
      !("value" in scale) ||
      typeof scale.value !== "number" ||
      !Number.isSafeInteger(scale.value)
    ) {
      return false;
    }
    const encoded = Schema.encodeUnknownResult(bigDecimalJsonCodec)(
      makeBigDecimal(coefficient.value, scale.value),
    );
    return (
      Result.isSuccess(encoded) &&
      Result.isSuccess(Schema.decodeUnknownResult(bigDecimalJsonCodec)(encoded.success))
    );
  } catch {
    return false;
  }
};

export const sourceLeasedRouteBy = (
  source: unknown,
): ReadonlyArray<string> | "invalid" | undefined => {
  const inspected = Result.try(() => {
    const candidate: unknown = source;
    if (!isRecord(candidate) || candidate["lifecycle"] !== "leased") {
      return undefined;
    }
    const routeBy = candidate["routeBy"];
    if (!Array.isArray(routeBy)) {
      return "invalid";
    }
    const routeFields = [...routeBy];
    if (
      routeFields.length === 0 ||
      !routeFields.every((field) => typeof field === "string") ||
      new Set(routeFields).size !== routeFields.length
    ) {
      return "invalid";
    }
    return routeFields;
  });
  return Result.isFailure(inspected) ? "invalid" : inspected.success;
};

const routeScalarIsSupported = (value: unknown): boolean =>
  value === null ||
  typeof value === "string" ||
  typeof value === "bigint" ||
  typeof value === "boolean" ||
  routeBigDecimalIsWireRoundtrippable(value) ||
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

const validateLiveQuerySourceRouteUnsafe = <Topics extends TopicDefinitions>(
  topics: Topics,
  topic: string,
  query: unknown,
): string | undefined => {
  const topicDefinition = topics[topic];
  if (topicDefinition === undefined) {
    return undefined;
  }
  const configuredRouteBy = sourceLeasedRouteBy(topicDefinition.source);
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
    const fields = topicDefinition.schema.fields;
    const fieldSchema = Object.hasOwn(fields, field) ? fields[field] : undefined;
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

export const validateLiveQuerySourceRoute = <Topics extends TopicDefinitions>(
  topics: Topics,
  topic: string,
  query: unknown,
): string | undefined => {
  const validation = Result.try(() => validateLiveQuerySourceRouteUnsafe(topics, topic, query));
  return Result.isFailure(validation)
    ? `Query for topic ${topic} contains unsupported reflective properties.`
    : validation.success;
};
