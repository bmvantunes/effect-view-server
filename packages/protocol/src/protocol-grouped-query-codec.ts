import type {
  Aggregates,
  FieldKey,
  GroupedOrderBy,
  RowSchema,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  Where,
} from "@effect-view-server/config";
import { viewServerSchemaFieldMetadata } from "@effect-view-server/config";
import {
  trustDecodedRuntimeQuery,
  type ValidatedRuntimeQuery,
} from "@effect-view-server/config/internal";
import { Effect, Schema } from "effect";
import type { JsonFieldSchema } from "./protocol-json-field-codec";
import {
  decodeWhere,
  decodeRouteBy,
  encodeWhere,
  encodeRouteBy,
  hasOwnField,
  hasTopic,
  invalidQuery,
  invalidTopic,
  ownProtocolQueryInput,
  requireRouteByRecord,
  shallowQueryInput,
  strictParseOptions,
  validateSourceRoute,
  validateWindow,
  viewServerDecodeTopic,
} from "./protocol-query-common";
import {
  LooseWireGroupedQuerySchema,
  type LooseWireGroupedQuery,
  type ViewServerWireGroupedQuery,
} from "./protocol-query-schema";

type TrustedGroupedQuery<Row> = {
  readonly groupBy: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
  readonly aggregates: Aggregates<Row>;
  readonly where?: Where<Row>;
  readonly routeBy?: Readonly<Record<string, unknown>>;
  readonly orderBy?: ReadonlyArray<GroupedOrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

type LooseWireGroupedOrderBy = NonNullable<LooseWireGroupedQuery["orderBy"]>[number];
type LooseWireGroupedFieldOrderBy = Extract<LooseWireGroupedOrderBy, { readonly field: string }>;
type LooseWireGroupedAggregateOrderBy = Extract<
  LooseWireGroupedOrderBy,
  { readonly aggregate: string }
>;

const hasOwnGroupedOrderField = (
  order: LooseWireGroupedOrderBy,
): order is LooseWireGroupedFieldOrderBy => Object.hasOwn(order, "field");

const hasOwnGroupedOrderAggregate = (
  order: LooseWireGroupedOrderBy,
): order is LooseWireGroupedAggregateOrderBy => Object.hasOwn(order, "aggregate");

export type ViewServerValidatedGroupedQuery<Row extends object> = TrustedGroupedQuery<Row> &
  ValidatedRuntimeQuery;

const dangerousRecordKeys = new Set(["__proto__", "prototype", "constructor"]);

const isNumericFieldSchema = (schema: JsonFieldSchema | undefined): boolean => {
  return schema !== undefined && viewServerSchemaFieldMetadata(schema).isNumeric;
};

const validateGroupedQuery = Effect.fn("ViewServerProtocol.groupedQuery.validate")(function* (
  topic: string,
  schema: RowSchema,
  decoded: LooseWireGroupedQuery,
) {
  if (decoded.groupBy.length === 0) {
    return yield* Effect.fail(
      invalidQuery(topic, "Grouped query groupBy must include at least one field"),
    );
  }
  const aggregateAliases = Object.keys(decoded.aggregates);
  if (aggregateAliases.length === 0) {
    return yield* Effect.fail(
      invalidQuery(topic, "Grouped query aggregates must include at least one aggregate"),
    );
  }
  for (const groupField of decoded.groupBy) {
    if (!hasOwnField(schema, groupField)) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
      );
    }
  }
  for (const [alias, aggregate] of Object.entries(decoded.aggregates)) {
    if (dangerousRecordKeys.has(alias)) {
      return yield* Effect.fail(
        invalidQuery(topic, `Grouped aggregate alias is not allowed: ${alias}`),
      );
    }
    if (decoded.groupBy.includes(alias)) {
      return yield* Effect.fail(
        invalidQuery(topic, `Aggregate alias collides with groupBy field: ${alias}`),
      );
    }
    if (aggregate.aggFunc !== "count" && !hasOwnField(schema, aggregate.field)) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
      );
    }
    if (
      (aggregate.aggFunc === "sum" || aggregate.aggFunc === "avg") &&
      !isNumericFieldSchema(schema.fields[aggregate.field])
    ) {
      return yield* Effect.fail(
        invalidQuery(topic, `Grouped aggregate ${alias} must reference a numeric field`),
      );
    }
  }
  if (decoded.orderBy !== undefined) {
    for (const entry of decoded.orderBy) {
      if (hasOwnGroupedOrderField(entry) && !decoded.groupBy.includes(entry.field)) {
        return yield* Effect.fail(
          invalidQuery(topic, `Grouped orderBy field is not in groupBy: ${entry.field}`),
        );
      }
      if (
        hasOwnGroupedOrderAggregate(entry) &&
        !Object.hasOwn(decoded.aggregates, entry.aggregate)
      ) {
        return yield* Effect.fail(
          invalidQuery(topic, `Grouped orderBy aggregate is not defined: ${entry.aggregate}`),
        );
      }
    }
  }
  yield* validateWindow(topic, decoded.offset, decoded.limit);
});

export const viewServerEncodeGroupedQuery = Effect.fn("ViewServerProtocol.groupedQuery.encode")(
  function* <const Topics extends TopicDefinitions, Topic extends Extract<keyof Topics, string>>(
    config: { readonly topics: Topics },
    topic: Topic,
    query: unknown,
  ) {
    if (!hasTopic(config, topic)) {
      return yield* Effect.fail(invalidTopic(topic));
    }
    const ownedQuery = yield* ownProtocolQueryInput(topic, query);
    const shallowQuery = yield* shallowQueryInput(topic, ownedQuery);
    const routeByInput = shallowQuery.hasRouteBy
      ? yield* requireRouteByRecord(topic, shallowQuery.routeBy)
      : undefined;
    const decodedShell = yield* Schema.decodeUnknownEffect(LooseWireGroupedQuerySchema)(
      shallowQuery.input,
      strictParseOptions,
    ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
    const decodedWhere =
      shallowQuery.where === undefined
        ? decodedShell
        : { ...decodedShell, where: shallowQuery.where };
    const decoded =
      routeByInput === undefined ? decodedWhere : { ...decodedWhere, routeBy: routeByInput };
    const topicSchema = config.topics[topic]!.schema;
    yield* validateSourceRoute(config, topic, decoded);
    yield* validateGroupedQuery(topic, topicSchema, decoded);
    const where = yield* encodeWhere(config, topic, decoded.where);
    const routeBy =
      routeByInput === undefined ? undefined : yield* encodeRouteBy(config, topic, routeByInput);
    const wireQuery: ViewServerWireGroupedQuery = {
      groupBy: decoded.groupBy,
      aggregates: decoded.aggregates,
      ...(where === undefined ? {} : { where }),
      ...(routeBy === undefined ? {} : { routeBy }),
      ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
      ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
      ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
    };
    return wireQuery;
  },
);

function validatedGroupedQuery<Row extends object>(
  query: LooseWireGroupedQuery,
): ViewServerValidatedGroupedQuery<Row>;
function validatedGroupedQuery(query: LooseWireGroupedQuery) {
  return trustDecodedRuntimeQuery(query);
}

const decodeGroupedQuery = Effect.fn("ViewServerProtocol.groupedQuery.decode")(function* (
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: unknown,
) {
  const decodedTopic = yield* viewServerDecodeTopic(config, topic);
  const topicSchema = config.topics[decodedTopic]!.schema;
  const ownedQuery = yield* ownProtocolQueryInput(topic, query);
  const shallowQuery = yield* shallowQueryInput(topic, ownedQuery);
  const routeBy = shallowQuery.hasRouteBy
    ? yield* decodeRouteBy(topic, topicSchema, shallowQuery.routeBy)
    : undefined;
  const decodedShell = yield* Schema.decodeUnknownEffect(LooseWireGroupedQuerySchema)(
    shallowQuery.input,
    strictParseOptions,
  ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
  const decodedWhere =
    shallowQuery.where === undefined
      ? decodedShell
      : { ...decodedShell, where: shallowQuery.where };
  const decoded = routeBy === undefined ? decodedWhere : { ...decodedWhere, routeBy };
  yield* validateGroupedQuery(topic, topicSchema, decoded);
  const where = yield* decodeWhere(topic, topicSchema, decoded.where);
  const trusted = validatedGroupedQuery<object>({
    groupBy: decoded.groupBy,
    aggregates: decoded.aggregates,
    ...(where === undefined ? {} : { where }),
    ...(routeBy === undefined ? {} : { routeBy }),
    ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
    ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
    ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
  });
  yield* validateSourceRoute(config, topic, trusted);
  return trusted;
});

export function viewServerDecodeGroupedQuery<
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: unknown,
): Effect.Effect<ViewServerValidatedGroupedQuery<TopicRow<Topics, Topic>>, ViewServerRuntimeError>;
export function viewServerDecodeGroupedQuery(
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: unknown,
): Effect.Effect<unknown, ViewServerRuntimeError> {
  return decodeGroupedQuery(config, topic, query);
}
