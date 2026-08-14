import { definedFields } from "@effect-view-server/effect-utils";
import type {
  FieldKey,
  OrderBy,
  RowSchema,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  Where,
} from "@effect-view-server/config";
import {
  trustDecodedRuntimeQuery,
  type ValidatedRuntimeQuery,
} from "@effect-view-server/config/internal";
import { Effect, Schema } from "effect";
import {
  decodeWhere,
  decodeRouteBy,
  encodeWhere,
  encodeRouteBy,
  getFieldSchema,
  hasOnlyKnownFields,
  hasTopic,
  invalidQuery,
  invalidTopic,
  snapshotProtocolQueryInput,
  decodeRouteByRecord,
  shallowQueryInput,
  strictParseOptions,
  validateSourceRoute,
  validateWindow,
  viewServerDecodeTopic,
} from "./protocol-query-common";
import {
  LooseWireRawQuerySchema,
  type LooseWireRawQuery,
  type ViewServerWireRawQuery,
} from "./protocol-query-schema";

type TrustedRawQuery<Row> = {
  readonly select: ReadonlyArray<FieldKey<Row>>;
  readonly where?: Where<Row>;
  readonly routeBy?: Readonly<Record<string, unknown>>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

export type ViewServerValidatedRawQuery<Row extends object> = TrustedRawQuery<Row> &
  ValidatedRuntimeQuery;

type ProtocolQueryInput = Schema.Schema.Type<typeof Schema.Unknown>;

const isRawQueryForTopic = (schema: RowSchema, query: LooseWireRawQuery): boolean => {
  if (!hasOnlyKnownFields(schema, query.select)) {
    return false;
  }
  if (
    query.orderBy !== undefined &&
    !hasOnlyKnownFields(
      schema,
      query.orderBy.map((entry) => entry.field),
    )
  ) {
    return false;
  }
  return true;
};

export const viewServerEncodeRawQuery = Effect.fn("ViewServerProtocol.query.encode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(config: { readonly topics: Topics }, topic: Topic, query: ProtocolQueryInput) {
  if (!hasTopic(config, topic)) {
    return yield* Effect.fail(invalidTopic(topic));
  }
  const ownedQuery = yield* snapshotProtocolQueryInput(topic, query);
  const shallowQuery = yield* shallowQueryInput(topic, ownedQuery);
  const routeByInput = shallowQuery.hasRouteBy
    ? yield* decodeRouteByRecord(topic, shallowQuery.routeBy)
    : undefined;
  const decodedShell = yield* Schema.decodeUnknownEffect(LooseWireRawQuerySchema)(
    shallowQuery.input,
    strictParseOptions,
  ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
  const decodedWhere =
    shallowQuery.where === undefined
      ? decodedShell
      : { ...decodedShell, where: shallowQuery.where };
  const decoded =
    routeByInput === undefined ? decodedWhere : { ...decodedWhere, routeBy: routeByInput };
  if (decoded.select.length === 0) {
    return yield* Effect.fail(invalidQuery(topic, "Query select must include at least one field"));
  }
  yield* validateSourceRoute(config, topic, decoded);
  yield* validateWindow(topic, decoded.offset, decoded.limit);
  for (const field of decoded.select) {
    if (getFieldSchema(config, topic, field) === undefined) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
      );
    }
  }
  if (decoded.orderBy !== undefined) {
    for (const entry of decoded.orderBy) {
      if (getFieldSchema(config, topic, entry.field) === undefined) {
        return yield* Effect.fail(
          invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
        );
      }
    }
  }
  const where = yield* encodeWhere(config, topic, decoded.where);
  const routeBy =
    routeByInput === undefined ? undefined : yield* encodeRouteBy(config, topic, routeByInput);
  const wireQuery: ViewServerWireRawQuery = {
    select: decoded.select,
    ...definedFields(where, (where) => ({ where })),
    ...definedFields(routeBy, (routeBy) => ({ routeBy })),
    ...definedFields(decoded.orderBy, (orderBy) => ({ orderBy })),
    ...definedFields(decoded.offset, (offset) => ({ offset })),
    ...definedFields(decoded.limit, (limit) => ({ limit })),
  };
  return wireQuery;
});

function validatedRawQuery<Row extends object>(
  query: LooseWireRawQuery,
): ViewServerValidatedRawQuery<Row>;
function validatedRawQuery(query: LooseWireRawQuery) {
  return trustDecodedRuntimeQuery(query);
}

const decodeRawQuery = Effect.fn("ViewServerProtocol.query.decode")(function* (
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: ProtocolQueryInput,
) {
  const decodedTopic = yield* viewServerDecodeTopic(config, topic);
  const topicSchema = config.topics[decodedTopic]!.schema;
  const ownedQuery = yield* snapshotProtocolQueryInput(topic, query);
  const shallowQuery = yield* shallowQueryInput(topic, ownedQuery);
  const routeBy = shallowQuery.hasRouteBy
    ? yield* decodeRouteBy(topic, topicSchema, shallowQuery.routeBy)
    : undefined;
  const decodedShell = yield* Schema.decodeUnknownEffect(LooseWireRawQuerySchema)(
    shallowQuery.input,
    strictParseOptions,
  ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
  const decodedWhere =
    shallowQuery.where === undefined
      ? decodedShell
      : { ...decodedShell, where: shallowQuery.where };
  const decoded = routeBy === undefined ? decodedWhere : { ...decodedWhere, routeBy };
  if (decoded.select.length === 0) {
    return yield* Effect.fail(invalidQuery(topic, "Query select must include at least one field"));
  }
  yield* validateWindow(topic, decoded.offset, decoded.limit);
  if (isRawQueryForTopic(topicSchema, decoded)) {
    const where = yield* decodeWhere(topic, topicSchema, decoded.where);
    const trusted = validatedRawQuery<object>({
      select: decoded.select,
      ...definedFields(where, (where) => ({ where })),
      ...definedFields(routeBy, (routeBy) => ({ routeBy })),
      ...definedFields(decoded.orderBy, (orderBy) => ({ orderBy })),
      ...definedFields(decoded.offset, (offset) => ({ offset })),
      ...definedFields(decoded.limit, (limit) => ({ limit })),
    });
    yield* validateSourceRoute(config, topic, trusted);
    return trusted;
  }
  return yield* Effect.fail(
    invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
  );
});

export function viewServerDecodeRawQuery<
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: ProtocolQueryInput,
): Effect.Effect<ViewServerValidatedRawQuery<TopicRow<Topics, Topic>>, ViewServerRuntimeError>;
export function viewServerDecodeRawQuery(
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: ProtocolQueryInput,
): Effect.Effect<unknown, ViewServerRuntimeError> {
  return decodeRawQuery(config, topic, query);
}
