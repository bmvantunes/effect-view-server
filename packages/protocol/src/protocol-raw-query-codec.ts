import type {
  FieldKey,
  OrderBy,
  RowSchema,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  Where,
} from "@effect-view-server/config";
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
  shallowWhereQueryInput,
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

export type ViewServerValidatedRawQuery<Row> = TrustedRawQuery<Row>;

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
>(config: { readonly topics: Topics }, topic: Topic, query: unknown) {
  if (!hasTopic(config, topic)) {
    return yield* Effect.fail(invalidTopic(topic));
  }
  const shallowQuery = yield* shallowWhereQueryInput(topic, query);
  const decodedShell = yield* Schema.decodeUnknownEffect(LooseWireRawQuerySchema)(
    shallowQuery.input,
    strictParseOptions,
  ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
  const decoded =
    shallowQuery.where === undefined
      ? decodedShell
      : { ...decodedShell, where: shallowQuery.where };
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
  const routeBy = yield* encodeRouteBy(config, topic, decoded.routeBy);
  const wireQuery: ViewServerWireRawQuery = {
    select: decoded.select,
    ...(where === undefined ? {} : { where }),
    ...(routeBy === undefined ? {} : { routeBy }),
    ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
    ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
    ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
  };
  return wireQuery;
});

function validatedRawQuery<Row>(query: LooseWireRawQuery): ViewServerValidatedRawQuery<Row>;
function validatedRawQuery(query: LooseWireRawQuery) {
  return query;
}

export const viewServerDecodeRawQuery: <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: unknown,
) => Effect.Effect<ViewServerValidatedRawQuery<TopicRow<Topics, Topic>>, ViewServerRuntimeError> =
  Effect.fn("ViewServerProtocol.query.decode")(function* <
    const Topics extends TopicDefinitions,
    Topic extends Extract<keyof Topics, string>,
  >(config: { readonly topics: Topics }, topic: Topic, query: unknown) {
    const decodedTopic = yield* viewServerDecodeTopic(config, topic);
    const shallowQuery = yield* shallowWhereQueryInput(topic, query);
    const decodedShell = yield* Schema.decodeUnknownEffect(LooseWireRawQuerySchema)(
      shallowQuery.input,
      strictParseOptions,
    ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
    const decoded =
      shallowQuery.where === undefined
        ? decodedShell
        : { ...decodedShell, where: shallowQuery.where };
    if (decoded.select.length === 0) {
      return yield* Effect.fail(
        invalidQuery(topic, "Query select must include at least one field"),
      );
    }
    yield* validateWindow(topic, decoded.offset, decoded.limit);
    const topicSchema = config.topics[decodedTopic]!.schema;
    if (isRawQueryForTopic(topicSchema, decoded)) {
      const where = yield* decodeWhere(topic, topicSchema, decoded.where);
      const routeBy = yield* decodeRouteBy(topic, topicSchema, decoded.routeBy);
      const trusted = validatedRawQuery<TopicRow<Topics, Topic>>({
        select: decoded.select,
        ...(where === undefined ? {} : { where }),
        ...(routeBy === undefined ? {} : { routeBy }),
        ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
        ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
        ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
      });
      yield* validateSourceRoute(config, topic, trusted);
      return trusted;
    }
    return yield* Effect.fail(
      invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
    );
  });
