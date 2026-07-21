import type {
  RowSchema,
  TopicDefinitions,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
import { snapshotViewServerQuery } from "@effect-view-server/effect-utils";
import { Effect, Schema } from "effect";
import {
  decodeWhere as decodeWhereExpressions,
  encodeWhere as encodeWhereExpressions,
} from "./protocol-field-filter-codec";
import {
  decodeRouteBy as decodeRouteByFields,
  encodeRouteBy as encodeRouteByFields,
} from "./protocol-route-field-codec";
import { ViewServerHealthQuerySchema } from "./protocol-query-schema";
import {
  isProtocolPlainRecord,
  protocolDenseArray,
  protocolRecordSnapshot,
} from "./protocol-structural-value";

export const strictParseOptions = {
  onExcessProperty: "error",
} as const;

export const isRecord = isProtocolPlainRecord;

type OwnedProtocolQueryInput = Readonly<Record<string, unknown>>;

export const isGroupedQueryInput = (
  query: OwnedProtocolQueryInput,
): query is OwnedProtocolQueryInput & { readonly groupBy: unknown } =>
  Object.hasOwn(query, "groupBy");

export const invalidTopic = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidTopic",
  message: `Unknown topic: ${topic}`,
  topic,
});

export const invalidQuery = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

export const ownProtocolQueryInput = Effect.fn("ViewServerProtocol.query.ownInput")(function* (
  topic: string,
  query: unknown,
) {
  return yield* Effect.try({
    try: (): OwnedProtocolQueryInput => snapshotViewServerQuery(query),
    catch: () => invalidQuery(topic, "Query input could not be inspected"),
  });
});

export const requireRouteByRecord = Effect.fn("ViewServerProtocol.query.routeBy.requireRecord")(
  function* (topic: string, routeBy: unknown) {
    const snapshot = protocolRecordSnapshot(routeBy);
    if (snapshot === undefined) {
      return yield* Effect.fail(invalidQuery(topic, "Query routeBy must be a plain object"));
    }
    return snapshot.source;
  },
);

const queryArrayFields = new Set(["select", "where", "groupBy", "orderBy"]);

export const shallowQueryInput = Effect.fn("ViewServerProtocol.query.shallowInput")(function* (
  topic: string,
  query: OwnedProtocolQueryInput,
) {
  const input: Record<string, unknown> = {};
  let where: ReadonlyArray<unknown> | undefined = undefined;
  let hasRouteBy = false;
  let routeBy: unknown = undefined;
  for (const [key, value] of Object.entries(query)) {
    if (
      queryArrayFields.has(key) &&
      Array.isArray(value) &&
      protocolDenseArray(value) === undefined
    ) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query ${key} must be a dense array without extra properties`),
      );
    }
    if (key === "where" && Array.isArray(value)) {
      where = value;
      Object.defineProperty(input, key, {
        configurable: true,
        enumerable: true,
        value: [],
        writable: true,
      });
    } else if (key === "routeBy") {
      hasRouteBy = true;
      routeBy = value;
      Object.defineProperty(input, key, {
        configurable: true,
        enumerable: true,
        value: {},
        writable: true,
      });
    } else {
      Object.defineProperty(input, key, {
        configurable: true,
        enumerable: true,
        value,
        writable: true,
      });
    }
  }
  return { input: Object.freeze(input), where, hasRouteBy, routeBy };
});

export const hasTopic = <Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(config.topics, topic);

export const hasOwnField = (schema: RowSchema, field: string): boolean =>
  Object.hasOwn(schema.fields, field);

export const getFieldSchema = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
) => {
  const schema = config.topics[topic]!.schema;
  return hasOwnField(schema, field) ? schema.fields[field] : undefined;
};

export const hasOnlyKnownFields = (schema: RowSchema, fields: Iterable<string>): boolean =>
  Array.from(fields).every((field) => hasOwnField(schema, field));

export const validateWindow = Effect.fn("ViewServerProtocol.query.window.validate")(function* (
  topic: string,
  offset: number | undefined,
  limit: number | undefined,
) {
  if (offset !== undefined && (!Number.isSafeInteger(offset) || offset < 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query offset must be a non-negative integer"));
  }
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query limit must be a non-negative integer"));
  }
});

export const validateSourceRoute = Effect.fn("ViewServerProtocol.query.route.validate")(function* <
  Topics extends TopicDefinitions,
>(config: { readonly topics: Topics }, topic: string, query: unknown) {
  const message = validateLiveQuerySourceRoute(config.topics, topic, query);
  if (message !== undefined) {
    return yield* Effect.fail(invalidQuery(topic, message));
  }
});

export const viewServerDecodeTopic: <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
) => Effect.Effect<Extract<keyof Topics, string>, ViewServerRuntimeError> = Effect.fn(
  "ViewServerProtocol.topic.decode",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
) {
  if (hasTopic(config, topic)) {
    return topic;
  }
  return yield* Effect.fail(invalidTopic(topic));
});

export const viewServerDecodeHealthQuery = Effect.fn("ViewServerProtocol.healthQuery.decode")(
  function* (topic: string, query: unknown) {
    const decoded = yield* Schema.decodeUnknownEffect(ViewServerHealthQuerySchema)(
      query,
      strictParseOptions,
    ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
    if (decoded.select.length !== 1 || decoded.select[0] !== "id") {
      return yield* Effect.fail(invalidQuery(topic, "Health query select must be exactly: id"));
    }
    return decoded;
  },
);

export const encodeWhere = Effect.fn("ViewServerProtocol.query.where.encode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(config: { readonly topics: Topics }, topic: Topic, where: ReadonlyArray<unknown> | undefined) {
  return yield* encodeWhereExpressions(topic, config.topics[topic]!.schema, where);
});

export const decodeWhere = Effect.fn("ViewServerProtocol.query.where.decode")(function* (
  topic: string,
  schema: RowSchema,
  where: ReadonlyArray<unknown> | undefined,
) {
  return yield* decodeWhereExpressions(topic, schema, where);
});

export const encodeRouteBy = Effect.fn("ViewServerProtocol.query.routeBy.encode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(config: { readonly topics: Topics }, topic: Topic, routeBy: unknown) {
  return yield* encodeRouteByFields(topic, config.topics[topic]!.schema, routeBy);
});

export const decodeRouteBy = Effect.fn("ViewServerProtocol.query.routeBy.decode")(function* (
  topic: string,
  schema: RowSchema,
  routeBy: unknown,
) {
  return yield* decodeRouteByFields(topic, schema, routeBy);
});
