import type {
  RowSchema,
  TopicDefinitions,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
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

export const strictParseOptions = {
  onExcessProperty: "error",
} as const;

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isGroupedQueryInput = (query: unknown): query is { readonly groupBy: unknown } =>
  isRecord(query) && Object.hasOwn(query, "groupBy");

type ShallowWhereQueryInput = {
  readonly input: unknown;
  readonly where: ReadonlyArray<unknown> | undefined;
};

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

export const shallowWhereQueryInput = Effect.fn("ViewServerProtocol.query.shallowWhereInput")(
  function* (topic: string, query: unknown) {
    return yield* Effect.try({
      try: (): ShallowWhereQueryInput => {
        if (!isRecord(query)) {
          return { input: query, where: undefined };
        }
        if (Object.getOwnPropertySymbols(query).length > 0) {
          throw new TypeError("Query fields must not use symbol keys.");
        }
        const input: Record<string, unknown> = {};
        let where: ReadonlyArray<unknown> | undefined = undefined;
        for (const key of Object.getOwnPropertyNames(query)) {
          const descriptor = Object.getOwnPropertyDescriptor(query, key);
          if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
            throw new TypeError("Query fields must be own enumerable data properties.");
          }
          if (key === "where" && Array.isArray(descriptor.value)) {
            where = descriptor.value;
            Object.defineProperty(input, key, {
              configurable: true,
              enumerable: true,
              value: [],
              writable: true,
            });
          } else {
            Object.defineProperty(input, key, {
              configurable: true,
              enumerable: true,
              value: descriptor.value,
              writable: true,
            });
          }
        }
        return { input: Object.freeze(input), where };
      },
      catch: () => invalidQuery(topic, "Query input could not be inspected"),
    });
  },
);

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
>(
  config: { readonly topics: Topics },
  topic: Topic,
  routeBy: Readonly<Record<string, unknown>> | undefined,
) {
  return yield* encodeRouteByFields(topic, config.topics[topic]!.schema, routeBy);
});

export const decodeRouteBy = Effect.fn("ViewServerProtocol.query.routeBy.decode")(function* (
  topic: string,
  schema: RowSchema,
  routeBy: Readonly<Record<string, unknown>> | undefined,
) {
  return yield* decodeRouteByFields(topic, schema, routeBy);
});
