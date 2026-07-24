import type {
  TopicRow,
  ViewServerConfigTopicShape,
  ViewServerRuntimeError,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import {
  sourceHealthSchema,
  type SourceHealthResultForDefinition,
} from "@effect-view-server/source-adapter";
import { isSourceDefinition } from "@effect-view-server/source-adapter/internal";
import { Effect, Option, Result, Schema } from "effect";
import { decodeJsonFieldValue, encodeJsonFieldValue } from "./protocol-json-field-codec";

const ViewServerSourceHealthRoutePayloadSchema = Schema.Record(Schema.String, Schema.Json);

export const ViewServerSourceHealthPayloadSchema = Schema.Struct({
  topic: Schema.String,
  routeBy: Schema.optionalKey(ViewServerSourceHealthRoutePayloadSchema),
});

export const ViewServerWireSourceHealthSchema = Schema.Json;

export type ViewServerSourceHealthPayload = typeof ViewServerSourceHealthPayloadSchema.Type;
export type ViewServerWireSourceHealth = typeof ViewServerWireSourceHealthSchema.Type;

const invalidSourceHealth = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

type CompiledSourceHealthContract = {
  readonly lifecycle: "materialized" | "leased";
  readonly result: Schema.Codec<unknown, unknown, never, never>;
  readonly route: Schema.Codec<Readonly<Record<string, unknown>>, unknown, never, never>;
  readonly routeFields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>;
};

type TopicSourceDefinition<
  Topics extends ViewServerConfigTopicShape,
  Topic extends keyof Topics,
> = Topics[Topic] extends { readonly source: infer Definition } ? Definition : never;

export type ViewServerDecodedSourceHealth<
  Topics extends ViewServerConfigTopicShape,
  Topic extends Extract<keyof Topics, string>,
> = SourceHealthResultForDefinition<TopicSourceDefinition<Topics, Topic>, TopicRow<Topics, Topic>>;

const compileSourceHealthContract = Effect.fn("ViewServerProtocol.sourceHealth.compile")(function* <
  Topics extends ViewServerConfigTopicShape,
>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
): Effect.fn.Return<CompiledSourceHealthContract, ViewServerRuntimeError> {
  const definition = config.topics[topic];
  if (definition === undefined) {
    return yield* Effect.fail(invalidSourceHealth(topic, `Topic ${topic} is not configured.`));
  }
  const source = Reflect.get(definition, "source");
  if (!isSourceDefinition(source)) {
    return yield* Effect.fail(
      invalidSourceHealth(topic, `Topic ${topic} has no canonical Source Definition.`),
    );
  }
  const lifecycle = source.lifecycle;
  const adapter = source.adapter;
  const adapterFailure = adapter.failureSchema;
  const declaration = Option.getOrThrow(
    Option.fromUndefinedOr(lifecycle === "materialized" ? adapter.materialized : adapter.leased),
  );
  const adapterMetrics = Reflect.get(declaration, "metrics");
  const rejectionLocation = Reflect.get(declaration, "rejectionLocation");
  const routeBy = source.routeBy;
  const routeFields: Record<string, Schema.Codec<unknown, unknown, never, never>> = {};
  for (const field of routeBy) {
    const fieldSchema = definition.schema.fields[field];
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        invalidSourceHealth(topic, `Topic ${topic} Source route field ${field} is missing.`),
      );
    }
    routeFields[field] = fieldSchema;
  }
  const route = Schema.Struct(routeFields);
  const health = sourceHealthSchema({
    adapterFailure,
    route,
    adapterMetrics,
    rejectionLocation,
  });
  const result =
    lifecycle === "materialized"
      ? health
      : Schema.Union([
          Schema.TaggedStruct("Inactive", { route }),
          Schema.TaggedStruct("Active", { route, health }),
        ]);
  return { lifecycle, result, route, routeFields };
});

const codecErrors = (topic: string) => ({
  invalid: (message: string) =>
    invalidSourceHealth(topic, `Invalid Source Health value: ${message}`),
  notJsonSafe: (message: string) =>
    invalidSourceHealth(topic, `Source Health is not wire-safe: ${message}`),
});

const exactRouteKeys = (
  candidate: unknown,
  routeFields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
): boolean => {
  if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
    return false;
  }
  const expected = Object.keys(routeFields);
  const keys = Result.try(() => Reflect.ownKeys(candidate));
  return (
    Result.isSuccess(keys) &&
    keys.success.length === expected.length &&
    keys.success.every((key) => typeof key === "string" && expected.includes(key))
  );
};

const requireExactRoute = Effect.fn("ViewServerProtocol.sourceHealth.route.exact")(function* (
  topic: string,
  routeFields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
  candidate: unknown,
) {
  if (!exactRouteKeys(candidate, routeFields)) {
    return yield* Effect.fail(
      invalidSourceHealth(
        topic,
        `Leased Source routeBy must contain all and only: ${Object.keys(routeFields).join(", ")}.`,
      ),
    );
  }
});

const readProperty = (candidate: unknown, key: string): unknown => {
  if (typeof candidate !== "object" || candidate === null) {
    return undefined;
  }
  const property = Result.try(() => Reflect.get(candidate, key));
  return Result.isSuccess(property) ? property.success : undefined;
};

const requireExactLeasedHealthRoutes = Effect.fn(
  "ViewServerProtocol.sourceHealth.leasedRoutes.exact",
)(function* (
  topic: string,
  routeFields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
  candidate: unknown,
) {
  const tag = readProperty(candidate, "_tag");
  if (tag === "Inactive") {
    yield* requireExactRoute(topic, routeFields, readProperty(candidate, "route"));
    return;
  }
  if (tag === "Active") {
    yield* requireExactRoute(topic, routeFields, readProperty(candidate, "route"));
    const health = readProperty(candidate, "health");
    const target = readProperty(health, "target");
    yield* requireExactRoute(topic, routeFields, readProperty(target, "route"));
  }
});

export const viewServerEncodeSourceHealthRequest = Effect.fn(
  "ViewServerProtocol.sourceHealth.request.encode",
)(function* <Topics extends ViewServerConfigTopicShape>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
  route: ReadonlyArray<object>,
) {
  const contract = yield* compileSourceHealthContract(config, topic);
  if (contract.lifecycle === "materialized") {
    if (route.length !== 0) {
      return yield* Effect.fail(
        invalidSourceHealth(topic, `Materialized Source Topic ${topic} does not accept routeBy.`),
      );
    }
    return { topic };
  }
  const candidate = route[0];
  if (route.length !== 1 || candidate === undefined) {
    return yield* Effect.fail(
      invalidSourceHealth(topic, `Leased Source Topic ${topic} requires exact routeBy.`),
    );
  }
  yield* requireExactRoute(topic, contract.routeFields, candidate);
  const routeBy: Record<string, Schema.Json> = {};
  for (const [field, fieldSchema] of Object.entries(contract.routeFields)) {
    routeBy[field] = yield* encodeJsonFieldValue(
      fieldSchema,
      readProperty(candidate, field),
      codecErrors(topic),
    );
  }
  return { topic, routeBy };
});

export const viewServerDecodeSourceHealthRequest = Effect.fn(
  "ViewServerProtocol.sourceHealth.request.decode",
)(function* <Topics extends ViewServerConfigTopicShape>(
  config: ViewServerTopicConfig<Topics>,
  payload: ViewServerSourceHealthPayload,
) {
  const contract = yield* compileSourceHealthContract(config, payload.topic);
  if (contract.lifecycle === "materialized") {
    if (payload.routeBy !== undefined) {
      return yield* Effect.fail(
        invalidSourceHealth(
          payload.topic,
          `Materialized Source Topic ${payload.topic} does not accept routeBy.`,
        ),
      );
    }
    return { topic: payload.topic, route: [] };
  }
  if (payload.routeBy === undefined) {
    return yield* Effect.fail(
      invalidSourceHealth(
        payload.topic,
        `Leased Source Topic ${payload.topic} requires exact routeBy.`,
      ),
    );
  }
  yield* requireExactRoute(payload.topic, contract.routeFields, payload.routeBy);
  const routeBy = yield* decodeJsonFieldValue(
    contract.route,
    payload.routeBy,
    codecErrors(payload.topic),
  );
  return { topic: payload.topic, route: [routeBy] };
});

export const viewServerEncodeSourceHealth = Effect.fn("ViewServerProtocol.sourceHealth.encode")(
  function* <Topics extends ViewServerConfigTopicShape>(
    config: ViewServerTopicConfig<Topics>,
    topic: string,
    value: unknown,
  ) {
    const contract = yield* compileSourceHealthContract(config, topic);
    if (contract.lifecycle === "leased") {
      yield* requireExactLeasedHealthRoutes(topic, contract.routeFields, value);
    }
    return yield* encodeJsonFieldValue(contract.result, value, codecErrors(topic));
  },
);

export const viewServerDecodeSourceHealth = Effect.fn("ViewServerProtocol.sourceHealth.decode")(
  function* <
    Topics extends ViewServerConfigTopicShape,
    Topic extends Extract<keyof Topics, string>,
  >(
    config: ViewServerTopicConfig<Topics>,
    topic: Topic,
    value: unknown,
  ): Effect.fn.Return<ViewServerDecodedSourceHealth<Topics, Topic>, ViewServerRuntimeError> {
    const contract = yield* compileSourceHealthContract(config, topic);
    if (contract.lifecycle === "leased") {
      yield* requireExactLeasedHealthRoutes(topic, contract.routeFields, value);
    }
    const decoded = yield* decodeJsonFieldValue(contract.result, value, codecErrors(topic));
    if (!isDecodedSourceHealth(contract.result, decoded)) {
      return yield* Effect.fail(
        invalidSourceHealth(topic, "Configured Source Health decoder returned an invalid value."),
      );
    }
    return decoded;

    function isDecodedSourceHealth(
      codec: Schema.Codec<unknown, unknown, never, never>,
      candidate: unknown,
    ): candidate is ViewServerDecodedSourceHealth<Topics, Topic> {
      return Schema.is(codec)(candidate);
    }
  },
);
