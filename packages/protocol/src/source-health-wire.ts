import type {
  TopicRow,
  ViewServerConfigTopicShape,
  ViewServerRuntimeError,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import {
  sourceHealthContractSchemas,
  type SourceHealthResultForDefinition,
} from "@effect-view-server/source-adapter";
import { isSourceDefinition } from "@effect-view-server/source-adapter/internal";
import { BigDecimal, Effect, Option, Result, Schema } from "effect";
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

export type CompiledSourceHealthContract = {
  readonly adapterIdentity: {
    readonly name: string;
    readonly version?: string | undefined;
  };
  readonly health: Schema.Codec<unknown, unknown, never, never>;
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

export const compileSourceHealthContract = Effect.fn("ViewServerProtocol.sourceHealth.compile")(
  function* <Topics extends ViewServerConfigTopicShape>(
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
    const healthContract = sourceHealthContractSchemas({
      adapterFailure,
      route,
      adapterMetrics,
      rejectionLocation,
      lifecycle,
    });
    return {
      adapterIdentity: source.identity,
      health: healthContract.health,
      lifecycle,
      result: healthContract.result,
      route,
      routeFields,
    };
  },
);

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

const exactAdapterIdentity = (
  expected: CompiledSourceHealthContract["adapterIdentity"],
  candidate: unknown,
): boolean => {
  if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
    return false;
  }
  const expectedKeys = expected.version === undefined ? ["name"] : ["name", "version"];
  const keys = Result.try(() => Reflect.ownKeys(candidate));
  return (
    Result.isSuccess(keys) &&
    keys.success.length === expectedKeys.length &&
    keys.success.every((key) => typeof key === "string" && expectedKeys.includes(key)) &&
    readProperty(candidate, "name") === expected.name &&
    readProperty(candidate, "version") === expected.version
  );
};

export const requireExactSourceHealth = Effect.fn("ViewServerProtocol.sourceHealth.exact")(
  function* (topic: string, contract: CompiledSourceHealthContract, candidate: unknown) {
    if (!exactAdapterIdentity(contract.adapterIdentity, readProperty(candidate, "adapter"))) {
      return yield* Effect.fail(
        invalidSourceHealth(
          topic,
          `Source Health adapter identity must match ${contract.adapterIdentity.name}.`,
        ),
      );
    }
    if (
      contract.lifecycle === "leased" &&
      readProperty(readProperty(candidate, "target"), "_tag") === "Leased"
    ) {
      yield* requireExactRoute(
        topic,
        contract.routeFields,
        readProperty(readProperty(candidate, "target"), "route"),
      );
    }
  },
);

const equalRouteValue = (left: unknown, right: unknown): boolean => {
  if (BigDecimal.isBigDecimal(left)) {
    return (
      BigDecimal.isBigDecimal(right) &&
      left.value === right.value &&
      Object.is(left.scale, right.scale)
    );
  }
  return Object.is(left, right);
};

const requireExactLeasedHealthRoutes = Effect.fn(
  "ViewServerProtocol.sourceHealth.leasedRoutes.exact",
)(function* (topic: string, contract: CompiledSourceHealthContract, candidate: unknown) {
  const routeFields = contract.routeFields;
  const tag = readProperty(candidate, "_tag");
  if (tag === "Inactive") {
    yield* requireExactRoute(topic, routeFields, readProperty(candidate, "route"));
    return;
  }
  if (tag === "Active") {
    const outerRoute = readProperty(candidate, "route");
    yield* requireExactRoute(topic, routeFields, outerRoute);
    const health = readProperty(candidate, "health");
    const target = readProperty(health, "target");
    const targetRoute = readProperty(target, "route");
    yield* requireExactRoute(topic, routeFields, targetRoute);
    if (
      readProperty(target, "_tag") !== "Leased" ||
      !Object.keys(routeFields).every((field) =>
        equalRouteValue(readProperty(outerRoute, field), readProperty(targetRoute, field)),
      )
    ) {
      return yield* Effect.fail(
        invalidSourceHealth(
          topic,
          "Active Leased Source Health route must match its target route.",
        ),
      );
    }
    yield* requireExactSourceHealth(topic, contract, health);
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
      yield* requireExactLeasedHealthRoutes(topic, contract, value);
    } else {
      yield* requireExactSourceHealth(topic, contract, value);
    }
    return yield* encodeJsonFieldValue(contract.result, value, codecErrors(topic));
  },
);

export const viewServerDecodeSourceHealth: <
  Topics extends ViewServerConfigTopicShape,
  Topic extends Extract<keyof Topics, string>,
>(
  config: ViewServerTopicConfig<Topics>,
  topic: Topic,
  value: unknown,
) => Effect.Effect<ViewServerDecodedSourceHealth<Topics, Topic>, ViewServerRuntimeError> =
  Effect.fn("ViewServerProtocol.sourceHealth.decode")(function* <
    Topics extends ViewServerConfigTopicShape,
    Topic extends Extract<keyof Topics, string>,
  >(config: ViewServerTopicConfig<Topics>, topic: Topic, value: unknown) {
    const contract = yield* compileSourceHealthContract(config, topic);
    if (contract.lifecycle === "leased") {
      yield* requireExactLeasedHealthRoutes(topic, contract, value);
    } else {
      yield* requireExactSourceHealth(topic, contract, value);
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
  });
