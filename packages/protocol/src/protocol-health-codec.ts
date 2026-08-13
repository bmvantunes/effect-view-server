import type {
  TopicDefinitions,
  ViewServerHealth,
  ViewServerTopicConfig,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { isSourceDefinition } from "@effect-view-server/source-adapter/internal";
import { Effect, Result, Schema } from "effect";
import { decodeJsonFieldValue, encodeJsonFieldValue } from "./protocol-json-field-codec";
import {
  configuredTopicNames,
  hasConfiguredTopic,
  invalidHealthRow,
} from "./protocol-health-common";
import {
  ViewServerHealthBaseSchema,
  ViewServerHealthSchema,
  type ViewServerWireHealth,
} from "./protocol-health-schema";
import {
  compileSourceHealthContract,
  validateExactSourceHealth,
  type CompiledSourceHealthContract,
} from "./source-health-wire";

export {
  viewServerDecodeHealthSummaryEvent,
  viewServerEncodeHealthSummaryEvent,
} from "./protocol-health-summary-codec";
export {
  ViewServerHealthSchema,
  ViewServerHealthSummaryRowSchema,
  ViewServerHealthTopicRowSchema,
} from "./protocol-health-schema";
export type { ViewServerWireHealth } from "./protocol-health-schema";
export {
  viewServerDecodeHealthTopicEvent,
  viewServerEncodeHealthTopicEvent,
} from "./protocol-health-topic-codec";

const invalidHealthPayload = (error: { readonly message: string }): ViewServerRuntimeError =>
  invalidHealthRow("__view_server_health", `Invalid health payload: ${error.message}`);

type NormalizedViewServerHealth = typeof ViewServerHealthBaseSchema.Type & {
  readonly sources: Readonly<Record<string, unknown>>;
};

function typedHealth<Topics extends TopicDefinitions>(
  health: NormalizedViewServerHealth,
): ViewServerHealth<Topics>;
function typedHealth(health: NormalizedViewServerHealth): unknown {
  return health;
}

const readProperty = (candidate: object, key: string): unknown => {
  const property = Result.try(() => Reflect.get(candidate, key));
  return Result.isSuccess(property) ? property.success : undefined;
};

const sourceRecordKeys = Effect.fn("ViewServerProtocol.health.sources.keys")(function* (candidate: {
  readonly sources?: unknown;
}) {
  const sources = candidate.sources;
  if (typeof sources !== "object" || sources === null || Array.isArray(sources)) {
    return yield* Effect.fail(
      invalidHealthRow(
        "__view_server_health",
        "Health payload sources must be a topic-keyed object.",
      ),
    );
  }
  const keys = Result.try(() => Reflect.ownKeys(sources));
  if (Result.isFailure(keys)) {
    return yield* Effect.fail(
      invalidHealthRow(
        "__view_server_health",
        "Health payload sources must contain string topic keys.",
      ),
    );
  }
  const stringKeys: Array<string> = [];
  for (const key of keys.success) {
    if (typeof key !== "string") {
      return yield* Effect.fail(
        invalidHealthRow(
          "__view_server_health",
          "Health payload sources must contain string topic keys.",
        ),
      );
    }
    stringKeys.push(key);
  }
  return {
    keys: stringKeys,
    sources,
  };
});

const configuredSourceTopics = <Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
): ReadonlyArray<{
  readonly lifecycle: "materialized" | "leased";
  readonly topic: string;
}> => {
  const topics: Array<{
    readonly lifecycle: "materialized" | "leased";
    readonly topic: string;
  }> = [];
  for (const [topic, definition] of Object.entries(config.topics)) {
    const source = Reflect.get(definition, "source");
    if (isSourceDefinition(source)) {
      topics.push({
        lifecycle: source.lifecycle,
        topic,
      });
    }
  }
  return topics;
};

const validateTopicKeys = Effect.fn("ViewServerProtocol.health.topics.validate")(function* <
  Topics extends TopicDefinitions,
>(config: ViewServerTopicConfig<Topics>, health: typeof ViewServerHealthBaseSchema.Type) {
  const configuredTopics = configuredTopicNames(config);
  const healthTopics = Object.keys(health.engine.topics);
  for (const topic of configuredTopics) {
    if (!Object.hasOwn(health.engine.topics, topic)) {
      return yield* Effect.fail(
        invalidHealthRow(topic, `Health payload is missing topic: ${topic}`),
      );
    }
  }
  for (const topic of healthTopics) {
    if (!hasConfiguredTopic(config, topic)) {
      return yield* Effect.fail(
        invalidHealthRow(topic, `Health payload references unknown topic: ${topic}`),
      );
    }
  }
});

const validateSourceTopicKeys = Effect.fn("ViewServerProtocol.health.sourceTopics.validate")(
  function* <Topics extends TopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    keys: ReadonlyArray<string>,
  ) {
    const configuredSources = configuredSourceTopics(config);
    const configuredSourceSet = new Set(configuredSources.map(({ topic }) => topic));
    const keySet = new Set(keys);
    for (const source of configuredSources) {
      if (source.lifecycle === "leased" && !keySet.has(source.topic)) {
        return yield* Effect.fail(
          invalidHealthRow(source.topic, `Health payload is missing source topic: ${source.topic}`),
        );
      }
    }
    for (const topic of keys) {
      if (!configuredSourceSet.has(topic)) {
        return yield* Effect.fail(
          invalidHealthRow(
            topic,
            `Health payload references unknown or source-free source topic: ${topic}`,
          ),
        );
      }
    }
    return configuredSources.map(({ topic }) => topic).filter((topic) => keySet.has(topic));
  },
);

const sourceHealthCodecErrors = (topic: string) => ({
  invalid: (message: string) =>
    invalidHealthRow(topic, `Invalid aggregate Source Health value: ${message}`),
  notJsonSafe: (message: string) =>
    invalidHealthRow(topic, `Aggregate Source Health is not wire-safe: ${message}`),
});

const projectSources = Effect.fn("ViewServerProtocol.health.sources.project")(function* <
  Topics extends TopicDefinitions,
  Output,
>(
  config: ViewServerTopicConfig<Topics>,
  candidate: { readonly sources?: unknown },
  project: (
    topic: string,
    contract: CompiledSourceHealthContract,
    value: unknown,
  ) => Effect.Effect<Output, ViewServerRuntimeError>,
): Effect.fn.Return<Readonly<Record<string, Output>>, ViewServerRuntimeError> {
  const { keys, sources } = yield* sourceRecordKeys(candidate);
  const sourceTopics = yield* validateSourceTopicKeys(config, keys);
  const projected: Record<string, Output> = {};
  for (const topic of sourceTopics) {
    const contract = yield* compileSourceHealthContract(config, topic).pipe(
      Effect.mapError((error) => invalidHealthRow(topic, error.message)),
    );
    projected[topic] = yield* project(topic, contract, readProperty(sources, topic));
  }
  return projected;
});

const projectSourceValues = Effect.fn("ViewServerProtocol.health.sourceValues.project")(function* <
  Output,
>(
  topic: string,
  contract: CompiledSourceHealthContract,
  value: unknown,
  project: (
    value: unknown,
    errors: ReturnType<typeof sourceHealthCodecErrors>,
  ) => Effect.Effect<Output, ViewServerRuntimeError>,
): Effect.fn.Return<Output | ReadonlyArray<Output>, ViewServerRuntimeError> {
  if (contract.lifecycle === "materialized") {
    yield* validateExactSourceHealth(topic, contract, value).pipe(
      Effect.mapError((error) => invalidHealthRow(topic, error.message)),
    );
    return yield* project(value, sourceHealthCodecErrors(topic));
  }
  if (!Array.isArray(value)) {
    return yield* Effect.fail(
      invalidHealthRow(topic, `Leased aggregate Source Health for ${topic} must be an array.`),
    );
  }
  const active: Array<Output> = [];
  for (const health of value) {
    yield* validateExactSourceHealth(topic, contract, health).pipe(
      Effect.mapError((error) => invalidHealthRow(topic, error.message)),
    );
    active.push(yield* project(health, sourceHealthCodecErrors(topic)));
  }
  return active;
});

const encodeSources = <Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  candidate: { readonly sources?: unknown },
): Effect.Effect<Readonly<Record<string, Schema.Json>>, ViewServerRuntimeError> =>
  projectSources(config, candidate, (topic, contract, value) =>
    projectSourceValues(topic, contract, value, (health, errors) =>
      encodeJsonFieldValue(contract.health, health, errors),
    ),
  );

const decodeSources = <Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  candidate: { readonly sources?: unknown },
): Effect.Effect<Readonly<Record<string, unknown>>, ViewServerRuntimeError> =>
  projectSources(config, candidate, (topic, contract, value) =>
    projectSourceValues(topic, contract, value, (health, errors) =>
      decodeJsonFieldValue(contract.health, health, errors),
    ),
  );

export const viewServerEncodeHealth = Effect.fn("ViewServerProtocol.health.encode")(function* <
  const Topics extends TopicDefinitions,
>(config: ViewServerTopicConfig<Topics>, health: ViewServerHealth<NoInfer<Topics>>) {
  const normalizedBase = yield* Schema.decodeUnknownEffect(ViewServerHealthBaseSchema)(health).pipe(
    Effect.mapError(invalidHealthPayload),
  );
  yield* validateTopicKeys(config, normalizedBase);
  const sources = yield* encodeSources(config, health);
  return yield* Schema.decodeUnknownEffect(ViewServerHealthSchema)({
    ...normalizedBase,
    sources,
  }).pipe(Effect.mapError(invalidHealthPayload));
});

export const viewServerDecodeHealth = Effect.fn("ViewServerProtocol.health.decode")(function* <
  const Topics extends TopicDefinitions,
>(config: ViewServerTopicConfig<Topics>, health: ViewServerWireHealth) {
  const normalizedWireHealth = yield* Schema.decodeUnknownEffect(ViewServerHealthSchema)(
    health,
  ).pipe(Effect.mapError(invalidHealthPayload));
  const normalizedBase = yield* Schema.decodeUnknownEffect(ViewServerHealthBaseSchema)(
    normalizedWireHealth,
  ).pipe(Effect.mapError(invalidHealthPayload));
  yield* validateTopicKeys(config, normalizedBase);
  const sources = yield* decodeSources(config, normalizedWireHealth);
  return typedHealth<Topics>({
    ...normalizedBase,
    sources,
  });
});
