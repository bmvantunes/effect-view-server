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
import { compileSourceHealthContract, requireExactSourceHealth } from "./source-health-wire";

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

const configuredSourceTopicNames = <Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
): ReadonlyArray<string> => {
  const topics: Array<string> = [];
  for (const [topic, definition] of Object.entries(config.topics)) {
    if (isSourceDefinition(Reflect.get(definition, "source"))) {
      topics.push(topic);
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
    const configuredSources = configuredSourceTopicNames(config);
    for (const topic of configuredSources) {
      if (!keys.includes(topic)) {
        return yield* Effect.fail(
          invalidHealthRow(topic, `Health payload is missing source topic: ${topic}`),
        );
      }
    }
    for (const topic of keys) {
      if (!configuredSources.includes(topic)) {
        return yield* Effect.fail(
          invalidHealthRow(
            topic,
            `Health payload references unknown or source-free source topic: ${topic}`,
          ),
        );
      }
    }
    return configuredSources;
  },
);

const sourceHealthCodecErrors = (topic: string) => ({
  invalid: (message: string) =>
    invalidHealthRow(topic, `Invalid aggregate Source Health value: ${message}`),
  notJsonSafe: (message: string) =>
    invalidHealthRow(topic, `Aggregate Source Health is not wire-safe: ${message}`),
});

const encodeSources = Effect.fn("ViewServerProtocol.health.sources.encode")(function* <
  Topics extends TopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  candidate: { readonly sources?: unknown },
): Effect.fn.Return<Readonly<Record<string, Schema.Json>>, ViewServerRuntimeError> {
  const { keys, sources } = yield* sourceRecordKeys(candidate);
  const sourceTopics = yield* validateSourceTopicKeys(config, keys);
  const encoded: Record<string, Schema.Json> = {};
  for (const topic of sourceTopics) {
    const contract = yield* compileSourceHealthContract(config, topic).pipe(
      Effect.mapError((error) => invalidHealthRow(topic, error.message)),
    );
    const value = readProperty(sources, topic);
    if (contract.lifecycle === "materialized") {
      yield* requireExactSourceHealth(topic, contract, value).pipe(
        Effect.mapError((error) => invalidHealthRow(topic, error.message)),
      );
      encoded[topic] = yield* encodeJsonFieldValue(
        contract.health,
        value,
        sourceHealthCodecErrors(topic),
      );
      continue;
    }
    if (!Array.isArray(value)) {
      return yield* Effect.fail(
        invalidHealthRow(topic, `Leased aggregate Source Health for ${topic} must be an array.`),
      );
    }
    const active: Array<Schema.Json> = [];
    for (const health of value) {
      yield* requireExactSourceHealth(topic, contract, health).pipe(
        Effect.mapError((error) => invalidHealthRow(topic, error.message)),
      );
      active.push(
        yield* encodeJsonFieldValue(contract.health, health, sourceHealthCodecErrors(topic)),
      );
    }
    encoded[topic] = active;
  }
  return encoded;
});

const decodeSources = Effect.fn("ViewServerProtocol.health.sources.decode")(function* <
  Topics extends TopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  candidate: { readonly sources?: unknown },
): Effect.fn.Return<Readonly<Record<string, unknown>>, ViewServerRuntimeError> {
  const { keys, sources } = yield* sourceRecordKeys(candidate);
  const sourceTopics = yield* validateSourceTopicKeys(config, keys);
  const decoded: Record<string, unknown> = {};
  for (const topic of sourceTopics) {
    const contract = yield* compileSourceHealthContract(config, topic).pipe(
      Effect.mapError((error) => invalidHealthRow(topic, error.message)),
    );
    const value = readProperty(sources, topic);
    if (contract.lifecycle === "materialized") {
      yield* requireExactSourceHealth(topic, contract, value).pipe(
        Effect.mapError((error) => invalidHealthRow(topic, error.message)),
      );
      decoded[topic] = yield* decodeJsonFieldValue(
        contract.health,
        value,
        sourceHealthCodecErrors(topic),
      );
      continue;
    }
    if (!Array.isArray(value)) {
      return yield* Effect.fail(
        invalidHealthRow(topic, `Leased aggregate Source Health for ${topic} must be an array.`),
      );
    }
    const active: Array<unknown> = [];
    for (const health of value) {
      yield* requireExactSourceHealth(topic, contract, health).pipe(
        Effect.mapError((error) => invalidHealthRow(topic, error.message)),
      );
      active.push(
        yield* decodeJsonFieldValue(contract.health, health, sourceHealthCodecErrors(topic)),
      );
    }
    decoded[topic] = active;
  }
  return decoded;
});

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
