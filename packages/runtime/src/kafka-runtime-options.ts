import type {
  KafkaStartFromHealth,
  GrpcRuntimeClients,
  RuntimeRegions,
  RuntimeValue,
  ViewServerConfig,
  ViewServerKafkaStartFrom,
} from "@effect-view-server/config";
import {
  makeKafkaSourceTopicsForConfig,
  type KafkaResolvedSourceTopicDefinition,
} from "@effect-view-server/config/internal";
import { Config, Effect } from "effect";
import { messageFromUnknown, ViewServerKafkaIngressError } from "./kafka-ingress-error";
import type { ViewServerKafkaRuntimeOptions } from "./kafka-runtime-option-contract";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ResolvedViewServerKafkaRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly consumerGroupId: string;
  readonly startFrom: ViewServerKafkaStartFrom;
  readonly consume: KafkaStartFromHealth;
  readonly regions: Record<string, string>;
  readonly topics: Record<string, KafkaResolvedSourceTopicDefinition<Topics, Regions>>;
};

const resolveRuntimeValue = <A>(value: RuntimeValue<A>): Effect.Effect<A, Config.ConfigError> =>
  Config.isConfig(value) ? value : Effect.succeed(value);

const defaultKafkaStartFrom = (consumerGroupId: string): ViewServerKafkaStartFrom => ({
  committedConsumerGroup: consumerGroupId,
});
const normalizeKafkaConsumePolicy = (
  consumerGroupId: string,
  startFrom: ViewServerKafkaStartFrom,
): ResolvedViewServerKafkaRuntimeOptions<ViewServerRuntimeTopicDefinitions>["consume"] => {
  if (startFrom === "earliest") {
    return {
      consumerGroupId,
      fallbackMode: "earliest",
      mode: "earliest",
    };
  }
  if (startFrom === "latest") {
    return {
      consumerGroupId,
      fallbackMode: "latest",
      mode: "latest",
    };
  }
  return {
    consumerGroupId: startFrom.committedConsumerGroup,
    fallbackMode: startFrom.fallback ?? "earliest",
    mode: "committed",
  };
};

const kafkaSourcesFromConfig = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
): Effect.Effect<
  Record<string, KafkaResolvedSourceTopicDefinition<Topics, Regions>>,
  ViewServerKafkaIngressError
> =>
  Effect.gen(function* () {
    const sourceTopics = yield* Effect.try({
      try: () => makeKafkaSourceTopicsForConfig<Topics, Regions>(config),
      catch: (cause) =>
        new ViewServerKafkaIngressError({
          message: `Invalid topic-owned Kafka source configuration: ${messageFromUnknown(cause)}`,
          cause,
        }),
    });
    const topics: Record<
      string,
      KafkaResolvedSourceTopicDefinition<Topics, Regions>
    > = Object.create(null);
    for (const sourceTopic of sourceTopics) {
      if (topics[sourceTopic.topic] !== undefined) {
        return yield* new ViewServerKafkaIngressError({
          message: `Kafka source topic is configured more than once: ${sourceTopic.topic}`,
          cause: sourceTopic.topic,
          sourceTopic: sourceTopic.topic,
        });
      }
      topics[sourceTopic.topic] = sourceTopic;
    }
    return topics;
  });

export const requireKafkaRuntimeOptionsForConfigSources = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
): Effect.Effect<void, ViewServerKafkaIngressError> =>
  Effect.gen(function* () {
    const configuredTopics = yield* kafkaSourcesFromConfig(config);
    if (Object.keys(configuredTopics).length > 0) {
      return yield* new ViewServerKafkaIngressError({
        message:
          "Kafka sources are configured, but runtime options.kafka.consumerGroupId was not provided.",
        cause: "missing-kafka-consumer-group",
      });
    }
  });

const validateKafkaTopicRegions = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  topics: Record<string, KafkaResolvedSourceTopicDefinition<Topics, Regions>>,
  regions: Record<string, string>,
): Effect.Effect<void, ViewServerKafkaIngressError> =>
  Effect.gen(function* () {
    for (const [sourceTopic, topic] of Object.entries(topics)) {
      for (const region of topic.regions) {
        if (regions[region] === undefined) {
          return yield* new ViewServerKafkaIngressError({
            message: `Kafka source topic ${sourceTopic} references unknown Kafka region: ${region}`,
            cause: {
              region,
              sourceTopic,
            },
            region,
            sourceTopic,
          });
        }
      }
    }
  });

const validateKafkaConsumerGroupId = (
  consumerGroupId: unknown,
): Effect.Effect<string, ViewServerKafkaIngressError> =>
  Effect.gen(function* () {
    if (typeof consumerGroupId !== "string" || consumerGroupId.length === 0) {
      return yield* new ViewServerKafkaIngressError({
        message:
          "Kafka sources are configured, but runtime options.kafka.consumerGroupId was not provided.",
        cause: "missing-kafka-consumer-group",
      });
    }
    return consumerGroupId;
  });

export const resolveViewServerKafkaRuntimeOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: ViewServerKafkaRuntimeOptions<Topics, Regions>,
) => Effect.Effect<
  ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  Config.ConfigError | ViewServerKafkaIngressError
> = Effect.fn("ViewServerRuntime.options.kafka.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: ViewServerKafkaRuntimeOptions<Topics, Regions>,
) {
  if (Object.prototype.hasOwnProperty.call(options, "topics")) {
    return yield* new ViewServerKafkaIngressError({
      message:
        "runtime options.kafka.topics is not supported; declare Kafka sources on View Server topics with kafkaSource.",
      cause: "unsupported-runtime-kafka-topics",
    });
  }
  const consumerGroupId = yield* validateKafkaConsumerGroupId(options.consumerGroupId);
  const configuredTopics = yield* kafkaSourcesFromConfig(config);
  const topics = configuredTopics;
  const sourceTopicCount = Object.keys(topics).length;
  if (sourceTopicCount === 0) {
    return yield* new ViewServerKafkaIngressError({
      message:
        "runtime options.kafka was provided, but no topic-owned Kafka sources were declared; remove options.kafka or add kafkaSource to a View Server topic.",
      cause: "missing-kafka-source-topics",
    });
  }
  const configuredRegions = options.regions ?? config.kafka;
  const entries = yield* Effect.forEach(
    Object.entries(configuredRegions ?? {}),
    ([region, value]) =>
      resolveRuntimeValue(value).pipe(Effect.map((bootstrap) => [region, bootstrap] as const)),
  );
  const regions: Record<string, string> = Object.create(null);
  for (const [region, bootstrap] of entries) {
    regions[region] = bootstrap;
  }
  if (Object.keys(regions).length === 0) {
    return yield* new ViewServerKafkaIngressError({
      message:
        "Kafka sources are configured, but no Kafka regions were provided on config.kafka or runtime options.kafka.regions.",
      cause: "missing-kafka-regions",
    });
  }
  yield* validateKafkaTopicRegions(topics, regions);
  const startFrom = options.startFrom ?? defaultKafkaStartFrom(consumerGroupId);
  return {
    consumerGroupId,
    consume: normalizeKafkaConsumePolicy(consumerGroupId, startFrom),
    regions,
    startFrom,
    topics,
  };
});
