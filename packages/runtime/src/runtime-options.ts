import type { ViewServerRuntimeCoreOptionsFor } from "@effect-view-server/runtime-core";
import {
  collectSourceOwnershipConflicts,
  type SourceOwnershipGrpcOptions,
  type SourceOwnershipKafkaOptions,
} from "@effect-view-server/runtime-core/internal";
import type { ViewServerWebSocketServerOptions } from "@effect-view-server/server";
import type {
  GrpcRuntimeClients,
  KafkaStartFromHealth,
  RuntimeRegions,
  RuntimeValue,
  ViewServerConfig,
  ViewServerTopicConfig,
  ViewServerKafkaStartFrom,
} from "@effect-view-server/config";
import {
  makeKafkaSourceTopicsForConfig,
  type KafkaResolvedSourceTopicDefinition,
} from "@effect-view-server/config/internal";
import type { Duration } from "effect";
import { Config, Duration as EffectDuration, Effect, Option } from "effect";
import type {
  ViewServerKafkaRuntimeOptions,
  ViewServerGrpcRuntimeOptions,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { ViewServerGrpcIngressError } from "./grpc-ingress";
import { messageFromUnknown, ViewServerKafkaIngressError } from "./kafka-ingress";

export type ResolvedViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly auth?: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>["auth"];
  readonly runtimeCoreOptions: ViewServerRuntimeCoreOptionsFor<Topics>;
  readonly serverOptions: ViewServerWebSocketServerOptions;
  readonly tcpPublishOptions?: {
    readonly host?: string;
    readonly maxConnections?: number;
    readonly port: number;
  };
  readonly kafkaOptions?: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>;
  readonly grpcOptions?: ResolvedViewServerGrpcRuntimeOptions<Topics, GrpcClients>;
};

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

type RuntimeGrpcFeedCallable = (...args: ReadonlyArray<never>) => unknown;

export type ResolvedViewServerGrpcFeedDefinition = {
  readonly lifecycle: "materialized" | "leased";
  readonly topic: string;
  readonly client: string;
  readonly method: string;
  readonly routeBy?: ReadonlyArray<string>;
  readonly request: RuntimeGrpcFeedCallable;
  readonly acquire: RuntimeGrpcFeedCallable;
  readonly release?: RuntimeGrpcFeedCallable;
  readonly map: RuntimeGrpcFeedCallable;
};

export type ResolvedViewServerGrpcRuntimeOptions<
  _Topics extends ViewServerRuntimeTopicDefinitions,
  Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly clients: Clients;
  readonly clientBaseUrls: Record<string, string>;
  readonly feeds: Record<string, ResolvedViewServerGrpcFeedDefinition>;
  readonly materializedReconnect: {
    readonly maxReconnects: number;
    readonly delay: Duration.Input;
  };
};

type ViewServerGrpcRuntimeOptionsWithRuntimeFeeds<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = ViewServerGrpcRuntimeOptions<Topics, Clients> & {
  readonly clients?: Clients;
  readonly feeds?: Record<string, ResolvedViewServerGrpcFeedDefinition>;
};

export type ViewServerRuntimeOptionsWithRuntimeFeeds<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = Omit<ViewServerRuntimeOptions<Topics, Regions, GrpcClients>, "grpc"> & {
  readonly grpc?: ViewServerGrpcRuntimeOptionsWithRuntimeFeeds<Topics, GrpcClients>;
};

const resolveRuntimeValue = <A>(value: RuntimeValue<A>): Effect.Effect<A, Config.ConfigError> =>
  Config.isConfig(value) ? value : Effect.succeed(value);

const defaultKafkaStartFrom = (consumerGroupId: string): ViewServerKafkaStartFrom => ({
  committedConsumerGroup: consumerGroupId,
});

const defaultGrpcMaterializedReconnect = {
  delay: "1 second",
  maxReconnects: 60,
} satisfies ResolvedViewServerGrpcRuntimeOptions<ViewServerRuntimeTopicDefinitions>["materializedReconnect"];

const validateGrpcMaterializedMaxReconnects = (
  maxReconnects: number,
): Effect.Effect<number, ViewServerGrpcIngressError> => {
  if (Number.isSafeInteger(maxReconnects) && maxReconnects >= 0) {
    return Effect.succeed(maxReconnects);
  }
  return Effect.fail(
    new ViewServerGrpcIngressError({
      message: "gRPC materialized reconnect maxReconnects must be a finite non-negative integer.",
      cause: maxReconnects,
      phase: "configuration",
    }),
  );
};

const validateGrpcMaterializedReconnectDelay = (
  delay: Duration.Input,
): Effect.Effect<Duration.Input, ViewServerGrpcIngressError> => {
  const duration = EffectDuration.fromInput(delay);
  if (Option.isSome(duration) && EffectDuration.isFinite(duration.value)) {
    const millis = EffectDuration.toMillis(duration.value);
    if (Number.isFinite(millis) && millis > 0) {
      return Effect.succeed(delay);
    }
  }
  return Effect.fail(
    new ViewServerGrpcIngressError({
      message: "gRPC materialized reconnect delay must be finite and positive.",
      cause: delay,
      phase: "configuration",
    }),
  );
};

type BoundGrpcSource = {
  readonly lifecycle: "materialized" | "leased";
  readonly client: string;
  readonly method: string;
  readonly routeBy?: ReadonlyArray<string>;
  readonly request: RuntimeGrpcFeedCallable;
  readonly acquire: RuntimeGrpcFeedCallable;
  readonly release?: RuntimeGrpcFeedCallable;
  readonly map: RuntimeGrpcFeedCallable;
};

type ValidGrpcTopicSourceMetadata = Extract<GrpcTopicSourceMetadata, { readonly _tag: "valid" }>;

const runtimeGrpcFeedCallable = (value: unknown): value is RuntimeGrpcFeedCallable =>
  typeof value === "function";

const grpcMethodIsServerStreaming = (method: unknown): boolean =>
  typeof method === "object" &&
  method !== null &&
  Reflect.get(method, "methodKind") === "server_streaming";

const boundGrpcSourceFromUnknown = (
  source: unknown,
  sourceMetadata: ValidGrpcTopicSourceMetadata,
): BoundGrpcSource | undefined => {
  const sourceObject = Object(source);
  const lifecycle = sourceMetadata.lifecycle;
  const client = Reflect.get(sourceObject, "client");
  const method = Reflect.get(sourceObject, "method");
  const request = Reflect.get(sourceObject, "request");
  const acquire = Reflect.get(sourceObject, "acquire");
  const release = Reflect.get(sourceObject, "release");
  const map = Reflect.get(sourceObject, "map");
  if (
    typeof client !== "string" ||
    typeof method !== "string" ||
    !runtimeGrpcFeedCallable(request) ||
    !runtimeGrpcFeedCallable(acquire) ||
    !runtimeGrpcFeedCallable(map) ||
    (release !== undefined && !runtimeGrpcFeedCallable(release))
  ) {
    return undefined;
  }
  if (lifecycle === "materialized") {
    return {
      lifecycle,
      client,
      method,
      request,
      acquire,
      ...(release === undefined ? {} : { release }),
      map,
    };
  }
  return {
    lifecycle,
    client,
    method,
    routeBy: sourceMetadata.routeBy,
    request,
    acquire,
    ...(release === undefined ? {} : { release }),
    map,
  };
};

const grpcFeedsFromConfig = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, Clients> | undefined,
): Record<string, ResolvedViewServerGrpcFeedDefinition> => {
  const feeds: Record<string, ResolvedViewServerGrpcFeedDefinition> = Object.create(null);
  if (config === undefined) {
    return feeds;
  }
  for (const [topic, topicDefinition] of Object.entries(config.topics)) {
    const source =
      typeof topicDefinition === "object" &&
      topicDefinition !== null &&
      Object.prototype.hasOwnProperty.call(topicDefinition, "grpcSource")
        ? Reflect.get(topicDefinition, "grpcSource")
        : undefined;
    const sourceMetadata = grpcTopicSourceMetadata(topicDefinition);
    if (sourceMetadata._tag !== "valid") {
      continue;
    }
    const bound = boundGrpcSourceFromUnknown(source, sourceMetadata);
    if (bound === undefined) {
      continue;
    }
    feeds[topic] = {
      lifecycle: bound.lifecycle,
      topic,
      client: bound.client,
      method: bound.method,
      ...(bound.routeBy === undefined ? {} : { routeBy: bound.routeBy }),
      request: bound.request,
      acquire: bound.acquire,
      ...(bound.release === undefined ? {} : { release: bound.release }),
      map: bound.map,
    };
  }
  return feeds;
};

const validateConfigGrpcSourceMetadata = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, Clients> | undefined,
): Effect.Effect<void, ViewServerGrpcIngressError> =>
  Effect.gen(function* () {
    if (config === undefined) {
      return;
    }
    for (const [topic, topicDefinition] of Object.entries(config.topics)) {
      const sourceMetadata = grpcTopicSourceMetadata(topicDefinition);
      if (sourceMetadata._tag !== "invalid") {
        continue;
      }
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${topic} declares invalid gRPC source metadata.`,
        cause: sourceMetadata.cause,
        feedName: topic,
        topic,
        phase: "configuration",
      });
    }
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

const emptyKafkaSourceTopics = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(): Record<string, KafkaResolvedSourceTopicDefinition<Topics, Regions>> => Object.create(null);

const requireKafkaRuntimeOptionsForConfigSources = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients> | undefined,
): Effect.Effect<void, ViewServerKafkaIngressError> =>
  Effect.gen(function* () {
    const configuredTopics =
      config === undefined
        ? emptyKafkaSourceTopics<Topics, Regions>()
        : yield* kafkaSourcesFromConfig(config);
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

const resolveKafkaOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients> | undefined,
  options: ViewServerKafkaRuntimeOptions<Topics, Regions>,
) => Effect.Effect<
  ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  Config.ConfigError | ViewServerKafkaIngressError
> = Effect.fn("ViewServerRuntime.options.kafka.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients> | undefined,
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
  const configuredRegions = options.regions ?? config?.kafka;
  const entries = yield* Effect.forEach(
    Object.entries(configuredRegions ?? {}),
    ([region, value]) =>
      resolveRuntimeValue(value).pipe(Effect.map((bootstrap) => [region, bootstrap] as const)),
  );
  const regions: Record<string, string> = Object.create(null);
  for (const [region, bootstrap] of entries) {
    regions[region] = bootstrap;
  }
  const configuredTopics =
    config === undefined
      ? emptyKafkaSourceTopics<Topics, Regions>()
      : yield* kafkaSourcesFromConfig(config);
  const topics = configuredTopics;
  const sourceTopicCount = Object.keys(topics).length;
  if (sourceTopicCount === 0) {
    return yield* new ViewServerKafkaIngressError({
      message:
        "runtime options.kafka was provided, but no topic-owned Kafka sources were declared; remove options.kafka or add kafkaSource to a View Server topic.",
      cause: "missing-kafka-source-topics",
    });
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

const resolveGrpcOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, RuntimeRegions, Clients> | undefined,
  options: ViewServerGrpcRuntimeOptions<Topics, Clients>,
  runtimeFeeds: Record<string, ResolvedViewServerGrpcFeedDefinition>,
  runtimeClients?: Clients,
) => Effect.Effect<
  ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  Config.ConfigError | ViewServerGrpcIngressError
> = Effect.fn("ViewServerRuntime.options.grpc.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, RuntimeRegions, Clients> | undefined,
  options: ViewServerGrpcRuntimeOptions<Topics, Clients>,
  runtimeFeeds: Record<string, ResolvedViewServerGrpcFeedDefinition>,
  runtimeClients?: Clients,
) {
  yield* validateConfigGrpcSourceMetadata(config);
  const clients = runtimeClients ?? config?.grpc?.clients;
  const configFeeds = grpcFeedsFromConfig(config);
  const feeds: Record<string, ResolvedViewServerGrpcFeedDefinition> = Object.create(null);
  for (const [feedName, feed] of Object.entries(configFeeds)) {
    feeds[feedName] = feed;
  }
  for (const [feedName, feed] of Object.entries(runtimeFeeds)) {
    feeds[feedName] = feed;
  }
  if (clients === undefined) {
    if (Object.keys(feeds).length > 0) {
      return yield* new ViewServerGrpcIngressError({
        message:
          "gRPC feeds are configured, but no gRPC clients were provided on config.grpc.clients.",
        cause: "missing-grpc-clients",
        phase: "configuration",
      });
    }
    return yield* new ViewServerGrpcIngressError({
      message:
        "runtime options.grpc was provided, but no gRPC clients were provided on config.grpc.clients.",
      cause: "missing-grpc-clients",
      phase: "configuration",
    });
  }
  const entries = yield* Effect.forEach(Object.entries(clients), ([clientName, client]) =>
    resolveRuntimeValue(client.baseUrl).pipe(
      Effect.map(
        (baseUrl) =>
          [
            clientName,
            {
              _tag: client._tag,
              baseUrl,
              protocol: client.protocol,
              service: client.service,
            },
          ] as const,
      ),
    ),
  );
  const clientBaseUrls: Record<string, string> = Object.create(null);
  for (const [clientName, client] of entries) {
    clientBaseUrls[clientName] = client.baseUrl;
  }
  const materializedReconnectDelay = yield* validateGrpcMaterializedReconnectDelay(
    options.materializedReconnect?.delay ?? defaultGrpcMaterializedReconnect.delay,
  );
  const materializedReconnectMaxReconnects = yield* validateGrpcMaterializedMaxReconnects(
    options.materializedReconnect?.maxReconnects ?? defaultGrpcMaterializedReconnect.maxReconnects,
  );
  const feedTopics = new Map<string, string>();
  for (const [feedName, feed] of Object.entries(feeds)) {
    const client = clients[feed.client];
    if (client === undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} references missing client: ${feed.client}`,
        cause: feed.client,
        feedName,
        topic: feed.topic,
        phase: "configuration",
      });
    }
    const method = Reflect.get(client.service.method, feed.method);
    if (!Object.prototype.hasOwnProperty.call(client.service.method, feed.method)) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} references missing method ${feed.method} on client ${feed.client}`,
        cause: feed.method,
        feedName,
        topic: feed.topic,
        phase: "configuration",
      });
    }
    if (!grpcMethodIsServerStreaming(method)) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} references non-server-streaming method ${feed.method} on client ${feed.client}`,
        cause: feed.method,
        feedName,
        topic: feed.topic,
        phase: "configuration",
      });
    }
    const previousFeedName = feedTopics.get(feed.topic);
    if (previousFeedName !== undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} conflicts with ${previousFeedName}; View Server topic ${feed.topic} already has a gRPC feed owner.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
    feedTopics.set(feed.topic, feedName);
  }
  return {
    clients,
    clientBaseUrls,
    feeds,
    materializedReconnect: {
      delay: materializedReconnectDelay,
      maxReconnects: materializedReconnectMaxReconnects,
    },
  };
});

const validatePublicGrpcRuntimeOptions = (
  options: ViewServerGrpcRuntimeOptions<ViewServerRuntimeTopicDefinitions> | undefined,
): Effect.Effect<void, ViewServerGrpcIngressError> => {
  if (options === undefined) {
    return Effect.void;
  }
  if (typeof options !== "object" || options === null || Array.isArray(options)) {
    return Effect.fail(
      new ViewServerGrpcIngressError({
        message: "runtime options.grpc must be an object when provided.",
        cause: options,
        phase: "configuration",
      }),
    );
  }
  for (const key of Object.getOwnPropertyNames(options)) {
    if (key === "materializedReconnect") {
      continue;
    }
    if (key === "clients") {
      return Effect.fail(
        new ViewServerGrpcIngressError({
          message:
            "runtime options.grpc.clients is not supported; bind gRPC clients in defineViewServerConfig.grpc.clients.",
          cause: key,
          phase: "configuration",
        }),
      );
    }
    if (key === "feeds") {
      return Effect.fail(
        new ViewServerGrpcIngressError({
          message:
            "runtime options.grpc.feeds is not supported; bind gRPC feeds on topic-owned grpcSource definitions.",
          cause: key,
          phase: "configuration",
        }),
      );
    }
    return Effect.fail(
      new ViewServerGrpcIngressError({
        message: `runtime options.grpc has unsupported key: ${key}`,
        cause: key,
        phase: "configuration",
      }),
    );
  }
  const materializedReconnect = options.materializedReconnect;
  if (typeof materializedReconnect === "object" && materializedReconnect !== null) {
    for (const key of Object.getOwnPropertyNames(materializedReconnect)) {
      if (key === "delay" || key === "maxReconnects") {
        continue;
      }
      return Effect.fail(
        new ViewServerGrpcIngressError({
          message: `runtime options.grpc.materializedReconnect has unsupported key: ${key}`,
          cause: key,
          phase: "configuration",
        }),
      );
    }
  }
  return Effect.void;
};

export const validateSourceOwnership: (
  kafkaOptions: SourceOwnershipKafkaOptions | undefined,
  grpcOptions: SourceOwnershipGrpcOptions | undefined,
) => Effect.Effect<void, ViewServerGrpcIngressError> = Effect.fn(
  "ViewServerRuntime.options.sourceOwnership.validate",
)(function* (
  kafkaOptions: SourceOwnershipKafkaOptions | undefined,
  grpcOptions: SourceOwnershipGrpcOptions | undefined,
) {
  const conflict = collectSourceOwnershipConflicts(kafkaOptions, grpcOptions)[0];
  if (conflict !== undefined) {
    return yield* new ViewServerGrpcIngressError({
      message: `View Server topic ${conflict.topic} cannot be owned by both Kafka source ${conflict.kafkaSource} and gRPC feed ${conflict.grpcFeed}.`,
      cause: conflict.topic,
      feedName: conflict.grpcFeed,
      topic: conflict.topic,
    });
  }
});

type GrpcTopicSourceMetadata =
  | {
      readonly _tag: "absent";
    }
  | {
      readonly _tag: "invalid";
      readonly cause: unknown;
    }
  | {
      readonly _tag: "valid";
      readonly lifecycle: "materialized";
    }
  | {
      readonly _tag: "valid";
      readonly lifecycle: "leased";
      readonly routeBy: ReadonlyArray<string>;
    };

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const hasOnlyOwnStringKeys = (value: object, allowedKeys: ReadonlyArray<string>): boolean =>
  Object.getOwnPropertyNames(value).every((key) => allowedKeys.includes(key));

const hasConcreteGrpcBinding = (source: object): boolean =>
  ["client", "method", "request", "acquire", "release", "map"].some((key) =>
    hasDefinedOwnProperty(source, key),
  );

const hasCompleteConcreteGrpcBinding = (source: object): boolean => {
  const request = Reflect.get(source, "request");
  const acquire = Reflect.get(source, "acquire");
  const release = Reflect.get(source, "release");
  const map = Reflect.get(source, "map");
  return (
    hasDefinedOwnProperty(source, "client") &&
    hasDefinedOwnProperty(source, "method") &&
    runtimeGrpcFeedCallable(request) &&
    runtimeGrpcFeedCallable(acquire) &&
    runtimeGrpcFeedCallable(map) &&
    (release === undefined || runtimeGrpcFeedCallable(release))
  );
};

const grpcTopicSourceFromUnknown = (source: unknown): GrpcTopicSourceMetadata => {
  if (typeof source !== "object" || source === null) {
    return { _tag: "invalid", cause: source };
  }
  if (Reflect.get(source, "kind") !== "grpc") {
    return { _tag: "invalid", cause: source };
  }
  const lifecycle = Reflect.get(source, "lifecycle");
  if (lifecycle !== "leased" && lifecycle !== "materialized") {
    return { _tag: "invalid", cause: source };
  }
  const sourceTag = Reflect.get(source, "_tag");
  if (lifecycle === "materialized") {
    if (
      !hasOnlyOwnStringKeys(source, [
        "_tag",
        "kind",
        "lifecycle",
        "client",
        "method",
        "request",
        "acquire",
        "release",
        "map",
      ])
    ) {
      return { _tag: "invalid", cause: source };
    }
    if (sourceTag !== "GrpcMaterializedTopicSource") {
      return { _tag: "invalid", cause: source };
    }
    if (hasConcreteGrpcBinding(source) && !hasCompleteConcreteGrpcBinding(source)) {
      return { _tag: "invalid", cause: source };
    }
    return { _tag: "valid", lifecycle };
  }
  if (
    !hasOnlyOwnStringKeys(source, [
      "_tag",
      "kind",
      "lifecycle",
      "routeBy",
      "client",
      "method",
      "request",
      "acquire",
      "release",
      "map",
    ])
  ) {
    return { _tag: "invalid", cause: source };
  }
  if (sourceTag !== "GrpcLeasedTopicSource") {
    return { _tag: "invalid", cause: source };
  }
  if (hasConcreteGrpcBinding(source) && !hasCompleteConcreteGrpcBinding(source)) {
    return { _tag: "invalid", cause: source };
  }
  const routeBy = Reflect.get(source, "routeBy");
  if (
    !Array.isArray(routeBy) ||
    routeBy.length === 0 ||
    !routeBy.every((field) => typeof field === "string")
  ) {
    return { _tag: "invalid", cause: source };
  }
  return { _tag: "valid", lifecycle, routeBy };
};

const grpcTopicSourceMetadata = (topicDefinition: unknown): GrpcTopicSourceMetadata => {
  if (typeof topicDefinition !== "object" || topicDefinition === null) {
    return { _tag: "absent" };
  }
  if (hasDefinedOwnProperty(topicDefinition, "grpcSource")) {
    return grpcTopicSourceFromUnknown(Reflect.get(topicDefinition, "grpcSource"));
  }
  return { _tag: "absent" };
};

const grpcFeedLeasedRouteBy = (feed: unknown): ReadonlyArray<string> | undefined => {
  const routeBy = Reflect.get(Object(feed), "routeBy");
  return Array.isArray(routeBy) && routeBy.every((field) => typeof field === "string")
    ? routeBy
    : undefined;
};

const sameRouteBy = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean =>
  left.length === right.length && left.every((field, index) => field === right[index]);

export const validateGrpcSourceFeeds: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerTopicConfig<Topics>,
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients> | undefined,
) => Effect.Effect<void, ViewServerGrpcIngressError> = Effect.fn(
  "ViewServerRuntime.options.grpcSourceFeeds.validate",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerTopicConfig<Topics>,
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients> | undefined,
) {
  const feedEntries = Object.entries(grpcOptions?.feeds ?? {});
  for (const [topic, topicDefinition] of Object.entries(config.topics)) {
    const sourceMetadata = grpcTopicSourceMetadata(topicDefinition);
    if (sourceMetadata._tag === "absent") {
      continue;
    }
    if (sourceMetadata._tag === "invalid") {
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${topic} declares invalid gRPC source metadata.`,
        cause: sourceMetadata.cause,
        feedName: topic,
        topic,
        phase: "configuration",
      });
    }
    const lifecycle = sourceMetadata.lifecycle;
    const matchingFeeds = feedEntries.filter(([_feedName, feed]) => feed.topic === topic);
    if (matchingFeeds.length === 0) {
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${topic} declares gRPC ${lifecycle} source but no matching gRPC feed was configured.`,
        cause: topic,
        feedName: topic,
        topic,
      });
    }
    const mismatchedFeed = matchingFeeds.find(([_feedName, feed]) => feed.lifecycle !== lifecycle);
    if (mismatchedFeed !== undefined) {
      const [feedName, feed] = mismatchedFeed;
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} lifecycle ${feed.lifecycle} does not match View Server topic ${topic} source lifecycle ${lifecycle}.`,
        cause: feed.lifecycle,
        feedName,
        topic,
      });
    }
    if (lifecycle === "leased") {
      const sourceRouteBy = sourceMetadata.routeBy;
      const routeMismatch = matchingFeeds.find(([_feedName, feed]) => {
        const feedRouteBy = grpcFeedLeasedRouteBy(feed);
        return feedRouteBy === undefined || !sameRouteBy(sourceRouteBy, feedRouteBy);
      });
      if (routeMismatch !== undefined) {
        const [feedName, feed] = routeMismatch;
        const feedRouteBy = grpcFeedLeasedRouteBy(feed) ?? [];
        return yield* new ViewServerGrpcIngressError({
          message: `gRPC leased feed ${feedName} routeBy ${feedRouteBy.join(", ")} does not match View Server topic ${topic} source routeBy ${sourceRouteBy.join(", ")}.`,
          cause: feedRouteBy,
          feedName,
          topic,
        });
      }
    }
  }
  for (const [feedName, feed] of feedEntries) {
    const topicDefinition = config.topics[feed.topic];
    if (topicDefinition === undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} references unknown View Server topic ${feed.topic}.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
    const sourceMetadata = grpcTopicSourceMetadata(topicDefinition);
    if (sourceMetadata._tag === "absent") {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} targets View Server topic ${feed.topic}, but that topic does not declare a gRPC source.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
  }
});

const resolveViewServerRuntimeOptionsWithConfig: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients> | undefined,
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  runtimeFeeds?: Record<string, ResolvedViewServerGrpcFeedDefinition>,
  runtimeClients?: GrpcClients,
) => Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
> = Effect.fn("ViewServerRuntime.options.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients> | undefined,
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  runtimeFeeds: Record<string, ResolvedViewServerGrpcFeedDefinition> = {},
  runtimeClients?: GrpcClients,
) {
  const runtimeCoreOptions = {
    ...(options.groupedIncrementalAdmissionLimits === undefined
      ? {}
      : { groupedIncrementalAdmissionLimits: options.groupedIncrementalAdmissionLimits }),
    ...(options.subscriptionQueueCapacity === undefined
      ? {}
      : { subscriptionQueueCapacity: options.subscriptionQueueCapacity }),
  };
  const serverOptions = {
    ...(options.host === undefined ? {} : { host: options.host }),
    ...(options.websocketPort === undefined ? {} : { port: options.websocketPort }),
    ...(options.rpcPath === undefined ? {} : { path: options.rpcPath }),
    ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
    ...(options.metricsPath === undefined ? {} : { metricsPath: options.metricsPath }),
  };
  const tcpPublishOptions =
    options.tcpPublishPort === undefined
      ? undefined
      : {
          ...(options.tcpPublishHost === undefined ? {} : { host: options.tcpPublishHost }),
          ...(options.tcpPublishMaxConnections === undefined
            ? {}
            : { maxConnections: options.tcpPublishMaxConnections }),
          port: options.tcpPublishPort,
        };
  const kafkaOptions =
    options.kafka === undefined
      ? (yield* requireKafkaRuntimeOptionsForConfigSources(config), undefined)
      : yield* resolveKafkaOptions(config, options.kafka);
  yield* validateConfigGrpcSourceMetadata(config);
  const configGrpcFeeds = grpcFeedsFromConfig(config);
  const grpcOptions =
    options.grpc === undefined
      ? Object.keys(configGrpcFeeds).length === 0
        ? undefined
        : yield* resolveGrpcOptions(config, {}, runtimeFeeds, runtimeClients)
      : yield* resolveGrpcOptions(config, options.grpc, runtimeFeeds, runtimeClients);
  yield* validateSourceOwnership(kafkaOptions, grpcOptions);
  return {
    ...(options.auth === undefined ? {} : { auth: options.auth }),
    runtimeCoreOptions,
    serverOptions,
    ...(tcpPublishOptions === undefined ? {} : { tcpPublishOptions }),
    ...(kafkaOptions === undefined ? {} : { kafkaOptions }),
    ...(grpcOptions === undefined ? {} : { grpcOptions }),
  };
});

export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  configOrOptions:
    | ViewServerConfig<Topics, Regions, GrpcClients>
    | ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  maybeOptions?: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
> {
  if ("defineRuntimeOptions" in configOrOptions) {
    return validatePublicGrpcRuntimeOptions(maybeOptions?.grpc).pipe(
      Effect.andThen(
        resolveViewServerRuntimeOptionsWithConfig(configOrOptions, maybeOptions ?? {}),
      ),
    );
  }
  return validatePublicGrpcRuntimeOptions(configOrOptions.grpc).pipe(
    Effect.andThen(resolveViewServerRuntimeOptionsWithConfig(undefined, configOrOptions)),
  );
}

export function resolveViewServerRuntimeOptionsWithRuntimeFeeds<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  options: ViewServerRuntimeOptionsWithRuntimeFeeds<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptionsWithRuntimeFeeds<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptionsWithRuntimeFeeds<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: ViewServerRuntimeOptionsWithRuntimeFeeds<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptionsWithRuntimeFeeds<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  configOrOptions:
    | ViewServerConfig<Topics, Regions, GrpcClients>
    | ViewServerRuntimeOptionsWithRuntimeFeeds<Topics, Regions, GrpcClients>,
  maybeOptions?: ViewServerRuntimeOptionsWithRuntimeFeeds<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
> {
  // Private dependency-injection seam for tests/benchmarks. Public runtime options
  // are validated by `resolveViewServerRuntimeOptions` and reject `grpc.feeds`.
  if ("defineRuntimeOptions" in configOrOptions) {
    return resolveViewServerRuntimeOptionsWithConfig(
      configOrOptions,
      maybeOptions ?? {},
      maybeOptions?.grpc?.feeds ?? {},
      maybeOptions?.grpc?.clients,
    );
  }
  return resolveViewServerRuntimeOptionsWithConfig(
    undefined,
    configOrOptions,
    configOrOptions.grpc?.feeds ?? {},
    configOrOptions.grpc?.clients,
  );
}
