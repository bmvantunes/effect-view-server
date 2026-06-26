import type { ViewServerRuntimeCoreOptionsFor } from "@view-server/runtime-core";
import type { ViewServerWebSocketServerOptions } from "@view-server/server";
import type {
  GrpcRuntimeClients,
  KafkaStartFromHealth,
  RuntimeRegions,
  RuntimeValue,
  ViewServerKafkaStartFrom,
} from "@view-server/config";
import { Config, Effect } from "effect";
import type {
  ViewServerKafkaRuntimeOptions,
  ViewServerGrpcRuntimeOptions,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { ViewServerGrpcIngressError } from "./grpc-ingress";

export type ResolvedViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly runtimeCoreOptions: ViewServerRuntimeCoreOptionsFor<Topics>;
  readonly serverOptions: ViewServerWebSocketServerOptions;
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
  readonly topics: ViewServerKafkaRuntimeOptions<Topics, Regions>["topics"];
};

export type ResolvedViewServerGrpcRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly clients: Clients;
  readonly clientBaseUrls: Record<string, string>;
  readonly feeds: ViewServerGrpcRuntimeOptions<Topics, Clients>["feeds"];
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

const resolveKafkaOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  options: ViewServerKafkaRuntimeOptions<Topics, Regions>,
) => Effect.Effect<ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>, Config.ConfigError> =
  Effect.fn("ViewServerRuntime.options.kafka.resolve")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
  >(options: ViewServerKafkaRuntimeOptions<Topics, Regions>) {
    const entries = yield* Effect.forEach(Object.entries(options.regions), ([region, value]) =>
      resolveRuntimeValue(value).pipe(Effect.map((bootstrap) => [region, bootstrap] as const)),
    );
    const regions: Record<string, string> = Object.create(null);
    for (const [region, bootstrap] of entries) {
      regions[region] = bootstrap;
    }
    const startFrom = options.startFrom ?? defaultKafkaStartFrom(options.consumerGroupId);
    return {
      consumerGroupId: options.consumerGroupId,
      consume: normalizeKafkaConsumePolicy(options.consumerGroupId, startFrom),
      regions,
      startFrom,
      topics: options.topics,
    };
  });

const resolveGrpcOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  options: ViewServerGrpcRuntimeOptions<Topics, Clients>,
) => Effect.Effect<
  ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  Config.ConfigError | ViewServerGrpcIngressError
> = Effect.fn("ViewServerRuntime.options.grpc.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(options: ViewServerGrpcRuntimeOptions<Topics, Clients>) {
  const entries = yield* Effect.forEach(Object.entries(options.clients), ([clientName, client]) =>
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
  const feedTopics = new Map<string, string>();
  for (const [feedName, feed] of Object.entries(options.feeds)) {
    if (feed.lifecycle === "leased") {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC leased feed ${feedName} is not supported by runtime startup yet.`,
        cause: feed.lifecycle,
        feedName,
        topic: feed.topic,
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
    clients: options.clients,
    clientBaseUrls,
    feeds: options.feeds,
  };
});

type SourceOwnershipKafkaOptions = {
  readonly topics: Readonly<Record<string, { readonly viewServerTopic: string }>>;
};

type SourceOwnershipGrpcOptions = {
  readonly feeds: Readonly<Record<string, { readonly topic: string }>>;
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
  if (kafkaOptions === undefined || grpcOptions === undefined) {
    return;
  }
  const grpcFeedByTopic = new Map<string, string>();
  for (const [feedName, feed] of Object.entries(grpcOptions.feeds)) {
    grpcFeedByTopic.set(feed.topic, feedName);
  }
  for (const [sourceTopic, kafkaTopic] of Object.entries(kafkaOptions.topics)) {
    const grpcFeedName = grpcFeedByTopic.get(kafkaTopic.viewServerTopic);
    if (grpcFeedName !== undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${kafkaTopic.viewServerTopic} cannot be owned by both Kafka source ${sourceTopic} and gRPC feed ${grpcFeedName}.`,
        cause: kafkaTopic.viewServerTopic,
        feedName: grpcFeedName,
        topic: kafkaTopic.viewServerTopic,
      });
    }
  }
});

export const resolveViewServerRuntimeOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
) => Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError
> = Effect.fn("ViewServerRuntime.options.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>) {
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
  };
  const kafkaOptions =
    options.kafka === undefined ? undefined : yield* resolveKafkaOptions(options.kafka);
  const grpcOptions =
    options.grpc === undefined ? undefined : yield* resolveGrpcOptions(options.grpc);
  yield* validateSourceOwnership(kafkaOptions, grpcOptions);
  return {
    runtimeCoreOptions,
    serverOptions,
    ...(kafkaOptions === undefined ? {} : { kafkaOptions }),
    ...(grpcOptions === undefined ? {} : { grpcOptions }),
  };
});
