import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  RuntimeValue,
  ViewServerConfig,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import {
  makeTopicSourceBindings,
  type TopicGrpcSourceValidMetadata,
} from "@effect-view-server/runtime-core/internal";
import type { Duration } from "effect";
import { Config, Duration as EffectDuration, Effect, Option } from "effect";
import { ViewServerGrpcIngressError } from "./grpc-source-lifecycle";
import type { ViewServerGrpcRuntimeOptions } from "./grpc-runtime-option-contract";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

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

const resolveRuntimeValue = <A>(value: RuntimeValue<A>): Effect.Effect<A, Config.ConfigError> =>
  Config.isConfig(value) ? value : Effect.succeed(value);

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

const runtimeGrpcFeedCallable = (value: unknown): value is RuntimeGrpcFeedCallable =>
  typeof value === "function";

const grpcMethodIsServerStreaming = (method: unknown): boolean =>
  typeof method === "object" &&
  method !== null &&
  Reflect.get(method, "methodKind") === "server_streaming";

const boundGrpcSourceFromUnknown = (
  source: unknown,
  sourceMetadata: TopicGrpcSourceValidMetadata,
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

export const grpcFeedsFromConfig = <
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
  for (const [topic, binding] of makeTopicSourceBindings(config)) {
    if (binding.grpcMetadata._tag !== "valid") {
      continue;
    }
    const bound = boundGrpcSourceFromUnknown(binding.grpcSource, binding.grpcMetadata);
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

export const hasGrpcSourceDeclarations = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, Clients> | undefined,
): boolean =>
  config !== undefined &&
  Array.from(makeTopicSourceBindings(config).values()).some(
    (binding) => binding.grpcMetadata._tag !== "absent",
  );

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
    for (const [topic, binding] of makeTopicSourceBindings(config)) {
      if (binding.grpcMetadata._tag !== "invalid") {
        continue;
      }
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${topic} declares invalid gRPC source metadata.`,
        cause: binding.grpcMetadata.cause,
        feedName: topic,
        topic,
        phase: "configuration",
      });
    }
  });

export const resolveViewServerGrpcRuntimeOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, RuntimeRegions, Clients> | undefined,
  options: ViewServerGrpcRuntimeOptions<Topics, Clients>,
) => Effect.Effect<
  ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  Config.ConfigError | ViewServerGrpcIngressError
> = Effect.fn("ViewServerRuntime.options.grpc.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, RuntimeRegions, Clients> | undefined,
  options: ViewServerGrpcRuntimeOptions<Topics, Clients>,
) {
  yield* validateConfigGrpcSourceMetadata(config);
  const clients = config?.grpc?.clients;
  const feeds = grpcFeedsFromConfig(config);
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

export const validatePublicGrpcRuntimeOptions = (
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
  const bindings = makeTopicSourceBindings(config);
  for (const [topic, binding] of bindings) {
    if (binding.grpcMetadata._tag === "absent") {
      continue;
    }
    if (binding.grpcMetadata._tag === "invalid") {
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${topic} declares invalid gRPC source metadata.`,
        cause: binding.grpcMetadata.cause,
        feedName: topic,
        topic,
        phase: "configuration",
      });
    }
    const lifecycle = binding.grpcMetadata.lifecycle;
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
      const sourceRouteBy = binding.grpcMetadata.routeBy;
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
    const binding = bindings.get(feed.topic);
    if (binding === undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} references unknown View Server topic ${feed.topic}.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
    if (binding.grpcMetadata._tag === "absent") {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} targets View Server topic ${feed.topic}, but that topic does not declare a gRPC source.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
  }
});
