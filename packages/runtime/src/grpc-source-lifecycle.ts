import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import type { GrpcClientValue, GrpcConnectClientDefinition } from "@effect-view-server/config";
import { Effect, Schema, Stream } from "effect";

export class ViewServerGrpcIngressError extends Schema.TaggedErrorClass<ViewServerGrpcIngressError>()(
  "ViewServerGrpcIngressError",
  {
    message: Schema.String,
    cause: Schema.Unknown,
    phase: Schema.optionalKey(
      Schema.Literals([
        "configuration",
        "client",
        "request",
        "acquire",
        "stream",
        "mapping",
        "publish",
        "release",
      ]),
    ),
    feedName: Schema.optionalKey(Schema.String),
    topic: Schema.optionalKey(Schema.String),
  },
) {}

export type ViewServerGrpcRuntimeCallable = (...args: ReadonlyArray<never>) => unknown;

export type ViewServerGrpcRuntimeSourceDefinition = {
  readonly topic: string;
  readonly request: ViewServerGrpcRuntimeCallable;
  readonly acquire: ViewServerGrpcRuntimeCallable;
  readonly release?: ViewServerGrpcRuntimeCallable;
};

export type ViewServerGrpcFeedSession = {
  readonly id: string | null;
  readonly forwardedHeaders: Readonly<Record<string, string>>;
  readonly systemHeaders: Readonly<Record<string, string>>;
};

export type ViewServerGrpcSourceInput<Route> = {
  readonly client: unknown;
  readonly request: unknown;
  readonly route: Route;
  readonly session: ViewServerGrpcFeedSession;
};

export type ViewServerGrpcClientFactory = <
  const ClientDefinition extends GrpcConnectClientDefinition,
>(
  definition: ClientDefinition,
  baseUrl: string,
) => GrpcClientValue<ClientDefinition>;

type ViewServerGrpcSourceKind = "materialized" | "leased";

const grpcSourceLabel = (kind: ViewServerGrpcSourceKind) => {
  switch (kind) {
    case "materialized":
      return "gRPC feed";
    case "leased":
      return "gRPC leased feed";
  }
};

const emptyHeaders = (): Readonly<Record<string, string>> => Object.freeze({});

export const makeGrpcFeedSession = (): ViewServerGrpcFeedSession =>
  Object.freeze({
    id: null,
    forwardedHeaders: emptyHeaders(),
    systemHeaders: emptyHeaders(),
  });

export const makeGrpcSourceInput = <Route>(
  client: unknown,
  request: unknown,
  route: Route,
): ViewServerGrpcSourceInput<Route> => ({
  client,
  request,
  route,
  session: makeGrpcFeedSession(),
});

export const makeViewServerGrpcSourceError = (input: {
  readonly message: string;
  readonly cause: unknown;
  readonly phase?: NonNullable<ViewServerGrpcIngressError["phase"]>;
  readonly feedName: string;
  readonly topic: string;
}) =>
  new ViewServerGrpcIngressError({
    message: input.message,
    cause: input.cause,
    ...(input.phase === undefined ? {} : { phase: input.phase }),
    feedName: input.feedName,
    topic: input.topic,
  });

export function makeDefaultGrpcClient<const ClientDefinition extends GrpcConnectClientDefinition>(
  definition: ClientDefinition,
  baseUrl: string,
): GrpcClientValue<ClientDefinition>;
export function makeDefaultGrpcClient(definition: GrpcConnectClientDefinition, baseUrl: string) {
  return createClient(definition.service, createGrpcTransport({ baseUrl }));
}

const isRuntimeGrpcStream = (value: unknown): value is Stream.Stream<unknown, unknown, never> =>
  Stream.isStream(value);

const isRuntimeGrpcReleaseEffect = (value: unknown): value is Effect.Effect<void, unknown, never> =>
  Effect.isEffect(value);

const callGrpcSourceRequest = Effect.fn("ViewServerRuntime.grpc.source.request")(function* (
  kind: ViewServerGrpcSourceKind,
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "request" | "topic">,
  args: ReadonlyArray<unknown>,
) {
  const label = grpcSourceLabel(kind);
  return yield* Effect.try({
    try: () => Reflect.apply(feed.request, undefined, args),
    catch: (cause) =>
      makeViewServerGrpcSourceError({
        message: `${label} request creation failed for ${feedName}`,
        cause,
        phase: "request",
        feedName,
        topic: feed.topic,
      }),
  });
});

export const callMaterializedGrpcSourceRequest = Effect.fn(
  "ViewServerRuntime.grpc.materialized.source.request",
)(function* (
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "request" | "topic">,
) {
  return yield* callGrpcSourceRequest("materialized", feedName, feed, []);
});

export const callLeasedGrpcSourceRequest = Effect.fn(
  "ViewServerRuntime.grpc.leased.source.request",
)(function* <Route>(
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "request" | "topic">,
  route: Route,
) {
  return yield* callGrpcSourceRequest("leased", feedName, feed, [route]);
});

const callGrpcSourceAcquire = Effect.fn("ViewServerRuntime.grpc.source.acquire")(function* <Route>(
  kind: ViewServerGrpcSourceKind,
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "acquire" | "topic">,
  input: ViewServerGrpcSourceInput<Route>,
) {
  const label = grpcSourceLabel(kind);
  const stream = yield* Effect.try({
    try: () => Reflect.apply(feed.acquire, undefined, [input]),
    catch: (cause) =>
      makeViewServerGrpcSourceError({
        message: `${label} acquire failed for ${feedName}`,
        cause,
        phase: "acquire",
        feedName,
        topic: feed.topic,
      }),
  });
  if (isRuntimeGrpcStream(stream)) {
    return stream;
  }
  return yield* makeViewServerGrpcSourceError({
    message: `${label} acquire did not return a Stream for ${feedName}`,
    cause: stream,
    phase: "acquire",
    feedName,
    topic: feed.topic,
  });
});

export const callMaterializedGrpcSourceAcquire = Effect.fn(
  "ViewServerRuntime.grpc.materialized.source.acquire",
)(function* (
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "acquire" | "topic">,
  input: ViewServerGrpcSourceInput<undefined>,
) {
  return yield* callGrpcSourceAcquire("materialized", feedName, feed, input);
});

export const callLeasedGrpcSourceAcquire = Effect.fn(
  "ViewServerRuntime.grpc.leased.source.acquire",
)(function* <Route>(
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "acquire" | "topic">,
  input: ViewServerGrpcSourceInput<Route>,
) {
  return yield* callGrpcSourceAcquire("leased", feedName, feed, input);
});

const callGrpcSourceRelease = Effect.fn("ViewServerRuntime.grpc.source.release")(function* <Route>(
  kind: ViewServerGrpcSourceKind,
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "release" | "topic">,
  input: ViewServerGrpcSourceInput<Route>,
) {
  const label = grpcSourceLabel(kind);
  const releaseCallback = feed.release;
  if (releaseCallback === undefined) {
    return;
  }
  const release = yield* Effect.try({
    try: () => Reflect.apply(releaseCallback, undefined, [input]),
    catch: (cause) =>
      makeViewServerGrpcSourceError({
        message: `${label} release failed for ${feedName}`,
        cause,
        phase: "release",
        feedName,
        topic: feed.topic,
      }),
  });
  if (!isRuntimeGrpcReleaseEffect(release)) {
    return yield* makeViewServerGrpcSourceError({
      message: `${label} release did not return an Effect for ${feedName}`,
      cause: release,
      phase: "release",
      feedName,
      topic: feed.topic,
    });
  }
  yield* release.pipe(
    Effect.asVoid,
    Effect.mapError((cause) =>
      makeViewServerGrpcSourceError({
        message: `${label} release failed for ${feedName}`,
        cause,
        phase: "release",
        feedName,
        topic: feed.topic,
      }),
    ),
  );
});

export const callMaterializedGrpcSourceRelease = Effect.fn(
  "ViewServerRuntime.grpc.materialized.source.release",
)(function* (
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "release" | "topic">,
  input: ViewServerGrpcSourceInput<undefined>,
) {
  yield* callGrpcSourceRelease("materialized", feedName, feed, input);
});

export const callLeasedGrpcSourceRelease = Effect.fn(
  "ViewServerRuntime.grpc.leased.source.release",
)(function* <Route>(
  feedName: string,
  feed: Pick<ViewServerGrpcRuntimeSourceDefinition, "release" | "topic">,
  input: ViewServerGrpcSourceInput<Route>,
) {
  yield* callGrpcSourceRelease("leased", feedName, feed, input);
});
