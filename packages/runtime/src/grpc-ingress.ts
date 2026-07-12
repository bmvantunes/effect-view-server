import { type GrpcRuntimeClients, type ViewServerTopicConfig } from "@effect-view-server/config";
import { validateDecodedRow } from "@effect-view-server/config/internal";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@effect-view-server/effect-utils";
import type { ViewServerRuntimeCoreInternalClient } from "@effect-view-server/runtime-core/internal";
import { Cause, Clock, Effect, Exit, Fiber, Option, Scope, Stream } from "effect";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import {
  callMaterializedGrpcSourceAcquire,
  callMaterializedGrpcSourceRelease,
  callMaterializedGrpcSourceRequest,
  makeGrpcSourceInput,
  makeDefaultGrpcClient,
  makeViewServerGrpcSourceError,
  ViewServerGrpcIngressError,
  type ViewServerGrpcClientFactory,
  type ViewServerGrpcRuntimeCallable,
} from "./grpc-source-lifecycle";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export { ViewServerGrpcIngressError } from "./grpc-source-lifecycle";

export type ViewServerGrpcIngress = {
  readonly close: Effect.Effect<void>;
};

type RuntimeMaterializedFeedDefinition<Topic extends string = string> = {
  readonly lifecycle: "materialized";
  readonly topic: Topic;
  readonly client: string;
  readonly method: string;
  readonly request: ViewServerGrpcRuntimeCallable;
  readonly acquire: ViewServerGrpcRuntimeCallable;
  readonly release?: ViewServerGrpcRuntimeCallable;
  readonly map: ViewServerGrpcRuntimeCallable;
};

const isRuntimeMaterializedFeed = <const Topic extends string>(feed: {
  readonly lifecycle: string;
}): feed is RuntimeMaterializedFeedDefinition<Topic> => feed.lifecycle === "materialized";

type ViewServerGrpcHealthRefreshRequest = Effect.Effect<void>;

const grpcMessageBatchSize = 256;
const grpcMessageBatchFlushInterval = "2 millis";

const ignoreGrpcFeedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring gRPC feed release failure.",
);
const ignoreGrpcHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring gRPC health refresh failure.",
);

const grpcIngressError = (input: {
  readonly message: string;
  readonly cause: unknown;
  readonly phase: NonNullable<ViewServerGrpcIngressError["phase"]>;
  readonly feedName: string;
  readonly topic: string;
}) => makeViewServerGrpcSourceError(input);

const hasMessage = (value: unknown): value is { readonly message: string } =>
  typeof value === "object" &&
  value !== null &&
  "message" in value &&
  typeof value.message === "string";

const messageFromUnknown = (value: unknown): string => {
  if (Cause.isCause(value)) {
    const failure = value.reasons.find(Cause.isFailReason);
    if (failure !== undefined) {
      return messageFromUnknown(failure.error);
    }
    return Cause.pretty(value);
  }
  if (value instanceof Error) {
    return value.message;
  }
  if (hasMessage(value)) {
    return value.message;
  }
  return String(value);
};

const grpcIngressErrorMessage = (error: ViewServerGrpcIngressError): string =>
  `${error.message}: ${messageFromUnknown(error.cause)}`;

const feedFailureMessage = (feedName: string, cause: Cause.Cause<unknown>): string => {
  const error = Cause.findErrorOption(cause);
  if (Option.isSome(error) && error.value instanceof ViewServerGrpcIngressError) {
    return `gRPC feed ${feedName} failed: ${grpcIngressErrorMessage(error.value)}`;
  }
  return `gRPC feed ${feedName} failed: ${Cause.pretty(cause)}`;
};

const unexpectedFeedCompletionMessage = (feedName: string): string =>
  `gRPC feed ${feedName} completed unexpectedly.`;

const isRestartableMaterializedFeedError = (error: ViewServerGrpcIngressError): boolean =>
  (error.phase === "acquire" || error.phase === "stream") &&
  (!Cause.isCause(error.cause) || !Cause.hasDies(error.cause));

const materializedFeedErrorHasInterrupts = (error: ViewServerGrpcIngressError): boolean =>
  Cause.isCause(error.cause) && Cause.hasInterrupts(error.cause);

const materializedFeedCauseHasInterrupts = (
  cause: Cause.Cause<ViewServerGrpcIngressError>,
): boolean => {
  if (Cause.hasInterrupts(cause)) {
    return true;
  }
  const error = Cause.findErrorOption(cause);
  return (
    Option.isSome(error) &&
    error.value instanceof ViewServerGrpcIngressError &&
    materializedFeedErrorHasInterrupts(error.value)
  );
};

const canReconnectMaterializedFeedCause = (
  cause: Cause.Cause<ViewServerGrpcIngressError>,
): boolean => {
  if (materializedFeedCauseHasInterrupts(cause)) {
    return false;
  }
  const error = Cause.findErrorOption(cause);
  return (
    Option.isSome(error) &&
    error.value instanceof ViewServerGrpcIngressError &&
    isRestartableMaterializedFeedError(error.value)
  );
};

const materializedFeedReconnectMessage = (
  feedName: string,
  exit: Exit.Exit<void, ViewServerGrpcIngressError>,
): string =>
  Exit.isSuccess(exit)
    ? unexpectedFeedCompletionMessage(feedName)
    : feedFailureMessage(feedName, exit.cause);

const materializedFeedExitEffect = (
  exit: Exit.Exit<void, ViewServerGrpcIngressError>,
): Effect.Effect<void, ViewServerGrpcIngressError> =>
  Exit.isSuccess(exit) ? Effect.void : Effect.failCause(exit.cause);

const markMaterializedFeedDegraded = Effect.fn("ViewServerRuntime.grpc.materialized.degraded")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
    health: ViewServerGrpcHealthLedger<Topics>,
    feedName: string,
    clientName: string,
    message: string,
  ) {
    yield* health.feedDegraded(feedName, message);
    yield* health.clientDegraded(clientName, message);
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  },
);

const handleMaterializedFeedExit = Effect.fn("ViewServerRuntime.grpc.materialized.exit")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  feedName: string,
  clientName: string,
  exit: Exit.Exit<void, ViewServerGrpcIngressError>,
) {
  if (Exit.isSuccess(exit)) {
    yield* markMaterializedFeedDegraded(
      requestHealthRefresh,
      health,
      feedName,
      clientName,
      unexpectedFeedCompletionMessage(feedName),
    );
    return;
  }
  if (materializedFeedCauseHasInterrupts(exit.cause)) {
    yield* health.feedStopping(feedName);
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
    return;
  }
  yield* markMaterializedFeedDegraded(
    requestHealthRefresh,
    health,
    feedName,
    clientName,
    feedFailureMessage(feedName, exit.cause),
  );
});

const mapMaterializedValue = Effect.fn("ViewServerRuntime.grpc.materialized.map")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends Extract<keyof Topics, string>,
>(
  config: ViewServerTopicConfig<Topics>,
  feed: RuntimeMaterializedFeedDefinition<Topic>,
  feedName: string,
  value: unknown,
) {
  const topicDefinition = config.topics[feed.topic];
  if (topicDefinition === undefined) {
    return yield* grpcIngressError({
      message: `gRPC feed ${feedName} references unknown topic ${feed.topic}`,
      cause: feed.topic,
      phase: "configuration",
      feedName,
      topic: feed.topic,
    });
  }
  const row = yield* Effect.try({
    try: () =>
      Reflect.apply(feed.map, undefined, [
        {
          value,
          route: undefined,
          schema: topicDefinition.schema,
        },
      ]),
    catch: (cause) =>
      grpcIngressError({
        message: `gRPC feed mapping failed for ${feedName}`,
        cause,
        phase: "mapping",
        feedName,
        topic: feed.topic,
      }),
  });
  const decodedRow = yield* validateDecodedRow(topicDefinition.schema, row).pipe(
    Effect.mapError((cause) =>
      grpcIngressError({
        message: `gRPC feed mapping produced an invalid row for ${feedName}`,
        cause,
        phase: "mapping",
        feedName,
        topic: feed.topic,
      }),
    ),
  );
  return decodedRow;
});

const publishMaterializedBatch = Effect.fn("ViewServerRuntime.grpc.materialized.publishBatch")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Topic extends Extract<keyof Topics, string>,
  >(
    config: ViewServerTopicConfig<Topics>,
    client: ViewServerRuntimeCoreInternalClient<Topics>,
    requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
    health: ViewServerGrpcHealthLedger<Topics>,
    feed: RuntimeMaterializedFeedDefinition<Topic>,
    feedName: string,
    values: ReadonlyArray<unknown>,
  ) {
    const rows = yield* Effect.forEach(values, (value) =>
      mapMaterializedValue(config, feed, feedName, value).pipe(
        Effect.tapError((error) =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((nowMillis) =>
              health.mappingFailed(feedName, {
                message: error.message,
                nowMillis,
              }),
            ),
          ),
        ),
      ),
    );
    yield* client.publishManyDecodedRows(feed.topic, rows).pipe(
      Effect.tapError((error) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((nowMillis) =>
            health.publishFailed(feedName, {
              message: error.message,
              nowMillis,
            }),
          ),
        ),
      ),
      Effect.mapError((cause) =>
        grpcIngressError({
          message: `gRPC feed publish failed for ${feedName}`,
          cause,
          phase: "publish",
          feedName,
          topic: feed.topic,
        }),
      ),
    );
    const nowMillis = yield* Clock.currentTimeMillis;
    yield* health.rowsPublished(feedName, {
      messages: values.length,
      rows: rows.length,
      nowMillis,
    });
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  },
);

const startMaterializedFeed = Effect.fn("ViewServerRuntime.grpc.materialized.start")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
  const Topic extends Extract<keyof Topics, string>,
>(
  scope: Scope.Scope,
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory,
  registerCloseFeedState: (closeFeedState: () => void) => void,
  feedName: string,
  feed: RuntimeMaterializedFeedDefinition<Topic>,
) {
  const clientDefinition = options.clients[feed.client];
  if (clientDefinition === undefined) {
    return yield* grpcIngressError({
      message: `gRPC feed ${feedName} references missing client: ${feed.client}`,
      cause: feed.client,
      phase: "configuration",
      feedName,
      topic: feed.topic,
    });
  }
  const baseUrl = options.clientBaseUrls[feed.client];
  if (baseUrl === undefined) {
    return yield* grpcIngressError({
      message: `gRPC feed ${feedName} references unresolved client URL: ${feed.client}`,
      cause: feed.client,
      phase: "configuration",
      feedName,
      topic: feed.topic,
    });
  }
  const grpcClient = yield* Effect.try({
    try: () => makeClient(clientDefinition, baseUrl),
    catch: (cause) =>
      grpcIngressError({
        message: `gRPC client creation failed for ${feedName}`,
        cause,
        phase: "client",
        feedName,
        topic: feed.topic,
      }),
  });
  const request = yield* callMaterializedGrpcSourceRequest(feedName, feed);
  const input = makeGrpcSourceInput(grpcClient, request, undefined);
  const startedAt = yield* Clock.currentTimeMillis;
  yield* health.clientConnected(feed.client, startedAt);
  yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  const maxReconnects = options.materializedReconnect.maxReconnects;
  const reconnectDelay = options.materializedReconnect.delay;
  let unstableExits = 0;
  let currentRunPublishedBatch = false;
  let currentRunStayedOpen = false;
  let closing = false;
  registerCloseFeedState(() => {
    closing = true;
  });
  const releaseFeedEffect = () => callMaterializedGrpcSourceRelease(feedName, feed, input);
  const acquireFeedStream = callMaterializedGrpcSourceAcquire(feedName, feed, input);
  const runFeedStream = Effect.fn("ViewServerRuntime.grpc.materialized.stream")(function* (
    stream: Stream.Stream<unknown, unknown>,
  ) {
    yield* health.feedReady(feedName);
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
    yield* Effect.sleep(reconnectDelay).pipe(
      Effect.flatMap(() =>
        Effect.sync(() => {
          currentRunStayedOpen = true;
        }),
      ),
      Effect.forkScoped({ startImmediately: true }),
      Effect.asVoid,
    );
    yield* stream.pipe(
      Stream.catchCause((cause) =>
        Stream.fail(
          new ViewServerGrpcIngressError({
            message: `gRPC feed stream failed for ${feedName}`,
            cause,
            phase: "stream",
            feedName,
            topic: feed.topic,
          }),
        ),
      ),
      Stream.groupedWithin(grpcMessageBatchSize, grpcMessageBatchFlushInterval),
      Stream.runForEach((values) =>
        publishMaterializedBatch(
          config,
          runtimeClient,
          requestHealthRefresh,
          health,
          feed,
          feedName,
          values,
        ).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              currentRunPublishedBatch = true;
            }),
          ),
        ),
      ),
    );
  });
  const runFeedOnce = Effect.fn("ViewServerRuntime.grpc.materialized.runOnce")(function* () {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const stream = yield* restore(acquireFeedStream);
        const useExit = yield* restore(Effect.scoped(runFeedStream(stream))).pipe(Effect.exit);
        const releaseExit = yield* (
          closing ? ignoreGrpcFeedReleaseFailure(releaseFeedEffect()) : releaseFeedEffect()
        ).pipe(Effect.exit);
        if (Exit.isFailure(releaseExit)) {
          if (closing) {
            return yield* materializedFeedExitEffect(useExit);
          }
          if (Exit.isFailure(useExit) && materializedFeedCauseHasInterrupts(useExit.cause)) {
            return yield* Effect.failCause(useExit.cause);
          }
          return yield* Effect.failCause(releaseExit.cause);
        }
        return yield* materializedFeedExitEffect(useExit);
      }),
    );
  });
  const runFeed = Effect.gen(function* () {
    while (true) {
      currentRunPublishedBatch = false;
      currentRunStayedOpen = false;
      const exit = yield* runFeedOnce().pipe(Effect.exit);
      if (Exit.isSuccess(exit)) {
        if (unstableExits < maxReconnects) {
          unstableExits += 1;
          yield* health.feedReconnecting(
            feedName,
            materializedFeedReconnectMessage(feedName, exit),
          );
          yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
          yield* Effect.sleep(reconnectDelay);
          continue;
        }
        yield* handleMaterializedFeedExit(
          requestHealthRefresh,
          health,
          feedName,
          feed.client,
          exit,
        );
        return;
      }
      if (Exit.isFailure(exit) && (currentRunPublishedBatch || currentRunStayedOpen)) {
        unstableExits = 0;
      }
      if (unstableExits < maxReconnects && canReconnectMaterializedFeedCause(exit.cause)) {
        unstableExits += 1;
        yield* health.feedReconnecting(feedName, materializedFeedReconnectMessage(feedName, exit));
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        yield* Effect.sleep(reconnectDelay);
        continue;
      }
      yield* handleMaterializedFeedExit(requestHealthRefresh, health, feedName, feed.client, exit);
      return;
    }
  });
  const fiber = yield* runFeed.pipe(Effect.forkIn(scope, { startImmediately: true }));
  yield* Scope.addFinalizer(scope, Fiber.interrupt(fiber));
});

export const makeViewServerGrpcIngress: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient?: ViewServerGrpcClientFactory,
) => Effect.Effect<ViewServerGrpcIngress, ViewServerGrpcIngressError> = Effect.fn(
  "ViewServerRuntime.grpc.makeIngress",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory = makeDefaultGrpcClient,
) {
  const scope = yield* Scope.make("parallel");
  const feedNames = Object.entries(options.feeds)
    .filter(([, feed]) => feed.lifecycle === "materialized")
    .map(([feedName]) => feedName);
  const closeFeedStates: Array<() => void> = [];
  return yield* Effect.gen(function* () {
    for (const [feedName, feed] of Object.entries(options.feeds)) {
      if (isRuntimeMaterializedFeed<Extract<keyof Topics, string>>(feed)) {
        yield* startMaterializedFeed(
          scope,
          config,
          client,
          requestHealthRefresh,
          options,
          health,
          makeClient,
          (closeFeedState) => {
            closeFeedStates.push(closeFeedState);
          },
          feedName,
          feed,
        );
      }
    }
    return {
      close: Effect.gen(function* () {
        for (const closeFeed of closeFeedStates) {
          closeFeed();
        }
        yield* Effect.forEach(feedNames, (feedName) => health.feedStopping(feedName), {
          discard: true,
        });
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        yield* Scope.close(scope, Exit.void);
      }),
    };
  }).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)));
});
