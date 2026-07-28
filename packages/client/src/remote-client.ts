import { BrowserSocket } from "@effect/platform-browser";
import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  GroupedResult,
  LiveQueryRow,
  PickRawFields,
  RawQuery,
  TopicDefinitions,
  TopicRow,
  ViewServerConfig,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
} from "@effect-view-server/config";
import {
  captureSourceHealthInput,
  runAllFinalizers,
  snapshotViewServerQuery,
  viewServerQuerySnapshotErrorMessage,
} from "@effect-view-server/effect-utils";
import {
  compileViewServerLiveEventCodec,
  ViewServerRpcs,
  viewServerDecodeHealth,
  viewServerDecodeHealthQuery,
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerEncodeLiveQuery,
  viewServerDecodeSourceHealth,
  viewServerEncodeSourceHealthRequest,
  type ViewServerRpcError,
  type ViewServerTrustedWireEvent,
  type ViewServerWireHealth,
  type ViewServerWireLiveQuery,
  type ViewServerWireSourceHealth,
} from "@effect-view-server/protocol";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  ManagedRuntime,
  Result,
  Semaphore,
  Scope,
  Stream,
} from "effect";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type {
  ViewServerLiveClient,
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
  ViewServerSourceHealthInputForTopic,
  ViewServerSourceHealthResultForTopic,
  ViewServerSourceHealthSubscription,
  ViewServerSourceOwnedTopic,
  ViewServerStatusEvent,
} from "./live-client";
import { makeRemoteHealthState } from "./remote-health";
import { makeRemoteSubscription } from "./remote-subscription";

export type ViewServerRemoteClientError = ViewServerRuntimeError | ViewServerTransportError;

export type ViewServerClientOptions = {
  readonly url: string;
  readonly subscriptionBufferSize?: number;
};

type ViewServerClientConstructionOptions = {
  readonly sourceHealthHandoff?: {
    readonly beforeReturn?: Effect.Effect<void>;
  };
};

export type ViewServerRemoteClient<Topics extends TopicDefinitions> = ViewServerLiveClient<Topics>;

const defaultSubscriptionBufferSize = 1_024;

const acquireRemoteResourceHandoff = <Value, Error, Requirements>(
  acquire: (
    markAcquired: (finalizer: Effect.Effect<void>) => Effect.Effect<void>,
  ) => Effect.Effect<Value, Error, Requirements>,
  beforeReturn: Effect.Effect<void> = Effect.void,
): Effect.Effect<Value, Error, Requirements> =>
  Effect.suspend(() => {
    let acquiredFinalizer: Effect.Effect<void> | undefined;
    const markAcquired = (finalizer: Effect.Effect<void>) =>
      Effect.sync(() => {
        acquiredFinalizer = finalizer;
      });
    return acquire(markAcquired).pipe(
      Effect.tap(() => beforeReturn),
      Effect.onExit((exit) =>
        Exit.isSuccess(exit) || acquiredFinalizer === undefined ? Effect.void : acquiredFinalizer,
      ),
    );
  });

const normalizeSubscriptionBufferSize = (subscriptionBufferSize: number | undefined): number => {
  if (subscriptionBufferSize === undefined) {
    return defaultSubscriptionBufferSize;
  }
  return Number.isSafeInteger(subscriptionBufferSize) && subscriptionBufferSize > 0
    ? subscriptionBufferSize
    : 1;
};

class ViewServerRpcClient extends Context.Service<
  ViewServerRpcClient,
  RpcClient.FromGroup<typeof ViewServerRpcs, RpcClientError>
>()("ViewServerRpcClient") {}

const rpcClientLayer = (url: string) =>
  Layer.effect(ViewServerRpcClient)(RpcClient.make(ViewServerRpcs)).pipe(
    Layer.provide(RpcClient.layerProtocolSocket()),
    Layer.provide([BrowserSocket.layerWebSocket(url), RpcSerialization.layerNdjson]),
  );

const transportError = (error: Error): ViewServerTransportError => ({
  _tag: "ViewServerTransportError",
  code: "TransportError",
  message: error.message,
});

export const mapViewServerRemoteError = (
  error: ViewServerRpcError | Error,
): ViewServerRemoteClientError => {
  if (error instanceof Error) {
    return transportError(error);
  }
  return error;
};

const subscriptionFailureStatus = <Topic extends string>(
  topic: Topic,
  error: ViewServerRemoteClientError,
): ViewServerStatusEvent<Topic> => {
  if (error.code === "BackpressureExceeded" || error.code === "SubscriptionClosed") {
    return {
      type: "status",
      topic,
      queryId: "remote",
      status: "closed",
      code: error.code,
      message: error.message,
    };
  }
  if (error.code === "SnapshotStale") {
    return {
      type: "status",
      topic,
      queryId: "remote",
      status: "stale",
      code: "SnapshotStale",
      message: error.message,
    };
  }
  return {
    type: "status",
    topic,
    queryId: "queryId" in error && error.queryId !== undefined ? error.queryId : "remote",
    status: "error",
    code: error.code,
    message: error.message,
  };
};

const subscriptionOverflowStatus = <Topic extends string>(
  topic: Topic,
  queuedEvents: number,
): ViewServerStatusEvent<Topic> => ({
  type: "status",
  topic,
  queryId: "remote",
  status: "closed",
  code: "BackpressureExceeded",
  message: `Remote subscription buffer exceeded capacity with ${queuedEvents} queued event(s).`,
});

export const makeViewServerClientWithConstructionOptions: <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerClientOptions,
  constructionOptions?: ViewServerClientConstructionOptions,
) => Effect.Effect<ViewServerRemoteClient<Topics>, ViewServerRemoteClientError> = Effect.fn(
  "ViewServerClient.remote.makeInternal",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerClientOptions,
  constructionOptions: ViewServerClientConstructionOptions = {},
) {
  const managedRuntime = ManagedRuntime.make(rpcClientLayer(options.url));
  const cleanupOnConstructionFailure = <Value, Error, Services>(
    effect: Effect.Effect<Value, Error, Services>,
  ): Effect.Effect<Value, Error, Services> =>
    effect.pipe(Effect.onError(() => managedRuntime.disposeEffect));

  const context = yield* cleanupOnConstructionFailure(managedRuntime.contextEffect);
  const rpc = Context.get(context, ViewServerRpcClient);

  const healthRpc = (): Effect.Effect<ViewServerWireHealth, ViewServerRemoteClientError> =>
    rpc["ViewServer.Health"](undefined).pipe(Effect.mapError(mapViewServerRemoteError));

  const subscriptionBufferSize = normalizeSubscriptionBufferSize(options.subscriptionBufferSize);

  const subscribeRpc = <Row, Topic extends string = string, Key extends string = string>(
    topic: Topic,
    query: ViewServerWireLiveQuery,
    decodeEvent: (
      event: ViewServerTrustedWireEvent,
    ) => Effect.Effect<ViewServerLiveEvent<Row, Topic, Key>, ViewServerRuntimeError>,
  ): Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>, ViewServerRemoteClientError> =>
    rpc["ViewServer.Subscribe"](
      {
        topic,
        query,
      },
      {
        streamBufferSize: subscriptionBufferSize,
      },
    ).pipe(Stream.mapError(mapViewServerRemoteError), Stream.mapEffect(decodeEvent));

  const initialHealth = yield* cleanupOnConstructionFailure(
    healthRpc().pipe(Effect.flatMap((next) => viewServerDecodeHealth(config, next))),
  );
  const remoteHealth = makeRemoteHealthState<Topics>(initialHealth);
  const clientScope = yield* Scope.make("parallel");
  type SharedSourceHealthEntry = {
    readonly close: Effect.Effect<void>;
    readonly stream: Stream.Stream<ViewServerWireSourceHealth, ViewServerRemoteClientError>;
    subscribers: number;
  };
  const sharedSourceHealth = new Map<string, SharedSourceHealthEntry>();
  const sharedSourceHealthLock = Semaphore.makeUnsafe(1);

  const close = runAllFinalizers([
    Scope.close(clientScope, Exit.void),
    managedRuntime.disposeEffect,
    remoteHealth.markStopping,
  ]);

  const streamToSubscription = <Row, Topic extends string = string, Key extends string = string>(
    topic: Topic,
    source: Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>, ViewServerRemoteClientError>,
    lifecycle: {
      readonly onOpen: Effect.Effect<void>;
      readonly onClose: Effect.Effect<void, unknown>;
    } = {
      onOpen: Effect.void,
      onClose: Effect.void,
    },
  ) =>
    makeRemoteSubscription<Row, ViewServerRemoteClientError, Topic, Key>({
      clientScope,
      failureStatus: subscriptionFailureStatus,
      lifecycle,
      overflowStatus: subscriptionOverflowStatus,
      source,
      subscriptionBufferSize,
      topic,
    });

  const subscribeWire = Effect.fn("ViewServerClient.remote.subscribe")(function* <
    Row,
    Topic extends Extract<keyof Topics, string>,
  >(
    topic: Topic,
    wireQuery: ViewServerWireLiveQuery,
    decodeEvent: (
      event: ViewServerTrustedWireEvent,
    ) => Effect.Effect<ViewServerLiveEvent<Row>, ViewServerRuntimeError>,
  ) {
    const stream = subscribeRpc<Row>(topic, wireQuery, decodeEvent);
    return yield* streamToSubscription(topic, stream, {
      onOpen: remoteHealth.updateSubscriptionCount(topic, 1),
      onClose: remoteHealth.updateSubscriptionCount(topic, -1),
    });
  });

  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<GroupedResult<TopicRow<Topics, Topic>, Query>>,
    ViewServerRemoteClientError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<PickRawFields<TopicRow<Topics, Topic>, Query>>,
    ViewServerRemoteClientError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRemoteClientError
  > {
    const capturedQuery = Result.try(() =>
      snapshotViewServerQuery<ExactLiveQueryInputForTopic<Topics, Topic, Query>>(query),
    );
    if (Result.isFailure(capturedQuery)) {
      return Effect.fail({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: viewServerQuerySnapshotErrorMessage,
        topic,
      });
    }
    return Effect.gen(function* () {
      type Row = LiveQueryRow<TopicRow<Topics, Topic>, Query>;
      const wireQuery = yield* viewServerEncodeLiveQuery(config, topic, capturedQuery.success);
      const eventCodec = compileViewServerLiveEventCodec(config, topic, capturedQuery.success);
      return yield* subscribeWire<Row, Topic>(topic, wireQuery, eventCodec.decodeTrusted);
    });
  }

  const subscribeHealthSummary = Effect.fn("ViewServerClient.remote.healthSummary.subscribe")(
    function* () {
      type Row = ViewServerHealthSummaryRow<Topics>;
      yield* viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, { select: ["id"] });
      const stream = subscribeRpc<Row, typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC, "summary">(
        VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        { select: ["id"] },
        (event) => viewServerDecodeHealthSummaryEvent<Topics>(config, event),
      );
      const subscription = yield* streamToSubscription<
        Row,
        typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        "summary"
      >(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, stream);
      const events = subscription.events.pipe(Stream.tap(remoteHealth.updateHealthSummaryRef));
      return {
        events,
        close: subscription.close,
      };
    },
  );

  const subscribeHealth = Effect.fn("ViewServerClient.remote.health.subscribe")(function* () {
    type Row = ViewServerHealthTopicRow<Extract<keyof Topics, string>>;
    yield* viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, { select: ["id"] });
    const stream = subscribeRpc<
      Row,
      typeof VIEW_SERVER_HEALTH_TOPIC,
      Extract<keyof Topics, string>
    >(VIEW_SERVER_HEALTH_TOPIC, { select: ["id"] }, (event) =>
      viewServerDecodeHealthTopicEvent<Topics>(config, event),
    );
    const subscription = yield* streamToSubscription<
      Row,
      typeof VIEW_SERVER_HEALTH_TOPIC,
      Extract<keyof Topics, string>
    >(VIEW_SERVER_HEALTH_TOPIC, stream);
    const events = subscription.events.pipe(Stream.tap(remoteHealth.updateHealthTopicRef));
    return {
      events,
      close: subscription.close,
    };
  });

  const subscribeSourceHealthWire = Effect.fn("ViewServerClient.remote.sourceHealth.subscribeWire")(
    function* <Topic extends ViewServerSourceOwnedTopic<Topics>>(
      topic: Topic,
      route: ReadonlyArray<object>,
    ): Effect.fn.Return<
      ViewServerSourceHealthSubscription<ViewServerSourceHealthResultForTopic<Topics, Topic>>,
      ViewServerRemoteClientError
    > {
      return yield* acquireRemoteResourceHandoff(
        (markAcquired) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const payload = yield* restore(
                viewServerEncodeSourceHealthRequest(config, topic, route),
              );
              const subscriptionKey = JSON.stringify(payload);
              const shared = yield* sharedSourceHealthLock.withPermit(
                acquireRemoteResourceHandoff((markEntryAcquired) =>
                  Effect.gen(function* () {
                    let entry = sharedSourceHealth.get(subscriptionKey);
                    if (entry === undefined) {
                      const entryScope = yield* Scope.fork(clientScope, "sequential");
                      yield* markEntryAcquired(Scope.close(entryScope, Exit.void));
                      const stream = yield* restore(
                        rpc["ViewServer.SourceHealth"](payload, {
                          streamBufferSize: subscriptionBufferSize,
                        }).pipe(
                          Stream.mapError(mapViewServerRemoteError),
                          Stream.share({
                            capacity: subscriptionBufferSize,
                            strategy: "suspend",
                            replay: 1,
                          }),
                          Effect.provideService(Scope.Scope, entryScope),
                        ),
                      );
                      const closeEntry = yield* Effect.cached(Scope.close(entryScope, Exit.void));
                      entry = {
                        close: closeEntry,
                        stream,
                        subscribers: 0,
                      };
                      sharedSourceHealth.set(subscriptionKey, entry);
                    }
                    entry.subscribers += 1;
                    const sharedEntry = entry;
                    const releaseShared = yield* Effect.cached(
                      sharedSourceHealthLock.withPermit(
                        Effect.suspend(() => {
                          sharedEntry.subscribers -= 1;
                          if (
                            sharedEntry.subscribers === 0 &&
                            sharedSourceHealth.get(subscriptionKey) === sharedEntry
                          ) {
                            sharedSourceHealth.delete(subscriptionKey);
                            return sharedEntry.close;
                          }
                          return Effect.void;
                        }),
                      ),
                    );
                    return {
                      entry: sharedEntry,
                      release: releaseShared,
                    };
                  }),
                ),
              );
              yield* markAcquired(shared.release);
              const subscriptionScope = yield* Scope.fork(clientScope, "sequential");
              yield* markAcquired(
                runAllFinalizers([Scope.close(subscriptionScope, Exit.void), shared.release]),
              );
              const interrupted = yield* Deferred.make<void>();
              yield* Scope.addFinalizer(
                subscriptionScope,
                Deferred.succeed(interrupted, undefined).pipe(
                  Effect.asVoid,
                  Effect.andThen(shared.release),
                ),
              );
              const closeSubscription = yield* Effect.cached(
                Scope.close(subscriptionScope, Exit.void),
              );
              yield* markAcquired(closeSubscription);
              const events = shared.entry.stream.pipe(
                Stream.mapEffect((value) =>
                  viewServerDecodeSourceHealth<Topics, Topic>(config, topic, value),
                ),
                Stream.interruptWhen(Deferred.await(interrupted)),
                Stream.ensuring(closeSubscription),
              );
              return {
                events,
                close: () => closeSubscription,
              };
            }),
          ),
        constructionOptions.sourceHealthHandoff?.beforeReturn,
      );
    },
  );

  function subscribeSourceHealth<
    const Input extends {
      readonly topic: ViewServerSourceOwnedTopic<Topics>;
    },
  >(
    input: ViewServerSourceHealthInputForTopic<Topics, Input["topic"], Input>,
  ): Effect.Effect<
    ViewServerSourceHealthSubscription<
      ViewServerSourceHealthResultForTopic<Topics, Input["topic"]>
    >,
    ViewServerRemoteClientError
  > {
    const captured = captureSourceHealthInput<Input["topic"]>(input);
    if (Result.isFailure(captured)) {
      return Effect.fail({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Source Health input must be one exact { topic, routeBy? } object.",
        topic: "<invalid>",
      });
    }
    return subscribeSourceHealthWire<Input["topic"]>(
      captured.success.topic,
      captured.success.route,
    );
  }

  return {
    subscribe,
    subscribeHealthSummary,
    subscribeHealth,
    subscribeSourceHealth,
    health: remoteHealth.readonlyHealth,
    close,
  };
});

export const makeViewServerClient: <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerClientOptions,
) => Effect.Effect<ViewServerRemoteClient<Topics>, ViewServerRemoteClientError> = Effect.fn(
  "ViewServerClient.remote.make",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerClientOptions,
) {
  return yield* makeViewServerClientWithConstructionOptions(config, options);
});

export const createViewServerClient = makeViewServerClient;
