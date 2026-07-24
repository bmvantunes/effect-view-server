import type {
  ColumnLiveViewEngineError,
  ColumnLiveViewSubscription,
  DecodableTopicDefinitions,
} from "@effect-view-server/column-live-view-engine";
import type {
  ColumnLiveViewEngineInternal,
  ColumnLiveViewTerminalObserver,
} from "@effect-view-server/column-live-view-engine/internal";
import type {
  ViewServerLiveSubscription,
  ViewServerSourceHealthSubscriber,
} from "@effect-view-server/client";
import type {
  ViewServerTopicConfig,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
import {
  snapshotViewServerQuery,
  runAllFinalizers,
  viewServerQuerySnapshotErrorMessage,
} from "@effect-view-server/effect-utils";
import { Deferred, Effect, Option, Result, Stream } from "effect";
import { engineQueryWithoutRoute } from "./engine-query";
import { makeRuntimeCoreLiveQueryFacade } from "./live-query-facade";
import type {
  ViewServerRuntimeCoreInternalLiveClient,
  ViewServerRuntimeCoreLiveClientModule,
  ViewServerRuntimeCoreQueryPartition,
} from "./live-client-contract";
import type { ViewServerRuntimeCoreProtocolQuerySubscriber } from "./protocol-query-subscriber";
import type { RuntimeCorePushedHealthHub } from "./pushed-health";
import { engineErrorToRuntimeError, invalidRuntimeQueryError } from "./runtime-error";
import { adaptRuntimeQuerySubscriber } from "./runtime-query-subscriber";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";
import { acquireRuntimeCoreResourceHandoff } from "./subscription-handoff";
import type { RuntimeCoreSourceManager } from "./source-runtime";

export const acquireRuntimeCoreLiveSubscription = Effect.fn(
  "ViewServerRuntimeCore.liveClient.acquireSubscription",
)(
  <Row>(
    acquisition: Effect.Effect<ColumnLiveViewSubscription<Row>, ColumnLiveViewEngineError>,
    requestHealthRefresh: Effect.Effect<void>,
  ): Effect.Effect<ViewServerLiveSubscription<Row>, ViewServerRuntimeError> =>
    Effect.suspend(() =>
      acquireRuntimeCoreResourceHandoff((markAcquired) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const subscription = yield* restore(
              acquisition.pipe(Effect.mapError(engineErrorToRuntimeError)),
            );
            const closeSubscription = subscription
              .close()
              .pipe(Effect.ensuring(requestHealthRefresh));
            yield* markAcquired(closeSubscription);
            const wrapped = {
              events: subscription.events.pipe(Stream.ensuring(closeSubscription)),
              close: () => closeSubscription,
            } satisfies ViewServerLiveSubscription<Row>;
            yield* restore(requestHealthRefresh);
            return wrapped;
          }),
        ),
      ),
    ),
);

export const makeRuntimeCoreLiveClientModule = Effect.fn("ViewServerRuntimeCore.liveClient.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    engine: ColumnLiveViewEngineInternal<Topics>,
    pushedHealth: RuntimeCorePushedHealthHub<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    sourceManager?: RuntimeCoreSourceManager<Topics>,
  ): Effect.Effect<ViewServerRuntimeCoreLiveClientModule<Topics>> =>
    Effect.sync(() => {
      const subscribeMissingSourceHealth: ViewServerSourceHealthSubscriber<
        Topics,
        ViewServerRuntimeError
      > = (...arguments_) => {
        const [topic] = arguments_;
        return Effect.fail(invalidRuntimeQueryError(topic, `Topic ${topic} has no Source.`));
      };
      const sources: Pick<
        RuntimeCoreSourceManager<Topics>,
        "acquireLeased" | "decorateMaterialized" | "subscribeSourceHealth"
      > = sourceManager ?? {
        acquireLeased: () => Effect.succeed(Option.none()),
        subscribeSourceHealth: subscribeMissingSourceHealth,
        decorateMaterialized: (_topic, subscription) => subscription,
      };
      const sourceOwnership = makeSourceOwnershipPolicy(config);
      const captureQuery = <Query extends Readonly<Record<string, unknown>>>(query: Query) =>
        Result.try(() => snapshotViewServerQuery(query));
      const querySnapshotError = (topic: string): ViewServerRuntimeError =>
        invalidRuntimeQueryError(topic, viewServerQuerySnapshotErrorMessage);
      const validateSourceRoute = (
        topic: string,
        query: Readonly<Record<string, unknown>>,
      ): ViewServerRuntimeError | undefined => {
        const routeError = validateLiveQuerySourceRoute(config.topics, topic, query);
        return routeError === undefined ? undefined : invalidRuntimeQueryError(topic, routeError);
      };
      const captureRoutedQuery = (
        topic: string,
        query: Readonly<Record<string, unknown>>,
      ): Result.Result<Readonly<Record<string, unknown>>, ViewServerRuntimeError> => {
        const capturedQuery = Result.mapError(captureQuery(query), () => querySnapshotError(topic));
        if (Result.isFailure(capturedQuery)) {
          return capturedQuery;
        }
        const routeError = validateSourceRoute(topic, capturedQuery.success);
        return routeError === undefined ? capturedQuery : Result.fail(routeError);
      };
      const wrapEngineSubscription = <Row>(
        acquisition: Effect.Effect<ColumnLiveViewSubscription<Row>, ColumnLiveViewEngineError>,
      ): Effect.Effect<ViewServerLiveSubscription<Row>, ViewServerRuntimeError> =>
        acquireRuntimeCoreLiveSubscription(acquisition, requestHealthRefresh);
      const attachSourceLease = <Row extends object>(
        subscription: ViewServerLiveSubscription<Row>,
        query: Readonly<Record<string, unknown>>,
        lease: import("./source-runtime").RuntimeCoreSourceLease,
        queryId: string,
      ) =>
        Effect.gen(function* () {
          const translated = lease.translate(subscription, query, queryId);
          const close = yield* Effect.cached(
            runAllFinalizers<ViewServerTransportError, never>([translated.close(), lease.release]),
          );
          const closeFinalizer = Effect.ignore(close);
          return {
            events: translated.events.pipe(Stream.ensuring(closeFinalizer)),
            close: () => close,
          };
        });
      const captureQueryId = (
        queryId: Deferred.Deferred<string>,
        delegate?: ColumnLiveViewTerminalObserver,
      ): ColumnLiveViewTerminalObserver => ({
        onQueryRegistered: (registeredQueryId) =>
          Deferred.succeed(queryId, registeredQueryId).pipe(
            Effect.andThen(delegate?.onQueryRegistered(registeredQueryId) ?? Effect.void),
          ),
        onTerminalOccurrence: (event) => delegate?.onTerminalOccurrence(event) ?? Effect.void,
        onTerminalReady: (event) => delegate?.onTerminalReady(event) ?? Effect.void,
      });
      const subscribeQuery = (
        topic: Extract<keyof Topics, string>,
        query: Readonly<Record<string, unknown>>,
      ): Effect.Effect<
        ViewServerLiveSubscription<object>,
        ViewServerRuntimeError | ViewServerTransportError
      > => {
        const capturedQuery = captureRoutedQuery(topic, query);
        return Effect.gen(function* () {
          const ownedQuery = yield* Effect.fromResult(capturedQuery);
          const lease = yield* sources.acquireLeased(topic, ownedQuery);
          return yield* acquireRuntimeCoreResourceHandoff((markAcquired) =>
            Effect.gen(function* () {
              if (Option.isSome(lease)) {
                yield* markAcquired(lease.value.release);
              }
              const registeredQueryId = yield* Deferred.make<string>();
              const terminalObserver = captureQueryId(registeredQueryId);
              const subscription = yield* wrapEngineSubscription(
                Option.isNone(lease)
                  ? engine.subscribeRuntimeObserved(
                      topic,
                      engineQueryWithoutRoute(ownedQuery),
                      terminalObserver,
                    )
                  : engine.subscribeRuntimeObservedPartitioned(
                      topic,
                      engineQueryWithoutRoute(ownedQuery),
                      lease.value.partition,
                      terminalObserver,
                    ),
              );
              yield* markAcquired(
                Option.isNone(lease)
                  ? Effect.ignore(subscription.close())
                  : Effect.ignore(runAllFinalizers([subscription.close(), lease.value.release])),
              );
              const queryId = yield* Deferred.await(registeredQueryId);
              const result = Option.isNone(lease)
                ? sources.decorateMaterialized(topic, subscription, queryId)
                : yield* attachSourceLease(subscription, ownedQuery, lease.value, queryId);
              yield* markAcquired(Effect.ignore(result.close()));
              return result;
            }),
          );
        });
      };
      const subscribeObservedQuery = <Topic extends Extract<keyof Topics, string>>(
        topic: Topic,
        query: Readonly<Record<string, unknown>>,
        terminalObserver: ColumnLiveViewTerminalObserver,
        partition?: ViewServerRuntimeCoreQueryPartition,
      ): Effect.Effect<
        ViewServerLiveSubscription<object>,
        ViewServerRuntimeError | ViewServerTransportError
      > => {
        const capturedQuery = captureRoutedQuery(topic, query);
        return Effect.gen(function* () {
          const ownedQuery = yield* Effect.fromResult(capturedQuery);
          const engineQuery = engineQueryWithoutRoute(ownedQuery);
          if (partition !== undefined) {
            return yield* wrapEngineSubscription(
              engine.subscribeRuntimeObservedPartitioned(
                topic,
                engineQuery,
                partition,
                terminalObserver,
              ),
            );
          }
          const lease = yield* sources.acquireLeased(topic, ownedQuery);
          return yield* acquireRuntimeCoreResourceHandoff((markAcquired) =>
            Effect.gen(function* () {
              if (Option.isSome(lease)) {
                yield* markAcquired(lease.value.release);
              }
              const registeredQueryId = yield* Deferred.make<string>();
              const capturingObserver = captureQueryId(registeredQueryId, terminalObserver);
              const subscription = yield* wrapEngineSubscription(
                Option.isNone(lease)
                  ? engine.subscribeRuntimeObserved(topic, engineQuery, capturingObserver)
                  : engine.subscribeRuntimeObservedPartitioned(
                      topic,
                      engineQuery,
                      lease.value.partition,
                      capturingObserver,
                    ),
              );
              yield* markAcquired(
                Option.isNone(lease)
                  ? Effect.ignore(subscription.close())
                  : Effect.ignore(runAllFinalizers([subscription.close(), lease.value.release])),
              );
              const queryId = yield* Deferred.await(registeredQueryId);
              const result = Option.isNone(lease)
                ? sources.decorateMaterialized(topic, subscription, queryId)
                : yield* attachSourceLease(subscription, ownedQuery, lease.value, queryId);
              yield* markAcquired(Effect.ignore(result.close()));
              return result;
            }),
          );
        });
      };
      const subscribeRuntimeQuery = (
        topic: Extract<keyof Topics, string>,
        query: Readonly<Record<string, unknown>>,
      ): Effect.Effect<
        ViewServerLiveSubscription<object>,
        ViewServerRuntimeError | ViewServerTransportError
      > => {
        const acquisition = subscribeQuery(topic, query);
        return sourceOwnership
          .requirePublicReadAllowed(topic, "runtimeCore")
          .pipe(Effect.flatMap(() => acquisition));
      };
      const subscribeRuntimeInternal: ViewServerRuntimeCoreInternalLiveClient<Topics>["subscribeRuntimeInternal"] =
        (topic, query) => {
          const capturedQuery = captureQuery(query);
          return Effect.fromResult(
            Result.mapError(capturedQuery, () => querySnapshotError(topic)),
          ).pipe(
            Effect.flatMap((ownedQuery) =>
              wrapEngineSubscription(engine.subscribeRuntime(topic, ownedQuery)),
            ),
          );
        };
      const subscribeRuntimeRoutedInternal: ViewServerRuntimeCoreInternalLiveClient<Topics>["subscribeRuntimeRoutedInternal"] =
        (topic, query) => subscribeQuery(topic, query);
      const subscribeRuntimeObservedInternal: ViewServerRuntimeCoreInternalLiveClient<Topics>["subscribeRuntimeObservedInternal"] =
        (topic, query, terminalObserver, partition) => {
          const capturedQuery = captureQuery(query);
          return Effect.fromResult(
            Result.mapError(capturedQuery, () => querySnapshotError(topic)),
          ).pipe(
            Effect.flatMap((ownedQuery) =>
              wrapEngineSubscription(
                partition === undefined
                  ? engine.subscribeRuntimeObserved(topic, ownedQuery, terminalObserver)
                  : engine.subscribeRuntimeObservedPartitioned(
                      topic,
                      ownedQuery,
                      partition,
                      terminalObserver,
                    ),
              ),
            ),
          );
        };
      const { subscribe, subscribeInternal, subscribeObservedInternal } =
        makeRuntimeCoreLiveQueryFacade<Topics>({
          subscribeQuery,
          subscribeObservedQuery,
          requirePublicReadAllowed: (topic) =>
            sourceOwnership.topics.get(topic)?.sourceLeased === true
              ? Effect.void
              : sourceOwnership.requirePublicReadAllowed(topic, "runtimeCore"),
        });
      const subscribeRuntime = adaptRuntimeQuerySubscriber<Topics>(subscribeRuntimeQuery);
      const protocolQuerySubscriber: ViewServerRuntimeCoreProtocolQuerySubscriber<Topics> = {
        subscribeProtocolQuery: subscribeRuntimeQuery,
      };
      const subscribeSourceHealth: ViewServerRuntimeCoreLiveClientModule<Topics>["liveClient"]["subscribeSourceHealth"] =
        (...arguments_) => sources.subscribeSourceHealth(...arguments_);
      const liveClient = {
        subscribe,
        subscribeRuntime,
        subscribeInternal,
        subscribeObservedInternal,
        subscribeRuntimeInternal,
        subscribeRuntimeRoutedInternal,
        subscribeRuntimeObservedInternal,
        subscribeHealthSummary: pushedHealth.subscribeHealthSummary,
        subscribeHealth: pushedHealth.subscribeHealth,
        subscribeSourceHealth,
        health: pushedHealth.health,
      } satisfies ViewServerRuntimeCoreLiveClientModule<Topics>["liveClient"];
      return { liveClient, protocolQuerySubscriber };
    }),
);
