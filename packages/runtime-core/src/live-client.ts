import type {
  ColumnLiveViewEngineError,
  ColumnLiveViewSubscription,
  DecodableTopicDefinitions,
} from "@effect-view-server/column-live-view-engine";
import type {
  ColumnLiveViewEngineInternal,
  ColumnLiveViewTerminalObserver,
} from "@effect-view-server/column-live-view-engine/internal";
import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import type {
  ViewServerTopicConfig,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
import {
  snapshotViewServerQuery,
  viewServerQuerySnapshotErrorMessage,
} from "@effect-view-server/effect-utils";
import { Effect, Result, Stream } from "effect";
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
  ): Effect.Effect<ViewServerRuntimeCoreLiveClientModule<Topics>> =>
    Effect.sync(() => {
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
      const subscribeQuery = (
        topic: Extract<keyof Topics, string>,
        query: Readonly<Record<string, unknown>>,
      ): Effect.Effect<
        ViewServerLiveSubscription<object>,
        ViewServerRuntimeError | ViewServerTransportError
      > => {
        const capturedQuery = captureRoutedQuery(topic, query);
        return Effect.fromResult(capturedQuery).pipe(
          Effect.flatMap((ownedQuery) =>
            wrapEngineSubscription(
              engine.subscribeRuntime(topic, engineQueryWithoutRoute(ownedQuery)),
            ),
          ),
        );
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
        return Effect.fromResult(capturedQuery).pipe(
          Effect.flatMap((ownedQuery) => {
            const engineQuery = engineQueryWithoutRoute(ownedQuery);
            return wrapEngineSubscription(
              partition === undefined
                ? engine.subscribeRuntimeObserved(topic, engineQuery, terminalObserver)
                : engine.subscribeRuntimeObservedPartitioned(
                    topic,
                    engineQuery,
                    partition,
                    terminalObserver,
                  ),
            );
          }),
        );
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
            sourceOwnership.requirePublicReadAllowed(topic, "runtimeCore"),
        });
      const subscribeRuntime = adaptRuntimeQuerySubscriber<Topics>(subscribeRuntimeQuery);
      const protocolQuerySubscriber: ViewServerRuntimeCoreProtocolQuerySubscriber<Topics> = {
        subscribeProtocolQuery: subscribeRuntimeQuery,
      };
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
        health: pushedHealth.health,
      } satisfies ViewServerRuntimeCoreLiveClientModule<Topics>["liveClient"];
      return { liveClient, protocolQuerySubscriber };
    }),
);
