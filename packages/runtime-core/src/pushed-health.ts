import type {
  ViewServerLiveClient,
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
} from "@effect-view-server/client";
import type {
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
} from "@effect-view-server/config";
import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@effect-view-server/effect-utils";
import { Cause, Clock, Effect, Queue, Semaphore, Stream, type Duration } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { makeCoalescedHealthReader, makeHealthRefreshScheduler } from "./health";
import {
  acquireRuntimeCoreResourceHandoff,
  type RuntimeCoreResourceHandoffOptions,
} from "./subscription-handoff";

const runtimeClosedError: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "Runtime Core is closed.",
};

const ignoreHealthSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring runtime health subscription close failure.",
);

const runtimeHealthBackpressureStatus = <Topic extends string>(
  topic: Topic,
  queryId: string,
  queuedEvents: number,
) =>
  ({
    type: "status",
    topic,
    queryId,
    status: "closed",
    code: "BackpressureExceeded",
    message: `Runtime health subscription closed because its event queue exceeded capacity with ${queuedEvents} queued event(s).`,
  }) satisfies ViewServerLiveEvent<never, Topic, never>;

export type RuntimeCorePushedHealthHub<Topics extends DecodableTopicDefinitions> = Pick<
  ViewServerLiveClient<Topics>,
  "health" | "subscribeHealth" | "subscribeHealthSummary"
> & {
  readonly close: Effect.Effect<void>;
  readonly refresh: Effect.Effect<ViewServerHealth<Topics>>;
  readonly requestRefresh: Effect.Effect<void>;
};

export type RuntimeCorePushedHealthHubOptions = {
  readonly afterRefreshEpochClaim?: Effect.Effect<void>;
  readonly afterSubscriptionCloseClaim?: Effect.Effect<void>;
  readonly beforeSubscriptionRegistration?: Effect.Effect<void>;
  readonly subscriptionHandoff?: RuntimeCoreResourceHandoffOptions;
};

type HealthSubscriptionCloseReason =
  | {
      readonly _tag: "normal";
    }
  | {
      readonly _tag: "backpressure";
      readonly queuedEvents: number;
    };

type HealthSnapshotOfferResult =
  | {
      readonly _tag: "offered";
    }
  | {
      readonly _tag: "backpressure";
      readonly queuedEvents: number;
    };

type RuntimeCoreHealthSummarySnapshot<Topics extends DecodableTopicDefinitions> = Extract<
  ViewServerLiveEvent<
    ViewServerHealthSummaryRow<Topics>,
    typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    "summary"
  >,
  { readonly type: "snapshot" }
>;

type RuntimeCoreHealthDetailSnapshot<Topics extends DecodableTopicDefinitions> = Extract<
  ViewServerLiveEvent<
    ViewServerHealthTopicRow<Extract<keyof Topics, string>>,
    typeof VIEW_SERVER_HEALTH_TOPIC,
    Extract<keyof Topics, string>
  >,
  { readonly type: "snapshot" }
>;

type RuntimeCoreHealthSnapshots<Topics extends DecodableTopicDefinitions> = {
  readonly detail: RuntimeCoreHealthDetailSnapshot<Topics>;
  readonly summary: RuntimeCoreHealthSummarySnapshot<Topics>;
};

const healthSnapshotsFromHealth = <Topics extends DecodableTopicDefinitions>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): RuntimeCoreHealthSnapshots<Topics> => {
  const detailRows = viewServerHealthTopicRowsFromHealth(health, updatedAtNanos);
  return {
    detail: {
      type: "snapshot",
      topic: VIEW_SERVER_HEALTH_TOPIC,
      queryId: "health",
      version: health.version,
      keys: detailRows.map((row) => row.id),
      rows: detailRows,
      totalRows: detailRows.length,
    },
    summary: {
      type: "snapshot",
      topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
      queryId: "health-summary",
      version: health.version,
      keys: ["summary"],
      rows: [viewServerHealthSummaryRowFromHealth(health, updatedAtNanos)],
      totalRows: 1,
    },
  };
};

export const makeRuntimeCorePushedHealthHub = Effect.fn("ViewServerRuntimeCore.pushedHealth.make")(
  function* <const Topics extends DecodableTopicDefinitions>(
    initialHealth: ViewServerHealth<Topics>,
    readHealth: Effect.Effect<ViewServerHealth<Topics>>,
    cadence?: Duration.Input,
    options: RuntimeCorePushedHealthHubOptions = {},
  ) {
    type ActiveHealthSubscription = {
      finishBackpressure: (queuedEvents: number) => Effect.Effect<void>;
      finishNormal: Effect.Effect<void>;
      offer: (snapshots: RuntimeCoreHealthSnapshots<Topics>) => HealthSnapshotOfferResult;
    };

    const initialUpdatedAtNanos = yield* Clock.currentTimeNanos;
    const health = AtomRef.make(initialHealth);
    let installedSnapshots = healthSnapshotsFromHealth(initialHealth, initialUpdatedAtNanos);
    const activeHealthSubscriptions = new Set<ActiveHealthSubscription>();
    const healthSubscriptionLock = Semaphore.makeUnsafe(1);
    let hubClosed = false;
    let installedRefreshEpoch = 0;
    let requestedRefreshEpoch = 0;

    const installHealth = Effect.fn("ViewServerRuntimeCore.pushedHealth.install")(function* (
      nextHealth: ViewServerHealth<Topics>,
      refreshEpoch: number,
    ) {
      const updatedAtNanos = yield* Clock.currentTimeNanos;
      const result = yield* healthSubscriptionLock.withPermit(
        Effect.gen(function* () {
          const claim = yield* Effect.sync(() => {
            if (hubClosed || refreshEpoch !== requestedRefreshEpoch) {
              return { _tag: "stale" as const, installed: health.value };
            }
            installedRefreshEpoch = refreshEpoch;
            installedSnapshots = healthSnapshotsFromHealth(nextHealth, updatedAtNanos);
            const backpressureFinalizers: Array<Effect.Effect<void>> = [];
            for (const subscription of activeHealthSubscriptions) {
              const offerResult = subscription.offer(installedSnapshots);
              if (offerResult._tag === "backpressure") {
                activeHealthSubscriptions.delete(subscription);
                backpressureFinalizers.push(
                  subscription.finishBackpressure(offerResult.queuedEvents),
                );
              }
            }
            return { _tag: "installed" as const, backpressureFinalizers };
          });
          if (claim._tag === "stale") {
            return claim.installed;
          }
          yield* runAllFinalizers([
            Effect.sync(() => health.update(() => nextHealth)),
            ...claim.backpressureFinalizers,
          ]);
          return health.value;
        }),
      );
      return result;
    });

    const coalescedHealthReader = makeCoalescedHealthReader(
      (refreshEpoch) =>
        readHealth.pipe(Effect.flatMap((nextHealth) => installHealth(nextHealth, refreshEpoch))),
      () => requestedRefreshEpoch,
    );
    const refresh = Effect.fn("ViewServerRuntimeCore.pushedHealth.refresh")(function* () {
      while (true) {
        const beforeRead = yield* healthSubscriptionLock.withPermit(
          Effect.sync(() =>
            hubClosed
              ? { _tag: "closed" as const, health: health.value }
              : { _tag: "read" as const },
          ),
        );
        if (beforeRead._tag === "closed") {
          return beforeRead.health;
        }
        yield* coalescedHealthReader();
        const afterRead = yield* healthSubscriptionLock.withPermit(
          Effect.sync(() =>
            hubClosed || installedRefreshEpoch === requestedRefreshEpoch
              ? { _tag: "settled" as const, health: health.value }
              : { _tag: "retry" as const },
          ),
        );
        if (afterRead._tag === "settled") {
          return afterRead.health;
        }
      }
    });
    const flushPendingRefresh = Effect.fn("ViewServerRuntimeCore.pushedHealth.flushPendingRefresh")(
      function* () {
        const pending = yield* healthSubscriptionLock.withPermit(
          Effect.sync(() => !hubClosed && installedRefreshEpoch !== requestedRefreshEpoch),
        );
        if (pending) {
          yield* refresh();
        }
      },
    );
    const scheduler = yield* makeHealthRefreshScheduler(flushPendingRefresh(), cadence);
    const requestRefresh = Effect.fn("ViewServerRuntimeCore.pushedHealth.requestRefresh")(
      function* () {
        yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const shouldSchedule = yield* healthSubscriptionLock.withPermit(
              Effect.sync(() => {
                if (hubClosed) {
                  return false;
                }
                requestedRefreshEpoch += 1;
                return true;
              }),
            );
            if (shouldSchedule) {
              yield* options.afterRefreshEpochClaim ?? Effect.void;
              yield* scheduler.request;
            }
          }),
        );
      },
    );

    const closeActiveHealthSubscriptions = Effect.suspend(() =>
      healthSubscriptionLock.withPermit(
        Effect.gen(function* () {
          const subscriptions = yield* Effect.sync(() => {
            hubClosed = true;
            const claimed = Array.from(activeHealthSubscriptions);
            activeHealthSubscriptions.clear();
            return claimed;
          });
          yield* runAllFinalizers([
            Effect.sync(() =>
              health.update((current) => ({
                ...current,
                status: "stopping",
              })),
            ),
            ...subscriptions.map((subscription) => subscription.finishNormal),
          ]);
        }),
      ),
    ).pipe(ignoreHealthSubscriptionCloseFailure);

    const close = (yield* Effect.cached(
      runAllFinalizers([scheduler.close, closeActiveHealthSubscriptions]),
    )).pipe(Effect.uninterruptible);

    const makeHealthSubscription = Effect.fn("ViewServerRuntimeCore.pushedHealth.subscribe")(
      function* <
        Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC | typeof VIEW_SERVER_HEALTH_TOPIC,
        Key extends string,
        Row extends { readonly id: Key },
      >(
        topic: Topic,
        queryId: string,
        selectSnapshot: (
          snapshots: RuntimeCoreHealthSnapshots<Topics>,
        ) => Extract<ViewServerLiveEvent<Row, Topic, Key>, { readonly type: "snapshot" }>,
      ) {
        return yield* acquireRuntimeCoreResourceHandoff(
          (markAcquired) =>
            Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const queue = yield* Queue.dropping<
                  ViewServerLiveEvent<Row, Topic, Key>,
                  Cause.Done
                >(64);
                const finishClosedSubscription = Effect.fn(
                  "ViewServerRuntimeCore.pushedHealth.subscription.finishClosed",
                )(function* (reason: HealthSubscriptionCloseReason) {
                  if (reason._tag === "backpressure") {
                    yield* Queue.clear(queue);
                    yield* Queue.offer(
                      queue,
                      runtimeHealthBackpressureStatus(topic, queryId, reason.queuedEvents),
                    );
                  }
                  yield* Queue.end(queue);
                });
                const subscription: ActiveHealthSubscription = {
                  finishBackpressure: (queuedEvents) =>
                    finishClosedSubscription({ _tag: "backpressure", queuedEvents }),
                  finishNormal: finishClosedSubscription({ _tag: "normal" }),
                  offer: (snapshots) => {
                    const offered = Queue.offerUnsafe(queue, selectSnapshot(snapshots));
                    return offered
                      ? { _tag: "offered" }
                      : { _tag: "backpressure", queuedEvents: Queue.sizeUnsafe(queue) };
                  },
                };
                const closeSubscription = Effect.fn(
                  "ViewServerRuntimeCore.pushedHealth.subscription.close",
                )(function* () {
                  yield* Effect.uninterruptible(
                    Effect.gen(function* () {
                      const shouldClose = yield* healthSubscriptionLock.withPermit(
                        Effect.sync(() => activeHealthSubscriptions.delete(subscription)),
                      );
                      if (shouldClose) {
                        yield* options.afterSubscriptionCloseClaim ?? Effect.void;
                        yield* subscription.finishNormal;
                      }
                    }),
                  );
                });
                const releaseSubscriptionAndEndQueue = () => closeSubscription();
                let registration: "closed" | "registered" | "retry" = "retry";
                while (registration === "retry") {
                  yield* restore(flushPendingRefresh());
                  yield* restore(options.beforeSubscriptionRegistration ?? Effect.void);
                  registration = yield* healthSubscriptionLock.withPermit(
                    Effect.sync(() => {
                      if (hubClosed) {
                        return "closed";
                      }
                      if (installedRefreshEpoch !== requestedRefreshEpoch) {
                        return "retry";
                      }
                      Queue.offerUnsafe(queue, selectSnapshot(installedSnapshots));
                      activeHealthSubscriptions.add(subscription);
                      return "registered";
                    }),
                  );
                }
                if (registration === "closed") {
                  yield* Queue.end(queue);
                  return yield* Effect.fail(runtimeClosedError);
                }
                const acquired = {
                  events: Stream.fromQueue(queue).pipe(
                    Stream.ensuring(releaseSubscriptionAndEndQueue()),
                  ),
                  close: releaseSubscriptionAndEndQueue,
                } satisfies ViewServerLiveSubscription<Row, Topic, Key>;
                yield* markAcquired(acquired.close());
                return acquired;
              }),
            ),
          options.subscriptionHandoff,
        );
      },
    );

    return {
      close,
      health: health.map((value) => value),
      refresh: refresh(),
      requestRefresh: requestRefresh(),
      subscribeHealthSummary: () =>
        makeHealthSubscription<
          typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          "summary",
          ViewServerHealthSummaryRow<Topics>
        >(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, "health-summary", (snapshots) => snapshots.summary),
      subscribeHealth: () =>
        makeHealthSubscription<
          typeof VIEW_SERVER_HEALTH_TOPIC,
          Extract<keyof Topics, string>,
          ViewServerHealthTopicRow<Extract<keyof Topics, string>>
        >(VIEW_SERVER_HEALTH_TOPIC, "health", (snapshots) => snapshots.detail),
    } satisfies RuntimeCorePushedHealthHub<Topics>;
  },
);
