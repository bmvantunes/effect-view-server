import { Cause, Effect, Exit, Queue, Ref, Scope, Stream } from "effect";
import { constant } from "effect/Function";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import type {
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
  ViewServerStatusEvent,
} from "./live-client";

const ignoreRemoteSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Remote subscription close failed.",
);
const ignoreRemoteSubscriptionStreamStartFailure =
  ignoreLoggedTypedFailuresPreserveNonTypedFailures("Remote subscription stream start failed.");

const completeQueueFromProducerExit = <Row, Topic extends string, Key extends string>(
  queue: Queue.Enqueue<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>,
  exit: Exit.Exit<void>,
) => {
  if (Exit.isSuccess(exit)) {
    return Queue.end(queue);
  }
  if (
    Cause.hasInterrupts(exit.cause) &&
    !Cause.hasDies(exit.cause) &&
    !Cause.hasFails(exit.cause)
  ) {
    return Queue.end(queue);
  }
  return Queue.failCause(queue, exit.cause);
};

const normalizeSubscriptionBufferSize = (subscriptionBufferSize: number): number =>
  Number.isSafeInteger(subscriptionBufferSize) && subscriptionBufferSize > 0
    ? subscriptionBufferSize
    : 1;

export type RemoteSubscriptionLifecycle = {
  readonly onOpen: Effect.Effect<void>;
  readonly onClose: Effect.Effect<void, unknown>;
};

export type RemoteSubscriptionOptions<
  Row,
  Error,
  Topic extends string = string,
  Key extends string = string,
> = {
  readonly clientScope: Scope.Scope;
  readonly overflowStatus: (topic: Topic, queuedEvents: number) => ViewServerStatusEvent<Topic>;
  readonly failureStatus: (topic: Topic, error: Error) => ViewServerStatusEvent<Topic>;
  readonly lifecycle?: RemoteSubscriptionLifecycle;
  readonly source: Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>, Error>;
  readonly subscriptionBufferSize: number;
  readonly topic: Topic;
};

const closeQueueForOverflow = Effect.fn("ViewServerClient.remote.subscription.closeForOverflow")(
  function* <Row, Topic extends string, Key extends string>(
    queue: Queue.Queue<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>,
    event: ViewServerStatusEvent<Topic>,
  ) {
    yield* Queue.clear(queue);
    yield* Queue.offer(queue, event);
    yield* Queue.end(queue);
  },
);

const offerRemoteEvent = Effect.fn("ViewServerClient.remote.subscription.offer")(function* <
  Row,
  Topic extends string,
  Key extends string,
>(
  queue: Queue.Queue<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>,
  event: ViewServerLiveEvent<Row, Topic, Key>,
  overflowStatus: (topic: Topic, queuedEvents: number) => ViewServerStatusEvent<Topic>,
  closeLifecycle: Effect.Effect<void>,
  topic: Topic,
) {
  const offered = yield* Queue.offer(queue, event);
  if (!offered) {
    const queuedEvents = yield* Queue.size(queue);
    yield* Queue.clear(queue);
    yield* closeLifecycle;
    yield* closeQueueForOverflow(queue, overflowStatus(topic, queuedEvents));
    return yield* Effect.interrupt;
  }
});

export const makeRemoteSubscription = Effect.fn("ViewServerClient.remote.subscription.make")(
  function* <Row, Error, Topic extends string = string, Key extends string = string>({
    clientScope,
    failureStatus,
    lifecycle = {
      onOpen: Effect.void,
      onClose: Effect.void,
    },
    overflowStatus,
    source,
    subscriptionBufferSize,
    topic,
  }: RemoteSubscriptionOptions<Row, Error, Topic, Key>) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const scope = yield* Scope.fork(clientScope, "parallel");
        const closeSubscription = Scope.close(scope, Exit.void).pipe(
          ignoreRemoteSubscriptionCloseFailure,
        );
        return yield* restore(
          Effect.gen(function* () {
            const lifecycleClosed = yield* Ref.make(false);
            const closeLifecycle = Effect.uninterruptible(
              Effect.gen(function* () {
                const shouldClose = yield* Ref.modify(lifecycleClosed, (closed) => [!closed, true]);
                if (shouldClose) {
                  yield* lifecycle.onClose.pipe(ignoreRemoteSubscriptionCloseFailure);
                }
              }),
            );
            const stream = source.pipe(
              Stream.catch((error) => Stream.make(failureStatus(topic, error))),
            );
            const queue = yield* Queue.dropping<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>(
              normalizeSubscriptionBufferSize(subscriptionBufferSize),
            );
            yield* Scope.addFinalizer(scope, closeLifecycle);
            yield* lifecycle.onOpen;
            yield* Stream.runForEach(stream, (event) =>
              offerRemoteEvent(queue, event, overflowStatus, closeLifecycle, topic),
            ).pipe(
              Effect.onExit((exit) => completeQueueFromProducerExit(queue, exit)),
              Effect.forkIn(scope, { startImmediately: true }),
              ignoreRemoteSubscriptionStreamStartFailure,
            );
            const subscription = {
              events: Stream.fromQueue(queue).pipe(Stream.ensuring(closeSubscription)),
              close: () => closeSubscription,
            } satisfies ViewServerLiveSubscription<Row, Topic, Key>;
            return subscription;
          }),
        ).pipe(Effect.onInterrupt(constant(closeSubscription)));
      }),
    );
  },
);
