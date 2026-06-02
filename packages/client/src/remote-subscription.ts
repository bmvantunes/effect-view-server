import { Cause, Effect, Exit, Queue, Scope, Stream } from "effect";
import { constant } from "effect/Function";
import type { StatusEvent } from "@view-server/config";
import type { ViewServerLiveEvent, ViewServerLiveSubscription } from "./live-client";

export type RemoteSubscriptionLifecycle = {
  readonly onOpen: Effect.Effect<void>;
  readonly onClose: Effect.Effect<void>;
};

export type RemoteSubscriptionOptions<Row, Error> = {
  readonly clientScope: Scope.Scope;
  readonly failureStatus: (topic: string, error: Error) => StatusEvent;
  readonly lifecycle?: RemoteSubscriptionLifecycle;
  readonly source: Stream.Stream<ViewServerLiveEvent<Row>, Error>;
  readonly subscriptionBufferSize: number;
  readonly topic: string;
};

export const makeRemoteSubscription = Effect.fn("ViewServerClient.remote.subscription.make")(
  function* <Row, Error>({
    clientScope,
    failureStatus,
    lifecycle = {
      onOpen: Effect.void,
      onClose: Effect.void,
    },
    source,
    subscriptionBufferSize,
    topic,
  }: RemoteSubscriptionOptions<Row, Error>) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const scope = yield* Scope.fork(clientScope, "parallel");
        const closeSubscription = Scope.close(scope, Exit.void).pipe(Effect.ignore);
        return yield* restore(
          Effect.gen(function* () {
            const stream = source.pipe(
              Stream.catch((error) => Stream.make(failureStatus(topic, error))),
            );
            const queue = yield* Queue.bounded<ViewServerLiveEvent<Row>, Cause.Done>(
              subscriptionBufferSize,
            );
            yield* Scope.addFinalizer(scope, lifecycle.onClose.pipe(Effect.ignore));
            yield* Stream.runIntoQueue(stream, queue).pipe(
              Effect.forkIn(scope, { startImmediately: true }),
              Effect.ignore,
            );
            yield* lifecycle.onOpen;
            const subscription = {
              events: Stream.fromQueue(queue).pipe(Stream.ensuring(closeSubscription)),
              close: () => closeSubscription,
            } satisfies ViewServerLiveSubscription<Row>;
            return subscription;
          }),
        ).pipe(Effect.onInterrupt(constant(closeSubscription)));
      }),
    );
  },
);
