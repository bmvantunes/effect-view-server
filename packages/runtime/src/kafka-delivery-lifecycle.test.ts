import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber } from "effect";
import { makeScopedKafkaDelivery } from "./kafka-delivery";
import { ViewServerKafkaIngressError } from "./kafka-ingress-error";

describe("Kafka delivery lifecycle", () => {
  it.effect("makes every close caller join the one scoped delivery shutdown", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const workerStarted = yield* Deferred.make<void>();
      const releaseStarted = yield* Deferred.make<void>();
      const allowRelease = yield* Deferred.make<void>();
      const firstCloseCompleted = yield* Deferred.make<void>();
      const secondCloseCompleted = yield* Deferred.make<void>();

      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.acquireRelease(Effect.void, () =>
            Deferred.succeed(releaseStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowRelease)),
              Effect.andThen(
                Effect.sync(() => {
                  releaseCount += 1;
                }),
              ),
            ),
          ),
          () => Deferred.succeed(workerStarted, undefined).pipe(Effect.andThen(Effect.never)),
        ),
      );

      yield* Deferred.await(workerStarted);
      const firstClose = yield* delivery.close.pipe(
        Effect.ensuring(Deferred.succeed(firstCloseCompleted, undefined)),
        Effect.forkChild({ startImmediately: true }),
      );
      const secondClose = yield* delivery.close.pipe(
        Effect.ensuring(Deferred.succeed(secondCloseCompleted, undefined)),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(releaseStarted);

      expect(yield* Deferred.isDone(firstCloseCompleted)).toBe(false);
      expect(yield* Deferred.isDone(secondCloseCompleted)).toBe(false);

      yield* Deferred.succeed(allowRelease, undefined);
      yield* Fiber.join(firstClose);
      yield* Fiber.join(secondClose);

      expect(releaseCount).toBe(1);
    }),
  );

  it.effect("joins worker-failure cleanup when runtime close races its blocked release", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const workerStarted = yield* Deferred.make<void>();
      const failWorker = yield* Deferred.make<void>();
      const releaseStarted = yield* Deferred.make<void>();
      const allowRelease = yield* Deferred.make<void>();
      const workerReleased = yield* Deferred.make<void>();
      const closeCompleted = yield* Deferred.make<void>();
      const failure = new ViewServerKafkaIngressError({
        message: "Kafka consumer failed",
        cause: "consumer-down",
        region: "local",
      });

      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.acquireRelease(Effect.void, () =>
            Deferred.succeed(releaseStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowRelease)),
              Effect.andThen(
                Effect.sync(() => {
                  releaseCount += 1;
                }),
              ),
              Effect.andThen(Deferred.succeed(workerReleased, undefined)),
            ),
          ),
          () =>
            Deferred.succeed(workerStarted, undefined).pipe(
              Effect.andThen(Deferred.await(failWorker)),
              Effect.andThen(Effect.fail(failure)),
            ),
        ),
      );

      yield* Deferred.await(workerStarted);
      yield* Deferred.succeed(failWorker, undefined);
      yield* Deferred.await(releaseStarted);
      const close = yield* delivery.close.pipe(
        Effect.ensuring(Deferred.succeed(closeCompleted, undefined)),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;

      expect(yield* Deferred.isDone(closeCompleted)).toBe(false);

      yield* Deferred.succeed(allowRelease, undefined);
      yield* Fiber.join(close);
      yield* Deferred.await(workerReleased);

      expect(releaseCount).toBe(1);
    }),
  );

  it.effect("rolls back earlier region deliveries when a later region cannot start", () =>
    Effect.gen(function* () {
      let firstReleaseCount = 0;
      let secondReleaseCount = 0;
      const firstReleased = yield* Deferred.make<void>();
      const secondReleased = yield* Deferred.make<void>();
      const startupFailure = new ViewServerKafkaIngressError({
        message: "Failed to start Kafka consumer for region cold",
        cause: "no-broker",
        region: "cold",
      });

      const exit = yield* Effect.exit(
        makeScopedKafkaDelivery((startWorker) =>
          Effect.gen(function* () {
            yield* startWorker(
              Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  firstReleaseCount += 1;
                }).pipe(Effect.andThen(Deferred.succeed(firstReleased, undefined))),
              ),
              () => Effect.never,
            );
            yield* startWorker(
              Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  secondReleaseCount += 1;
                }).pipe(Effect.andThen(Deferred.succeed(secondReleased, undefined))),
              ).pipe(Effect.andThen(Effect.fail(startupFailure))),
              () => Effect.never,
            );
          }),
        ),
      );

      expect({
        failed: Exit.isFailure(exit),
        firstReleaseCount,
        firstReleased: yield* Deferred.isDone(firstReleased),
        secondReleaseCount,
        secondReleased: yield* Deferred.isDone(secondReleased),
      }).toStrictEqual({
        failed: true,
        firstReleaseCount: 1,
        firstReleased: true,
        secondReleaseCount: 1,
        secondReleased: true,
      });
    }),
  );

  it.effect("starts every Region release while another Region release remains blocked", () =>
    Effect.gen(function* () {
      const blockedReleaseStarted = yield* Deferred.make<void>();
      const allowBlockedRelease = yield* Deferred.make<void>();
      const otherRegionReleased = yield* Deferred.make<void>();

      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        Effect.gen(function* () {
          yield* startWorker(
            Effect.acquireRelease(Effect.void, () =>
              Deferred.succeed(otherRegionReleased, undefined),
            ),
            () => Effect.never,
          );
          yield* startWorker(
            Effect.acquireRelease(Effect.void, () =>
              Deferred.succeed(blockedReleaseStarted, undefined).pipe(
                Effect.andThen(Deferred.await(allowBlockedRelease)),
              ),
            ),
            () => Effect.never,
          );
        }),
      );
      const close = yield* delivery.close.pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(blockedReleaseStarted);
      yield* Effect.yieldNow;
      const releasedWhileOtherRegionWasBlocked = yield* Deferred.isDone(otherRegionReleased);
      yield* Deferred.succeed(allowBlockedRelease, undefined);
      yield* Fiber.join(close);

      expect(releasedWhileOtherRegionWasBlocked).toBe(true);
    }),
  );

  it.effect("closes delivery resources to unblock an uninterruptible pending consumer read", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      let workerFinalizerCount = 0;
      const workerStarted = yield* Deferred.make<void>();
      const unblockConsumerRead = yield* Deferred.make<void>();

      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.acquireRelease(Effect.void, () =>
            Effect.sync(() => {
              releaseCount += 1;
            }).pipe(Effect.andThen(Deferred.succeed(unblockConsumerRead, undefined))),
          ),
          () =>
            Deferred.succeed(workerStarted, undefined).pipe(
              Effect.andThen(Deferred.await(unblockConsumerRead).pipe(Effect.uninterruptible)),
              Effect.ensuring(
                Effect.sync(() => {
                  workerFinalizerCount += 1;
                }),
              ),
            ),
        ),
      );

      yield* Deferred.await(workerStarted);
      yield* delivery.close.pipe(Effect.timeout("1 second"));

      expect({
        releaseCount,
        workerFinalizerCount,
      }).toStrictEqual({
        releaseCount: 1,
        workerFinalizerCount: 1,
      });
    }),
  );

  it.effect("clears Region health after in-flight observations and ordered resource shutdown", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const workerStarted = yield* Deferred.make<void>();
      const unblockObservation = yield* Deferred.make<void>();

      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.acquireRelease(Effect.void, () =>
            Effect.gen(function* () {
              operations.push("lag:stop");
              operations.push("listeners:close");
              operations.push("consumer:close");
              yield* Deferred.succeed(unblockObservation, undefined);
            }),
          ),
          () =>
            Deferred.succeed(workerStarted, undefined).pipe(
              Effect.andThen(
                Deferred.await(unblockObservation).pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      operations.push("observation:finished");
                    }),
                  ),
                  Effect.uninterruptible,
                ),
              ),
            ),
          Effect.sync(() => {
            operations.push("region:stopped");
          }),
        ),
      );

      yield* Deferred.await(workerStarted);
      yield* delivery.close;

      expect(operations).toStrictEqual([
        "lag:stop",
        "listeners:close",
        "consumer:close",
        "observation:finished",
        "region:stopped",
      ]);
    }),
  );

  it.effect("shares shutdown finalizer defects with delivery close callers", () =>
    Effect.gen(function* () {
      const workerStarted = yield* Deferred.make<void>();
      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.void,
          () => Deferred.succeed(workerStarted, undefined).pipe(Effect.andThen(Effect.never)),
          Effect.die(new Error("Region health shutdown defect")),
        ),
      );

      yield* Deferred.await(workerStarted);
      const closeExit = yield* Effect.exit(delivery.close);

      expect(Exit.isFailure(closeExit)).toBe(true);
    }),
  );

  it.effect("runs Region health shutdown after a worker resource finalizer defects", () =>
    Effect.gen(function* () {
      const workerStarted = yield* Deferred.make<void>();
      let regionStopped = 0;
      const delivery = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.acquireRelease(Effect.void, () =>
            Effect.die(new Error("consumer cleanup defect")),
          ),
          () => Deferred.succeed(workerStarted, undefined).pipe(Effect.andThen(Effect.never)),
          Effect.sync(() => {
            regionStopped += 1;
          }),
        ),
      );

      yield* Deferred.await(workerStarted);
      const closeExit = yield* Effect.exit(delivery.close);

      expect({
        closeFailed: Exit.isFailure(closeExit),
        regionStopped,
      }).toStrictEqual({
        closeFailed: true,
        regionStopped: 1,
      });
    }),
  );

  it.effect("releases an acquired Region when delivery startup is interrupted", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const acquisitionCompleted = yield* Deferred.make<void>();
      const resourceReleased = yield* Deferred.make<void>();
      const startup = yield* makeScopedKafkaDelivery((startWorker) =>
        startWorker(
          Effect.acquireRelease(Deferred.succeed(acquisitionCompleted, undefined), () =>
            Effect.sync(() => {
              releaseCount += 1;
            }).pipe(Effect.andThen(Deferred.succeed(resourceReleased, undefined))),
          ).pipe(Effect.andThen(Effect.never)),
          () => Effect.never,
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(acquisitionCompleted);
      yield* Fiber.interrupt(startup);
      const interrupted = yield* Fiber.await(startup);

      expect({
        interrupted: Exit.hasInterrupts(interrupted),
        releaseCount,
        resourceReleased: yield* Deferred.isDone(resourceReleased),
      }).toStrictEqual({
        interrupted: true,
        releaseCount: 1,
        resourceReleased: true,
      });
    }),
  );
});
