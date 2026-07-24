import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Scope, Stream } from "effect";
import { SourceBuffer } from "./server";

describe("Source Buffer primitives", () => {
  it("validates finite positive capacity during pure construction", () => {
    for (const capacity of [
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      expect(() => SourceBuffer.backpressurable<number>({ capacity })).toThrow(
        "positive finite integer",
      );
      expect(() => SourceBuffer.nonPausable<number>({ capacity })).toThrow(
        "positive finite integer",
      );
    }
  });

  it.effect("suspends a backpressurable emitter at capacity and tracks depth", () =>
    Effect.gen(function* () {
      let registrations = 0;
      let unregistrations = 0;
      const values = yield* Effect.scoped(
        Effect.gen(function* () {
          const buffer = yield* SourceBuffer.backpressurable<number>({
            capacity: 2,
            register: () =>
              Effect.sync(() => {
                registrations += 1;
                return Effect.sync(() => {
                  unregistrations += 1;
                });
              }),
          });
          yield* buffer.emit(1);
          yield* buffer.emit(2);
          const blocked = yield* buffer.emit(3).pipe(Effect.forkChild({ startImmediately: true }));
          yield* Effect.yieldNow;
          expect(blocked.pollUnsafe()).toBeUndefined();
          expect(yield* buffer.metrics).toStrictEqual({
            _tag: "Bounded",
            capacity: 2,
            depth: 2,
            highWaterMark: 2,
            overflowCount: 0n,
          });

          const collection = yield* buffer.stream.pipe(
            Stream.take(3),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Fiber.join(blocked);
          const collected = yield* Fiber.join(collection);
          expect(yield* buffer.metrics).toStrictEqual({
            _tag: "Bounded",
            capacity: 2,
            depth: 0,
            highWaterMark: 2,
            overflowCount: 0n,
          });
          return collected;
        }),
      );

      expect(values).toStrictEqual([1, 2, 3]);
      expect(registrations).toBe(1);
      expect(unregistrations).toBe(1);
    }),
  );

  it.effect("supports scoped buffers without callback registration", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const backpressurable = yield* SourceBuffer.backpressurable<number>({
          capacity: 1,
        });
        const nonPausable = yield* SourceBuffer.nonPausable<number>({
          capacity: 1,
        });
        expect(yield* backpressurable.metrics).toStrictEqual({
          _tag: "Bounded",
          capacity: 1,
          depth: 0,
          highWaterMark: 0,
          overflowCount: 0n,
        });
        expect(yield* nonPausable.metrics).toStrictEqual({
          _tag: "Bounded",
          capacity: 1,
          depth: 0,
          highWaterMark: 0,
          overflowCount: 0n,
        });
      }),
    ),
  );

  it.effect("shuts down retained emitters and releases blocked producers after Scope closure", () =>
    Effect.gen(function* () {
      const finalizationOrder: Array<string> = [];
      const scope = yield* Scope.make("sequential");
      const backpressurable = yield* SourceBuffer.backpressurable<number>({
        capacity: 1,
        register: () =>
          Effect.succeed(
            Effect.sync(() => {
              finalizationOrder.push("unregister");
            }),
          ),
      }).pipe(Effect.provideService(Scope.Scope, scope));
      const nonPausable = yield* SourceBuffer.nonPausable<number>({
        capacity: 1,
      }).pipe(Effect.provideService(Scope.Scope, scope));
      yield* backpressurable.emit(1);
      const blocked = yield* backpressurable
        .emit(2)
        .pipe(Effect.forkDetach({ startImmediately: true }));
      yield* Effect.yieldNow;
      expect(blocked.pollUnsafe()).toBeUndefined();

      yield* Scope.close(scope, Exit.void);
      yield* Fiber.await(blocked);
      nonPausable.emit(1);

      expect(blocked.pollUnsafe()).toBeDefined();
      expect(finalizationOrder).toStrictEqual(["unregister"]);
    }),
  );

  it.effect("fails a non-pausable Stream exactly once on overflow", () =>
    Effect.gen(function* () {
      let unregister: (() => void) | undefined;
      const observed: Array<number> = [];
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const buffer = yield* SourceBuffer.nonPausable<number>({
            capacity: 2,
            register: (emit) =>
              Effect.sync(() => {
                emit(1);
                unregister = () => undefined;
                return Effect.sync(() => {
                  unregister = undefined;
                });
              }),
          });
          buffer.emit(2);
          buffer.emit(3);
          buffer.emit(4);
          const failure = yield* Effect.flip(
            buffer.stream.pipe(
              Stream.runForEach((value) =>
                Effect.sync(() => {
                  observed.push(value);
                }),
              ),
            ),
          );
          const metrics = yield* buffer.metrics;
          return { failure, metrics };
        }),
      );

      expect(observed).toStrictEqual([1, 2]);
      expect(result.failure).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "SourceBufferOverflow",
          message: "Source Buffer exceeded its capacity of 2.",
          capacity: 2,
        },
      });
      expect(result.metrics).toStrictEqual({
        _tag: "Bounded",
        capacity: 2,
        depth: 0,
        highWaterMark: 2,
        overflowCount: 1n,
      });
      expect(unregister).toBeUndefined();
    }),
  );
});
