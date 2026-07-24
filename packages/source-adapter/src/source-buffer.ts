import { Cause, Effect, Queue, Scope, Stream } from "effect";
import type { SourceBufferMetrics, SourceExecutionFailure } from "./model";

export type BackpressurableSourceBuffer<Value> = {
  readonly emit: (value: Value) => Effect.Effect<void>;
  readonly stream: Stream.Stream<Value, SourceExecutionFailure<never>>;
  readonly metrics: Effect.Effect<SourceBufferMetrics>;
};

export type NonPausableSourceBuffer<Value> = {
  readonly emit: (value: Value) => void;
  readonly stream: Stream.Stream<Value, SourceExecutionFailure<never>>;
  readonly metrics: Effect.Effect<SourceBufferMetrics>;
};

type SourceBufferRegistration<Emitter, Error, Services> = (
  emitter: Emitter,
) => Effect.Effect<Effect.Effect<void>, Error, Services>;

type SourceBufferOptions<Emitter, Error, Services> = {
  readonly capacity: number;
  readonly register?: SourceBufferRegistration<Emitter, Error, Services>;
};

const validateCapacity = (capacity: number): number => {
  if (!Number.isSafeInteger(capacity) || capacity <= 0) {
    throw new TypeError("Source Buffer capacity must be a positive finite integer.");
  }
  return capacity;
};

const overflowFailure = (capacity: number): SourceExecutionFailure<never> => ({
  _tag: "RuntimeFailure",
  failure: {
    _tag: "SourceBufferOverflow",
    message: `Source Buffer exceeded its capacity of ${capacity}.`,
    capacity,
  },
});

const registerScoped = <Emitter, Error, Services>(
  emitter: Emitter,
  register: SourceBufferRegistration<Emitter, Error, Services> | undefined,
): Effect.Effect<void, Error, Services | Scope.Scope> =>
  register === undefined
    ? Effect.void
    : Effect.acquireRelease(register(emitter), (unregister) => unregister, {
        interruptible: true,
      }).pipe(Effect.asVoid);

const acquireBackpressurableSourceBuffer = Effect.fn("SourceBuffer.backpressurable.acquire")(
  function* <Value, Error, Services>(
    input: SourceBufferOptions<(value: Value) => Effect.Effect<void>, Error, Services>,
    capacity: number,
  ) {
    const queue = yield* Queue.bounded<Value, SourceExecutionFailure<never>>(capacity);
    yield* Effect.addFinalizer(() => Queue.shutdown(queue));
    let depth = 0;
    let highWaterMark = 0;
    const emit = Effect.fn("SourceBuffer.backpressurable.emit")(function* (value: Value) {
      const before = yield* Queue.size(queue);
      highWaterMark = Math.max(highWaterMark, Math.min(capacity, before + 1));
      yield* Queue.offer(queue, value);
      depth = yield* Queue.size(queue);
    });
    yield* registerScoped(emit, input.register);
    const stream = Stream.fromQueue(queue).pipe(
      Stream.tap(() =>
        Effect.sync(() => {
          depth = Queue.sizeUnsafe(queue);
        }),
      ),
    );
    return {
      emit,
      stream,
      metrics: Effect.sync(() => ({
        _tag: "Bounded",
        capacity,
        depth,
        highWaterMark,
        overflowCount: 0n,
      })),
    } satisfies BackpressurableSourceBuffer<Value>;
  },
);

export const makeBackpressurableSourceBuffer = <Value, Error = never, Services = never>(
  input: SourceBufferOptions<(value: Value) => Effect.Effect<void>, Error, Services>,
): Effect.Effect<BackpressurableSourceBuffer<Value>, Error, Services | Scope.Scope> =>
  acquireBackpressurableSourceBuffer(input, validateCapacity(input.capacity));

const acquireNonPausableSourceBuffer = Effect.fn("SourceBuffer.nonPausable.acquire")(function* <
  Value,
  Error,
  Services,
>(input: SourceBufferOptions<(value: Value) => void, Error, Services>, capacity: number) {
  const queue = yield* Queue.bounded<Value, SourceExecutionFailure<never>>(capacity);
  yield* Effect.addFinalizer(() => Queue.shutdown(queue));
  let depth = 0;
  let highWaterMark = 0;
  let overflowCount = 0n;
  let overflowed = false;
  const emit = (value: Value): void => {
    if (overflowed) {
      return;
    }
    if (!Queue.offerUnsafe(queue, value)) {
      overflowed = true;
      overflowCount += 1n;
      Queue.failCauseUnsafe(queue, Cause.fail(overflowFailure(capacity)));
      return;
    }
    depth = Queue.sizeUnsafe(queue);
    highWaterMark = Math.max(highWaterMark, depth);
  };
  yield* registerScoped(emit, input.register);
  const stream = Stream.fromQueue(queue).pipe(
    Stream.tap(() =>
      Effect.sync(() => {
        depth = Queue.sizeUnsafe(queue);
      }),
    ),
  );
  return {
    emit,
    stream,
    metrics: Effect.sync(() => ({
      _tag: "Bounded",
      capacity,
      depth,
      highWaterMark,
      overflowCount,
    })),
  } satisfies NonPausableSourceBuffer<Value>;
});

export const makeNonPausableSourceBuffer = <Value, Error = never, Services = never>(
  input: SourceBufferOptions<(value: Value) => void, Error, Services>,
): Effect.Effect<NonPausableSourceBuffer<Value>, Error, Services | Scope.Scope> =>
  acquireNonPausableSourceBuffer(input, validateCapacity(input.capacity));

export const SourceBuffer = {
  backpressurable: makeBackpressurableSourceBuffer,
  nonPausable: makeNonPausableSourceBuffer,
} as const;
