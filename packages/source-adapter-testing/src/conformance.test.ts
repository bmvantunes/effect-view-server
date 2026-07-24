import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Layer } from "effect";
import {
  SourceAdapterConformanceSubject,
  conformanceCallbackBuffer,
  conformanceLeasedSession,
  conformanceMaterializedSession,
  registerSourceAdapterConformance,
  type SourceAdapterConformanceCallbackBufferSnapshot,
} from "./conformance";

registerSourceAdapterConformance({
  name: "Source Adapter callback-buffer conformance contract",
  layer: Layer.succeed(SourceAdapterConformanceSubject, {
    exerciseCallbackBuffer: Effect.succeed({
      capacity: 2,
      backpressurableBlockedAtCapacity: true,
      backpressurableDeliveryOrder: ["first", "second"],
      backpressurableHighWaterMark: 2,
      nonPausableFailure: "SourceBufferOverflow",
      nonPausableOverflowCount: 1n,
      nonPausableHighWaterMark: 2,
      registrationCount: 2n,
      finalizationCount: 2n,
    } satisfies SourceAdapterConformanceCallbackBufferSnapshot),
  }),
  callbackBuffer: true,
});

describe("Source Adapter conformance driver requirements", () => {
  it.effect("fails explicitly when a requested driver is missing", () =>
    Effect.gen(function* () {
      const empty = Layer.succeed(SourceAdapterConformanceSubject, {});
      const materialized = yield* Effect.scoped(
        conformanceMaterializedSession().pipe(Effect.provide(empty), Effect.exit),
      );
      const leased = yield* Effect.scoped(
        conformanceLeasedSession().pipe(Effect.provide(empty), Effect.exit),
      );
      const callbackBuffer = yield* Effect.scoped(
        conformanceCallbackBuffer().pipe(Effect.provide(empty), Effect.exit),
      );

      expect([
        Exit.isFailure(materialized),
        Exit.isFailure(leased),
        Exit.isFailure(callbackBuffer),
      ]).toStrictEqual([true, true, true]);
    }),
  );
});
