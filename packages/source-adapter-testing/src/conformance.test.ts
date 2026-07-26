import { describe, expect, it } from "@effect/vitest";
import type { SourceToolkit } from "@effect-view-server/source-adapter";
import {
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceUpsert,
  markSourceToolkit,
} from "@effect-view-server/source-adapter/internal";
import { Context, Effect, Exit, Fiber, Layer, Option, Scope, Stream } from "effect";
import {
  SourceAdapterConformanceRow,
  SourceFixture,
  isSourceAdapterConformanceDriverValue,
  makeSourceAdapterConformanceDriver,
  sourceAdapterConformanceDefinitionIsLinked,
  type SourceFixtureFailure,
  type SourceFixtureRejectionLocation,
} from "./index";

const toolkit: SourceToolkit<
  SourceAdapterConformanceRow,
  SourceFixtureFailure,
  SourceFixtureRejectionLocation
> = markSourceToolkit({
  topic: "rows",
  upsert: (row) => Effect.succeed(makeSourceUpsert<SourceAdapterConformanceRow>(row)),
  decodeUpsert: (row: unknown) =>
    Effect.succeed(
      makeSourceUpsert({
        id: String(row),
        region: "decoded",
        value: "decoded",
      }),
    ),
  delete: (id) => Effect.succeed(makeSourceDelete(id)),
  delivery: (mutations, settlement) => Effect.succeed(makeSourceDelivery(mutations, settlement)),
  reject: (input) => Effect.succeed(makeSourceItemRejection(input)),
});

const target = {
  _tag: "Materialized",
  lane: "primary",
} as const;

describe("Source Adapter low-level conformance driver", () => {
  it.effect("builds a real aggregate runtime context and keeps raw controls transport-only", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(SourceAdapterConformanceRow);
        const driver = SourceFixture.conformanceDriver(fixture);
        const callbackBridge = Option.getOrThrow(Option.fromUndefinedOr(driver.callbackBridge));
        const context = yield* driver.runtimeContext;
        const service = Option.getOrThrow(
          Context.getOption(context, fixture.adapter.runtimeService),
        );
        const callbackLifecycle = Option.getOrThrow(Option.fromUndefinedOr(service.materialized));
        expect(
          yield* callbackLifecycle.metrics({
            topic: "rows",
            definition: fixture.callbackBridge.source.options,
            target: { _tag: "Materialized" },
          }),
        ).toStrictEqual({ observed: 0n });
        yield* fixture.controls.setMetrics({
          observed: 1n,
          details: {
            samples: [],
            payload: null,
          },
        });
        expect(
          yield* callbackLifecycle.metrics({
            topic: "rows",
            definition: fixture.callbackBridge.source.options,
            target: { _tag: "Materialized" },
          }),
        ).toStrictEqual({
          observed: 1n,
          details: {
            samples: [],
            payload: null,
          },
        });
        expect(driver.materialized?.source.lifecycle).toBe("materialized");
        expect(driver.leased?.source.lifecycle).toBe("leased");
        expect(driver.callbackBridge?.source.lifecycle).toBe("materialized");
        const materializedExpectations = Option.getOrThrow(
          Option.fromUndefinedOr(driver.expectations.materialized),
        );
        const leasedExpectations = Option.getOrThrow(
          Option.fromUndefinedOr(driver.expectations.leased),
        );
        expect(materializedExpectations.acquisitionFailure).toStrictEqual(
          SourceFixture.failure("conformance acquisition failure", "acquire"),
        );
        expect(materializedExpectations.streamFailure).toStrictEqual(
          SourceFixture.failure("conformance lane failure", "stream"),
        );
        expect(materializedExpectations.settlementFailure).toStrictEqual(
          SourceFixture.failure("conformance settlement failure", "settlement"),
        );
        expect(materializedExpectations.updatedMetrics).toStrictEqual({ observed: 42n });
        expect({
          materializedFailure: materializedExpectations.rejectionFailure("stream"),
          materializedLocation: materializedExpectations.rejectionLocation(target, 1n),
          materializedRowId: materializedExpectations.rowId(target, "row"),
          leasedFailure: leasedExpectations.rejectionFailure("settlement"),
          leasedLocation: leasedExpectations.rejectionLocation(
            {
              _tag: "Leased",
              route: { region: "eu" },
              lane: "sibling",
            },
            2n,
          ),
          leasedRowId: leasedExpectations.rowId(
            {
              _tag: "Leased",
              route: { region: "eu" },
              lane: "sibling",
            },
            "row",
          ),
        }).toStrictEqual({
          materializedFailure: SourceFixture.failure("conformance rejection", "stream"),
          materializedLocation: { lane: "primary", offset: 1n },
          materializedRowId: "row",
          leasedFailure: SourceFixture.failure("conformance rejection", "settlement"),
          leasedLocation: { lane: "sibling", offset: 2n },
          leasedRowId: "row",
        });
        expect(yield* driver.transport.observe(target)).toStrictEqual({
          acquisitions: 0n,
          finalizations: 0n,
          partialAcquisitionFinalizations: 0n,
          registrations: 0n,
          callbackFinalizations: 0n,
          finalizerStarted: false,
        });
        expect(
          yield* driver.transport.observe({
            _tag: "Leased",
            route: { region: "same" },
            lane: "primary",
          }),
        ).toStrictEqual({
          acquisitions: 0n,
          finalizations: 0n,
          partialAcquisitionFinalizations: 0n,
          registrations: 0n,
          callbackFinalizations: 0n,
          finalizerStarted: false,
        });
        yield* driver.transport.changes(target).pipe(Stream.take(1), Stream.runDrain);
        yield* fixture.controls.releaseFinalizer({
          _tag: "Materialized",
          lane: "not-blocked",
        });
        expect(
          Exit.isFailure(
            yield* fixture.controls.delete(target, "inactive-delete").pipe(Effect.exit),
          ),
        ).toBe(true);

        const inactiveCommands = yield* Effect.forEach(
          [
            {
              _tag: "Delivery",
              target,
              mutations: [
                {
                  _tag: "Upsert",
                  row: { id: "a", region: "eu", value: "a" },
                },
              ],
            },
            {
              _tag: "CorruptLaterMutation",
              target,
              firstRow: { id: "a", region: "eu", value: "a" },
              laterRow: { id: "b", region: "eu", value: "b" },
              field: "region",
              value: "other",
              settle: () => Effect.void,
            },
            {
              _tag: "Reject",
              target,
              phase: "stream",
              offset: 1n,
            },
            {
              _tag: "FailLane",
              target,
              phase: "stream",
            },
            {
              _tag: "CompleteLane",
              target,
            },
          ] as const,
          (command) => driver.transport.command(command).pipe(Effect.exit),
        );
        expect(inactiveCommands.map(Exit.isFailure)).toStrictEqual([true, true, true, true, true]);
        yield* driver.transport.command({
          _tag: "FailNextAcquisition",
          target,
          phase: "acquire",
          afterFirstResource: false,
        });
        yield* driver.transport.command({
          _tag: "FailNextAcquisition",
          target,
          phase: "acquire",
          afterFirstResource: true,
        });
        yield* driver.transport.command({
          _tag: "ConfigureNextAttempt",
          target,
          fault: "EmptyLanes",
        });
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "updated" });
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "invalid" });
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "reset" });
        yield* driver.transport.command({ _tag: "BlockNextFinalizer", target });
        yield* driver.transport.command({ _tag: "ReleaseFinalizer", target });
        yield* driver.transport.command({ _tag: "ReleaseFinalizer", target });
        const unknownCommand: unknown = Reflect.apply(driver.transport.command, undefined, [
          { _tag: "Unknown" },
        ]);
        if (!Effect.isEffect(unknownCommand)) {
          return yield* Effect.die("Expected an Effect from the raw command boundary.");
        }
        yield* unknownCommand.pipe(Effect.provideContext(Context.makeUnsafe<unknown>(new Map())));
        expect(
          Exit.isFailure(
            yield* callbackBridge
              .emitBackpressurable({
                id: "inactive",
                region: "eu",
                value: "inactive",
              })
              .pipe(Effect.exit),
          ),
        ).toBe(true);
        expect(
          Exit.isFailure(
            yield* callbackBridge
              .offerNonPausable({
                id: "inactive",
                region: "eu",
                value: "inactive",
              })
              .pipe(Effect.exit),
          ),
        ).toBe(true);
        expect(
          Exit.isFailure(
            yield* callbackBridge
              .offerBackpressurable({
                id: "inactive",
                region: "eu",
                value: "inactive",
              })
              .pipe(Effect.exit),
          ),
        ).toBe(true);
        expect(
          Exit.isFailure(
            yield* callbackBridge
              .emitNonPausable({
                id: "inactive",
                region: "eu",
                value: "inactive",
              })
              .pipe(Effect.exit),
          ),
        ).toBe(true);
      }),
    ),
  );

  it("rejects non-nominal layers, definitions, lifecycles, and empty leased routes", () => {
    const fixture = Effect.runSync(SourceFixture.make(SourceAdapterConformanceRow));
    const valid = SourceFixture.conformanceDriver(fixture);
    expect(isSourceAdapterConformanceDriverValue(valid)).toBe(true);
    expect(
      sourceAdapterConformanceDefinitionIsLinked(
        valid.materialized?.source,
        valid.adapter,
        "materialized",
      ),
    ).toBe(true);
    expect(
      sourceAdapterConformanceDefinitionIsLinked(
        valid.materialized?.source,
        valid.adapter,
        "leased",
      ),
    ).toBe(false);
    expect(
      sourceAdapterConformanceDefinitionIsLinked(valid.materialized?.source, {}, "materialized"),
    ).toBe(false);
    expect(isSourceAdapterConformanceDriverValue(null)).toBe(false);
    expect(isSourceAdapterConformanceDriverValue("driver")).toBe(false);
    expect(isSourceAdapterConformanceDriverValue({})).toBe(false);
    const driverBrand = Option.getOrThrow(
      Option.fromUndefinedOr(Object.getOwnPropertySymbols(valid)[0]),
    );
    const mismatchedBrand = { ...valid };
    Object.defineProperty(mismatchedBrand, driverBrand, {
      value: () => ({}),
    });
    expect(isSourceAdapterConformanceDriverValue(mismatchedBrand)).toBe(false);
    const base = {
      adapter: valid.adapter,
      expectations: valid.expectations,
      transport: valid.transport,
    };
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: {},
          materialized: valid.materialized,
          ...base,
        },
      ]),
    ).toThrow("nominal aggregate runtime Layer");
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: Layer.empty,
          adapter: {},
          expectations: valid.expectations,
          transport: valid.transport,
          materialized: valid.materialized,
        },
      ]),
    ).toThrow("nominal adapter handle");
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: Layer.empty,
          ...base,
        },
      ]),
    ).toThrow("at least one lifecycle");
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: Layer.empty,
          ...base,
          materialized: {
            ...valid.materialized,
            source: {},
          },
        },
      ]),
    ).toThrow("nominal materialized Definition");
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: Layer.empty,
          ...base,
          expectations: {
            materialized: undefined,
            leased: valid.expectations.leased,
          },
          materialized: valid.materialized,
          leased: valid.leased,
        },
      ]),
    ).toThrow("exact materialized expectations");
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: Layer.empty,
          ...base,
          expectations: {
            materialized: valid.expectations.materialized,
            leased: valid.expectations.leased,
          },
          leased: valid.leased,
        },
      ]),
    ).toThrow("exact materialized expectations");
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: Layer.empty,
          ...base,
          leased: {
            source: valid.materialized?.source,
            sameRoute: { region: "same" },
            distinctRoute: { region: "distinct" },
          },
        },
      ]),
    ).toThrow("nominal leased Definition");
    expect(() =>
      Reflect.apply(makeSourceAdapterConformanceDriver, undefined, [
        {
          runtimeLayer: Layer.empty,
          adapter: base.adapter,
          transport: base.transport,
          expectations: {
            materialized: undefined,
            leased: valid.expectations.leased,
          },
          leased: {
            source: valid.leased?.source,
            delayedRetrySource: valid.leased?.delayedRetrySource,
            singleRetrySource: valid.leased?.singleRetrySource,
            sameRoute: {},
            distinctRoute: {},
          },
        },
      ]),
    ).toThrow("two non-empty routes");
  });

  it.effect("drives fixture attempts, faults, metrics, and finalization without Runtime Core", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(SourceAdapterConformanceRow);
      const driver = SourceFixture.conformanceDriver(fixture);
      const context = yield* Effect.scoped(Layer.build(fixture.layer));
      const service = Context.get(context, fixture.adapter.runtimeService);
      const lifecycle = Option.getOrThrow(Option.fromUndefinedOr(service.materialized));
      const definition = fixture.materializedSource({
        label: "local-canary",
        lanes: ["primary", "sibling"],
      });
      const attemptScope = yield* Scope.make();
      const attempt = yield* lifecycle
        .acquire({
          definition: definition.options,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, attemptScope));
      yield* fixture.controls.awaitActive(target);
      const eventsFiber = yield* attempt.lanes[0].events.pipe(
        Stream.take(7),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* fixture.controls.upsert(
        target,
        {
          id: "upsert",
          region: "eu",
          value: "upsert",
        },
        () => Effect.void,
      );
      yield* fixture.controls.delete(target, "upsert", () => Effect.void);
      yield* fixture.controls.delivery(
        target,
        [
          {
            _tag: "Upsert",
            row: { id: "delivery", region: "eu", value: "delivery" },
          },
          {
            _tag: "Delete",
            id: "delivery",
          },
        ],
        () => Effect.void,
      );
      yield* fixture.controls.corruptAfterDecode(
        target,
        { id: "corrupt", region: "eu", value: "corrupt" },
        "value",
        "changed",
        () => Effect.void,
      );
      yield* driver.transport.command({
        _tag: "CorruptLaterMutation",
        target,
        firstRow: { id: "first", region: "eu", value: "first" },
        laterRow: { id: "later", region: "eu", value: "later" },
        field: "value",
        value: "changed",
        settle: () => Effect.fail("expected settlement failure"),
      });
      yield* driver.transport.command({
        _tag: "Reject",
        target,
        phase: "stream",
        offset: 1n,
        settle: () => Effect.fail("expected rejection settlement failure"),
      });
      yield* fixture.controls.reject(target, SourceFixture.failure("unsettled", "stream"), {
        lane: "primary",
        offset: 2n,
      });
      const events = yield* Fiber.join(eventsFiber);
      expect(events.map((event) => event._tag)).toStrictEqual([
        "SourceDelivery",
        "SourceDelivery",
        "SourceDelivery",
        "SourceDelivery",
        "SourceDelivery",
        "SourceItemRejection",
        "SourceItemRejection",
      ]);
      const settlements = yield* Effect.forEach(events, (event) =>
        event.settle(Exit.void).pipe(Effect.exit),
      );
      expect(settlements.map(Exit.isFailure)).toStrictEqual([
        false,
        false,
        false,
        false,
        true,
        true,
        false,
      ]);
      expect(
        Exit.isFailure(
          yield* fixture.controls
            .upsert(
              { _tag: "Materialized", lane: "missing" },
              { id: "missing", region: "eu", value: "missing" },
            )
            .pipe(Effect.exit),
        ),
      ).toBe(true);
      expect(
        yield* lifecycle.metrics({
          topic: "rows",
          definition: definition.options,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual({ observed: 0n });
      yield* fixture.controls.setMetrics({
        observed: 3n,
        details: {
          samples: [],
          payload: null,
        },
      });
      expect(
        yield* lifecycle.metrics({
          topic: "rows",
          definition: fixture.callbackBridge.source.options,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual({
        observed: 3n,
        details: {
          samples: [],
          payload: null,
        },
      });
      yield* fixture.controls.setMetrics({
        observed: 2n,
        details: {
          samples: [1, "two"],
          payload: { ok: true },
        },
      });
      expect(
        yield* lifecycle.metrics({
          topic: "rows",
          definition: definition.options,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual({
        observed: 2n,
        details: {
          samples: [1, "two"],
          payload: { ok: true },
        },
      });
      expect(fixture.controls.metricReads()).toBe(3n);
      yield* fixture.controls.blockNextFinalizer(target);
      const closeFiber = yield* Scope.close(attemptScope, Exit.void).pipe(Effect.forkChild);
      yield* fixture.controls.changes(target).pipe(
        Stream.filter((observation) => observation.finalizerStarted),
        Stream.take(1),
        Stream.runDrain,
      );
      expect(fixture.controls.finalizerStarted(target)).toBe(true);
      yield* fixture.controls.releaseFinalizer(target);
      yield* Fiber.join(closeFiber);
      yield* fixture.controls.awaitCounts(target, {
        acquisitions: 1n,
        finalizations: 1n,
      });
      expect(fixture.controls.counts(target)).toStrictEqual({
        acquisitions: 1n,
        finalizations: 1n,
      });

      yield* fixture.controls.failNextAcquisition(target, SourceFixture.failure("next", "acquire"));
      expect(
        Exit.isFailure(
          yield* Effect.scoped(
            lifecycle.acquire({
              definition: definition.options,
              target: { _tag: "Materialized" },
              toolkit,
            }),
          ).pipe(Effect.exit),
        ),
      ).toBe(true);
      yield* fixture.controls.failNextPartialAcquisition(
        target,
        SourceFixture.failure("partial", "acquire"),
      );
      expect(
        Exit.isFailure(
          yield* Effect.scoped(
            lifecycle.acquire({
              definition: definition.options,
              target: { _tag: "Materialized" },
              toolkit,
            }),
          ).pipe(Effect.exit),
        ),
      ).toBe(true);
      expect(fixture.controls.partialAcquisitionFinalizations(target)).toBe(1n);

      const failingScope = yield* Scope.make();
      const failingAttempt = yield* lifecycle
        .acquire({
          definition: definition.options,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, failingScope));
      const failedStream = yield* failingAttempt.lanes[0].events.pipe(
        Stream.runDrain,
        Effect.exit,
        Effect.forkChild,
      );
      yield* fixture.controls.fail(target, SourceFixture.failure("stream failed", "stream"));
      expect(Exit.isFailure(yield* Fiber.join(failedStream))).toBe(true);
      yield* Scope.close(failingScope, Exit.void);

      const invalidScope = yield* Scope.make();
      const invalidAttempt = yield* lifecycle
        .acquire({
          definition: definition.options,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, invalidScope));
      const invalidEvent = yield* invalidAttempt.lanes[0].events.pipe(
        Stream.take(1),
        Stream.runHead,
        Effect.exit,
        Effect.forkChild,
      );
      yield* fixture.controls.upsert(target, {
        id: 1,
        region: "invalid",
        value: "invalid",
      });
      expect(Exit.isFailure(yield* Fiber.join(invalidEvent))).toBe(true);
      yield* Scope.close(invalidScope, Exit.void);

      const completeScope = yield* Scope.make();
      const completeAttempt = yield* lifecycle
        .acquire({
          definition: definition.options,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, completeScope));
      const completed = yield* completeAttempt.lanes[0].events.pipe(
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* driver.transport.command({ _tag: "CompleteLane", target });
      yield* Fiber.join(completed);
      yield* Scope.close(completeScope, Exit.void);

      for (const fault of ["EmptyLanes", "EmptyLaneId", "DuplicateLaneId"] as const) {
        yield* fixture.controls.configureNextAttempt(target, fault);
        const faulted = yield* Effect.scoped(
          lifecycle.acquire({
            definition: definition.options,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ).pipe(Effect.exit);
        expect(Exit.isFailure(faulted), fault).toBe(true);
      }

      for (const fault of ["ChangedLaneIds", "MissingBufferMetrics"] as const) {
        yield* fixture.controls.configureNextAttempt(target, fault);
        const faulted = yield* Effect.scoped(
          lifecycle.acquire({
            definition: definition.options,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ).pipe(Effect.exit);
        expect(Exit.isSuccess(faulted), fault).toBe(true);
      }

      const singleLaneDefinition = fixture.materializedSource({
        label: "single-lane-fault",
        lanes: ["primary"],
      });
      yield* fixture.controls.configureNextAttempt(target, "DuplicateLaneId");
      expect(
        Exit.isFailure(
          yield* Effect.scoped(
            lifecycle.acquire({
              definition: singleLaneDefinition.options,
              target: { _tag: "Materialized" },
              toolkit,
            }),
          ).pipe(Effect.exit),
        ),
      ).toBe(true);

      const defaultDefinition = fixture.materializedSource();
      const defaultScope = yield* Scope.make();
      const defaultAttempt = yield* lifecycle
        .acquire({
          definition: defaultDefinition.options,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, defaultScope));
      const defaultEvent = yield* defaultAttempt.lanes[0].events.pipe(
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* fixture.controls.upsert(
        { _tag: "Materialized" },
        { id: "default", region: "eu", value: "default" },
      );
      yield* Option.getOrThrow(yield* Fiber.join(defaultEvent)).settle(Exit.void);
      yield* Scope.close(defaultScope, Exit.void);

      const leased = Option.getOrThrow(Option.fromUndefinedOr(service.leased));
      const leasedDefinition = fixture.leasedSource(["region"]);
      const leasedScope = yield* Scope.make();
      const leasedAttempt = yield* leased
        .acquire({
          definition: leasedDefinition.options,
          target: { _tag: "Leased", route: { region: "eu" } },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, leasedScope));
      const leasedEvent = yield* leasedAttempt.lanes[0].events.pipe(
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* fixture.controls.delivery(
        {
          _tag: "Leased",
          route: { region: "eu" },
          lane: "fixture",
        },
        [
          {
            _tag: "Upsert",
            row: { id: "leased", region: "eu", value: "leased" },
          },
        ],
      );
      yield* Option.getOrThrow(yield* Fiber.join(leasedEvent)).settle(Exit.void);
      yield* Scope.close(leasedScope, Exit.void);
      expect(fixture.materializedSource().lifecycle).toBe("materialized");
    }),
  );

  it.effect("uses the fixture's real callback adapter and registration finalizers", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(SourceAdapterConformanceRow);
      const context = yield* Effect.scoped(Layer.build(fixture.callbackBridge.layer));
      const service = Context.get(context, fixture.callbackBridge.source.adapter.runtimeService);
      const lifecycle = Option.getOrThrow(Option.fromUndefinedOr(service.materialized));
      const scope = yield* Scope.make();
      const attempt = yield* lifecycle
        .acquire({
          definition: fixture.callbackBridge.source.options,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, scope));
      const backpressurableEvent = yield* attempt.lanes[0].events.pipe(
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      const backpressurableEmit = yield* fixture.callbackBridge
        .emitBackpressurable({
          id: "backpressurable",
          region: "eu",
          value: "callback",
        })
        .pipe(Effect.forkChild);
      const first = Option.getOrThrow(yield* Fiber.join(backpressurableEvent));
      yield* first.settle(Exit.void);
      yield* Fiber.join(backpressurableEmit);

      const nonPausableLane = Option.getOrThrow(Option.fromUndefinedOr(attempt.lanes[1]));
      const nonPausableEvent = yield* nonPausableLane.events.pipe(
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      const nonPausableEmit = yield* fixture.callbackBridge
        .emitNonPausable({
          id: "non-pausable",
          region: "eu",
          value: "callback",
        })
        .pipe(Effect.forkChild);
      const second = Option.getOrThrow(yield* Fiber.join(nonPausableEvent));
      yield* second.settle(Exit.void);
      yield* Fiber.join(nonPausableEmit);
      expect(
        yield* lifecycle.metrics({
          topic: "rows",
          definition: fixture.callbackBridge.source.options,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual({ observed: 0n });
      yield* fixture.controls.setMetrics({
        observed: 4n,
        details: {
          samples: [],
          payload: null,
        },
      });
      expect(
        yield* lifecycle.metrics({
          topic: "rows",
          definition: fixture.callbackBridge.source.options,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual({
        observed: 4n,
        details: {
          samples: [],
          payload: null,
        },
      });
      expect(fixture.callbackBridge.registrations()).toBe(2n);
      yield* Scope.close(scope, Exit.void);
      expect(fixture.callbackBridge.finalizations()).toBe(2n);
      yield* fixture.callbackBridge.releaseConsumer;

      yield* fixture.callbackBridge.pauseNextConsumer;
      const pausedScope = yield* Scope.make();
      const pausedAttempt = yield* lifecycle
        .acquire({
          definition: fixture.callbackBridge.source.options,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, pausedScope));
      yield* fixture.callbackBridge.offerBackpressurable({
        id: "queued-first",
        region: "eu",
        value: "callback",
      });
      const blocked = yield* fixture.callbackBridge
        .offerBackpressurable({
          id: "queued-second",
          region: "eu",
          value: "callback",
        })
        .pipe(Effect.forkChild);
      yield* fixture.callbackBridge.offerNonPausable({
        id: "non-pausable-first",
        region: "eu",
        value: "callback",
      });
      yield* fixture.callbackBridge.offerNonPausable({
        id: "non-pausable-overflow",
        region: "eu",
        value: "callback",
      });
      const queued = yield* pausedAttempt.lanes[0].events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const overflow = yield* Option.getOrThrow(
        Option.fromUndefinedOr(pausedAttempt.lanes[1]),
      ).events.pipe(Stream.runDrain, Effect.exit, Effect.forkChild);
      yield* fixture.callbackBridge.releaseConsumer;
      yield* Fiber.join(blocked);
      const queuedEvents = yield* Fiber.join(queued);
      yield* Effect.forEach(queuedEvents, (event) => event.settle(Exit.void), {
        discard: true,
      });
      expect(Exit.isFailure(yield* Fiber.join(overflow))).toBe(true);
      yield* Scope.close(pausedScope, Exit.void);
      expect(fixture.callbackBridge.finalizations()).toBe(4n);
    }),
  );
});
