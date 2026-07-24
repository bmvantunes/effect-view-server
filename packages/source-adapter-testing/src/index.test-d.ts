import { describe, expectTypeOf, it } from "@effect/vitest";
import type {
  SourceDefinition,
  SourceDefinitionRouteFields,
} from "@effect-view-server/source-adapter";
import { Context, Effect, Layer, Schema } from "effect";
import {
  SourceAdapterConformanceDriver,
  SourceAdapterConformanceRow,
  SourceFixture,
  inspectSourceAdapterPackageConformance,
  makeSourceAdapterConformanceDriver,
  sourceAdapterConformanceDefinitionIsLinked,
  type ControllableSourceFixture,
  type SourceAdapterConformanceDriverInput,
  type SourceAdapterConformanceDriverValue,
  type SourceAdapterPackageInspectionOptions,
  type SourceFixtureFailure,
  type SourceFixtureLeasedDefinition,
  type SourceFixtureMaterializedDefinition,
} from "./index";

const FixtureRow = Schema.Struct({
  id: Schema.String,
  value: Schema.Number,
});

declare const fixture: ControllableSourceFixture<typeof FixtureRow.Type>;
declare const conformanceFixture: ControllableSourceFixture<SourceAdapterConformanceRow>;
declare const conformanceDriver: SourceAdapterConformanceDriverValue;
declare const unrelatedMaterializedDefinition: SourceDefinition<
  {
    readonly identity: {
      readonly name: "unrelated-adapter";
    };
  },
  "materialized",
  unknown,
  readonly [],
  unknown,
  SourceAdapterConformanceRow
>;
declare const unbrandedConformanceDriver: Pick<
  SourceAdapterConformanceDriverValue<typeof conformanceFixture.adapter>,
  | "adapter"
  | "expectations"
  | "runtimeContext"
  | "materialized"
  | "leased"
  | "callbackBridge"
  | "transport"
>;
declare const packageInspectionOptions: SourceAdapterPackageInspectionOptions;

const materialized = fixture.materializedSource({
  label: "orders",
});
const leased = fixture.leasedSource(["region", "desk"], {
  label: "orders",
});

describe("Source Adapter testing surface type contracts", () => {
  it("preserves exact fixture definitions and layer requirements", () => {
    expectTypeOf(materialized).toEqualTypeOf<
      SourceFixtureMaterializedDefinition<typeof FixtureRow.Type>
    >();
    expectTypeOf(leased).toEqualTypeOf<
      SourceFixtureLeasedDefinition<readonly ["region", "desk"], typeof FixtureRow.Type>
    >();
    expectTypeOf<SourceDefinitionRouteFields<typeof leased>>().toEqualTypeOf<
      readonly ["region", "desk"]
    >();
    expectTypeOf(fixture.layer).not.toBeAny();
    expectTypeOf<
      Effect.Success<ReturnType<typeof SourceFixture.make<typeof FixtureRow.Type>>>
    >().toEqualTypeOf<ControllableSourceFixture<typeof FixtureRow.Type>>();
    expectTypeOf<Context.Service.Identifier<typeof fixture.adapter.runtimeService>>().not.toBeAny();
  });

  it("enforces exact fixture commands", () => {
    const rejection = fixture.controls.reject(
      { _tag: "Materialized" },
      SourceFixture.failure("invalid", "stream"),
      {
        lane: "fixture",
        offset: 1n,
      },
    );
    const upsert = fixture.controls.upsert(
      {
        _tag: "Leased",
        route: { region: "eu" },
      },
      {
        id: "a",
      },
    );
    expectTypeOf(rejection).toEqualTypeOf<Effect.Effect<void, SourceFixtureFailure>>();
    expectTypeOf(upsert).toEqualTypeOf<Effect.Effect<void, SourceFixtureFailure>>();
    const wrongFailure: SourceFixtureFailure = SourceFixture.failure(
      "invalid",
      // @ts-expect-error fixture failure phases are exact.
      "transport",
    );
    expectTypeOf(wrongFailure).not.toBeAny();

    // @ts-expect-error Leased fixture definitions require a non-empty route tuple.
    fixture.leasedSource([], {
      label: "orders",
    });
    fixture.materializedSource({
      label: "orders",
      // @ts-expect-error fixture definition options are exact.
      unexpected: true,
    });
  });

  it("exposes a raw driver without host-level behavioral results", () => {
    const exactDriver = SourceFixture.conformanceDriver(conformanceFixture);
    const layer = Layer.succeed(SourceAdapterConformanceDriver, conformanceDriver);
    expectTypeOf(layer).toEqualTypeOf<Layer.Layer<SourceAdapterConformanceDriver>>();
    expectTypeOf(exactDriver).toExtend<SourceAdapterConformanceDriverValue>();
    expectTypeOf(exactDriver.adapter).toEqualTypeOf<typeof conformanceFixture.adapter>();
    expectTypeOf<SourceAdapterConformanceDriverInput<never, never>>().not.toBeAny();
    expectTypeOf(conformanceDriver.transport.command).not.toBeAny();
    expectTypeOf(conformanceDriver.transport.observe).not.toBeAny();
    expectTypeOf(
      sourceAdapterConformanceDefinitionIsLinked(
        exactDriver.materialized?.source,
        exactDriver.adapter,
        "materialized",
      ),
    ).toEqualTypeOf<boolean>();

    // @ts-expect-error the canonical conformance driver requires the canonical row contract.
    SourceFixture.conformanceDriver(fixture);
  });

  it("keeps every definition linked to the factory adapter", () => {
    const exactDriver = SourceFixture.conformanceDriver(conformanceFixture);
    const materialized = {
      source: conformanceFixture.materializedSource({
        label: "materialized",
      }),
      delayedRetrySource: conformanceFixture.materializedSource({
        label: "materialized-delayed",
      }),
      singleRetrySource: conformanceFixture.materializedSource({
        label: "materialized-single",
      }),
    };
    const leased = {
      source: conformanceFixture.leasedSource(["region"], {
        label: "leased",
      }),
      delayedRetrySource: conformanceFixture.leasedSource(["region"], {
        label: "leased-delayed",
      }),
      singleRetrySource: conformanceFixture.leasedSource(["region"], {
        label: "leased-single",
      }),
      sameRoute: { region: "eu" },
      distinctRoute: { region: "us" },
    };
    const validInput = {
      adapter: conformanceFixture.adapter,
      expectations: exactDriver.expectations,
      transport: exactDriver.transport,
      runtimeLayer: conformanceFixture.layer,
      materialized,
      leased,
      callbackBridge: conformanceFixture.callbackBridge,
    };
    const linked = makeSourceAdapterConformanceDriver(validInput);
    expectTypeOf(linked.adapter).toEqualTypeOf<typeof conformanceFixture.adapter>();

    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        ...materialized,
        // @ts-expect-error every conformance definition must use the factory adapter.
        source: unrelatedMaterializedDefinition,
      },
    });

    // @ts-expect-error Context layers require the nominal conformance Driver brand.
    Layer.succeed(SourceAdapterConformanceDriver, unbrandedConformanceDriver);
  });

  it("requires a real package root and executable artifact fixtures", () => {
    expectTypeOf(inspectSourceAdapterPackageConformance(packageInspectionOptions)).not.toBeAny();

    const invalidInspection = inspectSourceAdapterPackageConformance({
      ...packageInspectionOptions,
      // @ts-expect-error a published adapter must declare at least one platform export.
      platforms: [],
    });
    expectTypeOf(invalidInspection).not.toBeAny();
  });
});
