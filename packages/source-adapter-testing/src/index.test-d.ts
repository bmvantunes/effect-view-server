import { describe, expectTypeOf, it } from "@effect/vitest";
import type {
  SourceDefinition,
  SourceDefinitionRow,
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
declare const extraFieldConformanceFixture: ControllableSourceFixture<
  SourceAdapterConformanceRow & {
    readonly extra: string;
  }
>;
declare const narrowedConformanceFixture: ControllableSourceFixture<{
  readonly id: "fixed";
  readonly region: string;
  readonly value: string;
}>;
declare const widenedConformanceFixture: ControllableSourceFixture<{
  readonly id: string;
  readonly region: string;
  readonly value: string | number;
}>;
declare const unionRowConformanceFixture: ControllableSourceFixture<
  | SourceAdapterConformanceRow
  | (SourceAdapterConformanceRow & {
      readonly extra: string;
    })
>;
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
const canonicalMaterializedDefinitions = {
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
const widenedMaterializedDefinitions = {
  source: widenedConformanceFixture.materializedSource(),
  delayedRetrySource: widenedConformanceFixture.materializedSource(),
  singleRetrySource: widenedConformanceFixture.materializedSource(),
};
declare const optionalMaterializedDefinitions:
  | typeof canonicalMaterializedDefinitions
  | typeof widenedMaterializedDefinitions
  | undefined;
declare const optionalCallbackBridge:
  | typeof conformanceFixture.callbackBridge
  | typeof widenedConformanceFixture.callbackBridge
  | undefined;
declare const nestedMaterializedSourceUnion:
  | (typeof canonicalMaterializedDefinitions)["source"]
  | ReturnType<typeof narrowedConformanceFixture.materializedSource>;
declare const nestedCallbackSourceUnion:
  | (typeof conformanceFixture.callbackBridge)["source"]
  | (typeof extraFieldConformanceFixture.callbackBridge)["source"];
const conditionalExtraDefinition =
  Math.random() > 0.5
    ? conformanceFixture.materializedSource()
    : extraFieldConformanceFixture.materializedSource();

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
    expectTypeOf<
      SourceDefinitionRow<ReturnType<typeof conformanceFixture.materializedSource>>
    >().toEqualTypeOf<SourceAdapterConformanceRow>();
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
    const materialized = canonicalMaterializedDefinitions;
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
    const invalidUnionInput = {
      ...validInput,
      materialized: widenedMaterializedDefinitions,
    };
    const unionInput = Math.random() > 0.5 ? validInput : invalidUnionInput;
    const linked = makeSourceAdapterConformanceDriver(validInput);
    expectTypeOf(linked.adapter).toEqualTypeOf<typeof conformanceFixture.adapter>();
    // @ts-expect-error raw conformance drivers reject Definitions with a non-canonical row.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        source: fixture.materializedSource(),
        delayedRetrySource: fixture.materializedSource(),
        singleRetrySource: fixture.materializedSource(),
      },
    });
    // @ts-expect-error canonical conformance rows reject extra fields.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        source: extraFieldConformanceFixture.materializedSource(),
        delayedRetrySource: extraFieldConformanceFixture.materializedSource(),
        singleRetrySource: extraFieldConformanceFixture.materializedSource(),
      },
    });
    // @ts-expect-error canonical conformance row fields reject narrower types.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        source: narrowedConformanceFixture.materializedSource(),
        delayedRetrySource: narrowedConformanceFixture.materializedSource(),
        singleRetrySource: narrowedConformanceFixture.materializedSource(),
      },
    });
    // @ts-expect-error canonical conformance row fields reject wider types.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        source: widenedConformanceFixture.materializedSource(),
        delayedRetrySource: widenedConformanceFixture.materializedSource(),
        singleRetrySource: widenedConformanceFixture.materializedSource(),
      },
    });
    // @ts-expect-error delayed-retry definitions require the exact canonical row.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        ...materialized,
        delayedRetrySource: extraFieldConformanceFixture.materializedSource(),
      },
    });
    // @ts-expect-error single-retry definitions require the exact canonical row.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        ...materialized,
        singleRetrySource: extraFieldConformanceFixture.materializedSource(),
      },
    });
    // @ts-expect-error callback bridge definitions require the exact canonical row.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      callbackBridge: extraFieldConformanceFixture.callbackBridge,
    });
    // @ts-expect-error every member of a union input must use the canonical row.
    makeSourceAdapterConformanceDriver(unionInput);
    // @ts-expect-error optional lifecycle unions reject non-canonical definition members.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: optionalMaterializedDefinitions,
    });
    // @ts-expect-error optional callback unions reject non-canonical source members.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      callbackBridge: optionalCallbackBridge,
    });
    // @ts-expect-error every member of a nested lifecycle Definition union must be canonical.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        ...materialized,
        source: nestedMaterializedSourceUnion,
      },
    });
    // @ts-expect-error every member of a nested callback Definition union must be canonical.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      callbackBridge: {
        ...conformanceFixture.callbackBridge,
        source: nestedCallbackSourceUnion,
      },
    });
    // @ts-expect-error every member of a Definition's Row union must be canonical.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        ...materialized,
        source: unionRowConformanceFixture.materializedSource(),
      },
    });
    // @ts-expect-error inferred conditional Definition branches must all use the canonical Row.
    makeSourceAdapterConformanceDriver({
      ...validInput,
      materialized: {
        ...materialized,
        source: conditionalExtraDefinition,
      },
    });

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
