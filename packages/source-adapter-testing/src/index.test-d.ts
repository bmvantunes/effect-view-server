import { describe, expectTypeOf, it } from "@effect/vitest";
import type { SourceDefinitionRouteFields } from "@effect-view-server/source-adapter";
import { Context, Effect, Layer, Schema } from "effect";
import {
  SourceAdapterConformanceSubject,
  SourceAdapterPackageConformanceSubject,
  SourceFixture,
  registerSourceAdapterConformance,
  registerSourceAdapterPackageConformance,
  type ControllableSourceFixture,
  type SourceAdapterConformanceMaterializedSnapshot,
  type SourceAdapterConformanceSubjectValue,
  type SourceAdapterPackageConformanceSubjectValue,
  type SourceFixtureFailure,
  type SourceFixtureLeasedDefinition,
  type SourceFixtureMaterializedDefinition,
} from "./index";

const FixtureRow = Schema.Struct({
  id: Schema.String,
  value: Schema.Number,
});

declare const fixture: ControllableSourceFixture<typeof FixtureRow.Type>;
declare const conformanceSubject: SourceAdapterConformanceSubjectValue;
declare const packageConformanceSubject: SourceAdapterPackageConformanceSubjectValue;

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

  it("exposes an exact active conformance Layer contract", () => {
    const layer = Layer.succeed(SourceAdapterConformanceSubject, conformanceSubject);
    expectTypeOf(layer).toEqualTypeOf<Layer.Layer<SourceAdapterConformanceSubject>>();
    expectTypeOf(
      registerSourceAdapterConformance({
        name: "third-party adapter",
        layer,
        materialized: true,
        leased: true,
      }),
    ).toEqualTypeOf<void>();
    expectTypeOf<SourceAdapterConformanceMaterializedSnapshot>().not.toBeAny();

    registerSourceAdapterConformance({
      name: "invalid",
      // @ts-expect-error conformance requires its exact nominal Subject Layer.
      layer: Layer.empty,
      materialized: true,
    });

    registerSourceAdapterConformance({
      name: "invalid callback option",
      layer,
      // @ts-expect-error callback-buffer conformance is enabled with a boolean.
      callbackBuffer: "yes",
    });
  });

  it("exposes an exact package conformance Layer contract", () => {
    const layer = Layer.succeed(SourceAdapterPackageConformanceSubject, packageConformanceSubject);
    expectTypeOf(layer).toEqualTypeOf<Layer.Layer<SourceAdapterPackageConformanceSubject>>();
    expectTypeOf(
      registerSourceAdapterPackageConformance({
        name: "published adapter package",
        layer,
        platformExports: ["./kafka"],
        effectPeerDependencies: ["@effect/platform"],
      }),
    ).toEqualTypeOf<void>();

    registerSourceAdapterPackageConformance({
      name: "invalid platform list",
      layer,
      // @ts-expect-error a published adapter must declare at least one platform export.
      platformExports: [],
    });
    registerSourceAdapterPackageConformance({
      name: "invalid package subject",
      // @ts-expect-error package conformance requires its exact nominal Subject Layer.
      layer: Layer.empty,
      platformExports: ["./kafka"],
    });
  });
});
