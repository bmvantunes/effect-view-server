import { expect } from "@effect/vitest";
import {
  SourceAdapterConformanceDriver,
  isSourceAdapterConformanceDriverValue,
  sourceAdapterConformanceDefinitionIsLinked,
  type SourceAdapterConformanceSuiteOptions,
} from "@effect-view-server/source-adapter-testing";
import { Effect } from "effect";

export type SourceAdapterPackageLinkEvidence = {
  readonly contract: {
    readonly adapter: unknown;
    readonly lifecycles: Readonly<
      Partial<Record<"materialized" | "leased", { readonly definition: unknown }>>
    >;
  };
};

type PackageRegistrationOptions<Inspection> = {
  readonly inspection: Inspection;
  readonly behavioral: SourceAdapterConformanceSuiteOptions;
};

export const makeSourceAdapterPackageConformanceCheck =
  <Inspection, Evidence extends SourceAdapterPackageLinkEvidence, Error>(
    inspect: (inspection: Inspection) => Effect.Effect<Evidence, Error>,
    validate: (evidence: Evidence, inspection: Inspection) => ReadonlyArray<unknown>,
  ) =>
  (
    options: PackageRegistrationOptions<Inspection>,
  ): Effect.Effect<void, Error, SourceAdapterConformanceDriver> =>
    Effect.gen(function* () {
      const evidence = yield* inspect(options.inspection);
      expect(validate(evidence, options.inspection)).toStrictEqual([]);
      const driver = yield* SourceAdapterConformanceDriver;
      expect(isSourceAdapterConformanceDriverValue(driver)).toBe(true);
      expect(driver.adapter).toBe(evidence.contract.adapter);
      for (const lifecycle of ["materialized", "leased"] as const) {
        const builtDefinition = evidence.contract.lifecycles[lifecycle]?.definition;
        const definitions = driver[lifecycle];
        const enabled = options.behavioral[lifecycle] === true;
        expect({
          built: builtDefinition !== undefined,
          driver: definitions !== undefined,
          enabled,
        }).toStrictEqual({
          built: enabled,
          driver: enabled,
          enabled,
        });
        if (builtDefinition !== undefined) {
          expect(
            sourceAdapterConformanceDefinitionIsLinked(builtDefinition, driver.adapter, lifecycle),
          ).toBe(true);
        }
        if (definitions !== undefined) {
          expect(
            [
              definitions.source,
              definitions.delayedRetrySource,
              definitions.singleRetrySource,
            ].every((definition) =>
              sourceAdapterConformanceDefinitionIsLinked(definition, driver.adapter, lifecycle),
            ),
          ).toBe(true);
        }
      }
      expect(driver.callbackBridge !== undefined).toBe(options.behavioral.callbackBridge === true);
      if (driver.callbackBridge !== undefined) {
        expect(
          sourceAdapterConformanceDefinitionIsLinked(
            driver.callbackBridge.source,
            driver.adapter,
            "materialized",
          ),
        ).toBe(true);
      }
    });

type RegisterPackageCheck = (name: string, check: () => Effect.Effect<void, unknown>) => void;

export const makeSourceAdapterPackageConformanceRegistrar =
  <Inspection extends { readonly name: string }>(
    registerCheck: RegisterPackageCheck,
    registerBehavioral: (options: SourceAdapterConformanceSuiteOptions) => void,
    check: (
      options: PackageRegistrationOptions<Inspection>,
    ) => Effect.Effect<void, unknown, SourceAdapterConformanceDriver>,
  ) =>
  (options: PackageRegistrationOptions<Inspection>): void => {
    registerCheck(
      `${options.inspection.name}: built package and behavioral driver are linked`,
      () => check(options).pipe(Effect.provide(options.behavioral.layer)),
    );
    registerBehavioral(options.behavioral);
  };
