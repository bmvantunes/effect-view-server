import { fileURLToPath } from "node:url";
import { expect, it } from "@effect/vitest";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  SourceAdapterConformanceDriver,
  SourceAdapterConformanceRow,
  SourceFixture,
  inspectSourceAdapterPackageConformance,
  makeSourceAdapterConformanceDriver,
  type SourceAdapterPackageInspectionOptions,
  validateSourceAdapterPackageConformance,
} from "@effect-view-server/source-adapter-testing";
import { Config, Effect, Layer, Stream } from "effect";

Reflect.set(
  globalThis,
  Symbol.for("@effect-view-server/source-adapter-testing/PublicSdkFixtureRuntime"),
  {
    SourceAdapter,
    SourceAdapterServer,
  },
);

const {
  adapter: builtPackageAdapter,
  leasedSource: builtPackageLeasedSource,
  source: builtPackageSource,
} = await import("../../source-adapter-testing/test-fixtures/package-adapter/contract.js");
import {
  makeSourceAdapterPackageConformanceCheck,
  makeSourceAdapterPackageConformanceRegistrar,
} from "./package-registration";

const builtPackageRoot = fileURLToPath(
  new URL("../../source-adapter-testing/test-fixtures/package-adapter", import.meta.url),
);

const builtPackageInspection: SourceAdapterPackageInspectionOptions = {
  name: "real built Source Adapter fixture",
  packageRoot: builtPackageRoot,
  contract: {
    adapterExport: "adapter",
    serverAdapterExport: "serverAdapter",
    failure: {
      valid: {
        _tag: "PackageFixtureFailure",
        message: "offline",
      },
      invalid: {
        _tag: "PackageFixtureFailure",
        message: 1,
      },
    },
    lifecycles: [
      {
        lifecycle: "materialized",
        definitionExport: "source",
        definitionArguments: [{ stream: "orders" }],
        metrics: {
          valid: { observed: 1n },
          invalid: { observed: "1" },
        },
        rejectionLocation: {
          valid: { offset: 1n },
          invalid: { offset: 1 },
        },
      },
      {
        lifecycle: "leased",
        definitionExport: "leasedSource",
        definitionArguments: [["region"], { stream: "orders" }],
        metrics: {
          valid: { observed: 1n },
          invalid: { observed: "1" },
        },
        rejectionLocation: {
          valid: { offset: 1n },
          invalid: { offset: 1 },
        },
      },
    ],
  },
  typeTestProject: "tsconfig.json",
  browser: {
    budgetBytes: 96 * 1024,
  },
  platforms: [
    {
      export: "./node",
      viewServer: { topics: {} },
      exactResources: { resources: ["client"] },
      emptyResources: { resources: [] },
      missingResources: {},
      extraResources: { resources: ["client"], extra: true },
      duplicateResources: { resources: ["client", "client"] },
      exactConfigResources: { resources: Config.succeed(["client"]) },
    },
  ],
  effectPeerDependencies: ["@effect/platform-node"],
};

const builtDefinition = builtPackageSource({ stream: "orders" });
const builtLeasedDefinition = builtPackageLeasedSource(["region"], {
  stream: "orders",
});
const builtPackageDriver = makeSourceAdapterConformanceDriver({
  adapter: builtPackageAdapter,
  expectations: {
    materialized: {
      acquisitionFailure: undefined,
      partialAcquisitionFinalizationCount: 1n,
      streamFailure: undefined,
      settlementFailure: undefined,
      rejectionFailure: () => undefined,
      rejectionLocation: (_target, offset) => ({ offset }),
      updatedMetrics: { observed: 1n },
    },
    leased: {
      acquisitionFailure: undefined,
      partialAcquisitionFinalizationCount: 1n,
      streamFailure: undefined,
      settlementFailure: undefined,
      rejectionFailure: () => undefined,
      rejectionLocation: (_target, offset) => ({ offset }),
      updatedMetrics: { observed: 1n },
    },
  },
  runtimeLayer: Layer.empty,
  materialized: {
    source: builtDefinition,
    delayedRetrySource: builtDefinition,
    singleRetrySource: builtDefinition,
  },
  leased: {
    source: builtLeasedDefinition,
    delayedRetrySource: builtLeasedDefinition,
    singleRetrySource: builtLeasedDefinition,
    sameRoute: { region: "eu" },
    distinctRoute: { region: "us" },
  },
  transport: {
    command: () => Effect.void,
    observe: () =>
      Effect.succeed({
        acquisitions: 0n,
        finalizations: 0n,
        partialAcquisitionFinalizations: 0n,
        registrations: 0n,
        callbackFinalizations: 0n,
        finalizerStarted: false,
      }),
    changes: () => Stream.empty,
  },
});

it.effect("checks exact built-package and behavioral adapter identity", () =>
  Effect.gen(function* () {
    const fixture = yield* SourceFixture.make(SourceAdapterConformanceRow);
    const driver = SourceFixture.conformanceDriver(fixture);
    const check = makeSourceAdapterPackageConformanceCheck(
      (inspection: { readonly name: string }) =>
        Effect.succeed({
          contract: {
            adapter: driver.adapter,
            lifecycles: {
              materialized: {
                definition: driver.materialized?.source,
              },
              leased: {
                definition: driver.leased?.source,
              },
            },
          },
          inspection,
        }),
      (evidence, inspection) =>
        evidence.inspection === inspection ? [] : [{ code: "MismatchedInspection" }],
    );

    yield* check({
      inspection: { name: "fixture" },
      behavioral: {
        name: "fixture",
        layer: Layer.succeed(SourceAdapterConformanceDriver, driver),
        materialized: true,
        leased: true,
        callbackBridge: true,
      },
    }).pipe(Effect.provideService(SourceAdapterConformanceDriver, driver));

    const materializedDriver = makeSourceAdapterConformanceDriver({
      adapter: fixture.adapter,
      expectations: {
        materialized: driver.expectations.materialized,
        leased: undefined,
      },
      transport: driver.transport,
      runtimeLayer: fixture.layer,
      materialized: {
        source: fixture.materializedSource({ label: "materialized" }),
        delayedRetrySource: fixture.materializedSource({
          label: "materialized-delayed",
        }),
        singleRetrySource: fixture.materializedSource({
          label: "materialized-single",
        }),
      },
    });
    const materializedCheck = makeSourceAdapterPackageConformanceCheck(
      () =>
        Effect.succeed({
          contract: {
            adapter: materializedDriver.adapter,
            lifecycles: {
              materialized: {
                definition: materializedDriver.materialized?.source,
              },
            },
          },
        }),
      () => [],
    );
    yield* materializedCheck({
      inspection: { name: "materialized fixture" },
      behavioral: {
        name: "materialized fixture",
        layer: Layer.succeed(SourceAdapterConformanceDriver, materializedDriver),
        materialized: true,
      },
    }).pipe(Effect.provideService(SourceAdapterConformanceDriver, materializedDriver));
  }),
);

it.effect("rejects package issues and an unrelated behavioral adapter", () =>
  Effect.gen(function* () {
    const fixture = yield* SourceFixture.make(SourceAdapterConformanceRow);
    const driver = SourceFixture.conformanceDriver(fixture);
    const inspection = { name: "fixture" };
    const invalidPackage = makeSourceAdapterPackageConformanceCheck(
      () =>
        Effect.succeed({
          contract: {
            adapter: driver.adapter,
            lifecycles: {
              materialized: {
                definition: driver.materialized?.source,
              },
              leased: {
                definition: driver.leased?.source,
              },
            },
          },
        }),
      () => [{ code: "InvalidPackage" }],
    );
    const unrelatedAdapter = makeSourceAdapterPackageConformanceCheck(
      () =>
        Effect.succeed({
          contract: {
            adapter: {},
            lifecycles: {
              materialized: {
                definition: driver.materialized?.source,
              },
              leased: {
                definition: driver.leased?.source,
              },
            },
          },
        }),
      () => [],
    );
    const omittedLifecycle = makeSourceAdapterPackageConformanceCheck(
      () =>
        Effect.succeed({
          contract: {
            adapter: driver.adapter,
            lifecycles: {
              materialized: {
                definition: driver.materialized?.source,
              },
              leased: {
                definition: driver.leased?.source,
              },
            },
          },
        }),
      () => [],
    );

    expect(
      (yield* invalidPackage({
        inspection,
        behavioral: {
          name: "fixture",
          layer: Layer.succeed(SourceAdapterConformanceDriver, driver),
          materialized: true,
          leased: true,
          callbackBridge: true,
        },
      }).pipe(Effect.provideService(SourceAdapterConformanceDriver, driver), Effect.exit))._tag,
    ).toBe("Failure");
    expect(
      (yield* unrelatedAdapter({
        inspection,
        behavioral: {
          name: "fixture",
          layer: Layer.succeed(SourceAdapterConformanceDriver, driver),
          materialized: true,
          leased: true,
          callbackBridge: true,
        },
      }).pipe(Effect.provideService(SourceAdapterConformanceDriver, driver), Effect.exit))._tag,
    ).toBe("Failure");
    expect(
      (yield* omittedLifecycle({
        inspection,
        behavioral: {
          name: "fixture",
          layer: Layer.succeed(SourceAdapterConformanceDriver, driver),
          materialized: true,
        },
      }).pipe(Effect.provideService(SourceAdapterConformanceDriver, driver), Effect.exit))._tag,
    ).toBe("Failure");
  }),
);

it.effect("registers one linked-package check before the behavioral suite", () =>
  Effect.gen(function* () {
    const fixture = yield* SourceFixture.make(SourceAdapterConformanceRow);
    const driver = SourceFixture.conformanceDriver(fixture);
    const layer = Layer.succeed(SourceAdapterConformanceDriver, driver);
    const registrations: Array<string> = [];
    const effects: Array<Effect.Effect<void, unknown>> = [];
    const register = makeSourceAdapterPackageConformanceRegistrar(
      (name, check) => {
        registrations.push(name);
        effects.push(check());
      },
      (options) => {
        registrations.push(options.name);
      },
      () =>
        Effect.gen(function* () {
          const suppliedDriver = yield* SourceAdapterConformanceDriver;
          expect(suppliedDriver.adapter).toBe(driver.adapter);
        }),
    );

    register({
      inspection: {
        name: "fixture package",
      },
      behavioral: {
        name: "fixture behavior",
        layer,
        materialized: true,
        leased: true,
        callbackBridge: true,
      },
    });

    expect(registrations).toStrictEqual([
      "fixture package: built package and behavioral driver are linked",
      "fixture behavior",
    ]);
    yield* Effect.all(effects);
  }),
);

it.effect("links a real built package through the registrar-provided Driver Layer", () =>
  Effect.gen(function* () {
    const effects: Array<Effect.Effect<void, unknown>> = [];
    const behavioralRegistrations: Array<string> = [];
    const check = makeSourceAdapterPackageConformanceCheck(
      inspectSourceAdapterPackageConformance,
      validateSourceAdapterPackageConformance,
    );
    const register = makeSourceAdapterPackageConformanceRegistrar(
      (_name, packageCheck) => {
        effects.push(packageCheck());
      },
      (behavioral) => {
        behavioralRegistrations.push(behavioral.name);
      },
      check,
    );

    register({
      inspection: builtPackageInspection,
      behavioral: {
        name: "real built Source Adapter behavior",
        layer: Layer.succeed(SourceAdapterConformanceDriver, builtPackageDriver),
        materialized: true,
        leased: true,
      },
    });

    yield* Effect.all(effects);
    expect(behavioralRegistrations).toStrictEqual(["real built Source Adapter behavior"]);
  }),
);
