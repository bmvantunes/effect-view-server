import type {
  SourceAdapterHandle,
  SourceDefinition,
  SourceLifecycleDeclaration,
} from "effect-view-server/source-adapter";

export type PackageFixtureFailure = {
  readonly _tag: "PackageFixtureFailure";
  readonly message: string;
};

export type PackageFixtureMetrics = {
  readonly observed: bigint;
};

export type PackageFixtureLocation = {
  readonly offset: bigint;
};

export type PackageFixtureDefinitionOptions = {
  readonly stream: string;
};

export type PackageFixtureLifecycle = SourceLifecycleDeclaration<
  PackageFixtureMetrics,
  PackageFixtureLocation,
  PackageFixtureDefinitionOptions
>;

export declare const adapter: SourceAdapterHandle<
  "package-fixture",
  "1",
  PackageFixtureFailure,
  PackageFixtureLifecycle,
  PackageFixtureLifecycle
>;

export declare const source: (options: PackageFixtureDefinitionOptions) => SourceDefinition<
  typeof adapter,
  "materialized",
  PackageFixtureDefinitionOptions,
  readonly [],
  never,
  {
    readonly id: string;
    readonly region: string;
    readonly value: string;
  }
>;

export declare const leasedSource: (
  routeBy: readonly ["region"],
  options: PackageFixtureDefinitionOptions,
) => SourceDefinition<
  typeof adapter,
  "leased",
  PackageFixtureDefinitionOptions,
  readonly ["region"],
  never,
  {
    readonly id: string;
    readonly region: string;
    readonly value: string;
  }
>;

export declare const throwingSource: () => never;
export declare const primitiveSource: () => string;
