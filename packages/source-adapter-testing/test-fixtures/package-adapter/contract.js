import { SourceAdapter } from "effect-view-server/source-adapter";
import { Schema } from "effect";

export const PackageFixtureFailure = Schema.TaggedStruct("PackageFixtureFailure", {
  message: Schema.String,
});

export const PackageFixtureMetrics = Schema.Struct({
  observed: Schema.BigInt,
});

export const PackageFixtureLocation = Schema.Struct({
  offset: Schema.BigInt,
});

export const adapter = SourceAdapter.make({
  identity: {
    name: "package-fixture",
    version: "1",
  },
  failure: PackageFixtureFailure,
  materialized: {
    metrics: PackageFixtureMetrics,
    rejectionLocation: PackageFixtureLocation,
    definitionOptions: SourceAdapter.definitionOptions(),
  },
  leased: {
    metrics: PackageFixtureMetrics,
    rejectionLocation: PackageFixtureLocation,
    definitionOptions: SourceAdapter.definitionOptions(),
  },
});

export const source = (options) => adapter.materializedSource(options);
export const leasedSource = (routeBy, options) => adapter.leasedSource(routeBy, options);

export const throwingSource = () => {
  throw new Error("definition construction failed");
};

export const primitiveSource = () => "not-a-definition";
