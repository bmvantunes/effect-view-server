import { SourceAdapter } from "effect-view-server/source-adapter";
import { Schema } from "effect";

export const PackageMaterializedOnlyFailure = Schema.TaggedStruct(
  "PackageMaterializedOnlyFailure",
  {
    message: Schema.String,
  },
);

export const PackageMaterializedOnlyMetrics = Schema.Struct({
  observed: Schema.BigInt,
});

export const PackageMaterializedOnlyLocation = Schema.Struct({
  offset: Schema.BigInt,
});

export const adapter = SourceAdapter.make({
  identity: {
    name: "package-materialized-only",
    version: "1",
  },
  failure: PackageMaterializedOnlyFailure,
  materialized: {
    metrics: PackageMaterializedOnlyMetrics,
    rejectionLocation: PackageMaterializedOnlyLocation,
    definitionOptions: SourceAdapter.definitionOptions(),
  },
  leased: undefined,
});

export const source = (options) => adapter.materializedSource(options);
