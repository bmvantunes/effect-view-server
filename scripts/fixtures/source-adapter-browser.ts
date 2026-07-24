import { Schema } from "effect";
import { SourceAdapter } from "effect-view-server/source-adapter";

const BrowserFailure = Schema.TaggedStruct("BrowserFailure", {
  message: Schema.String,
});
const BrowserMetrics = Schema.Struct({
  observed: Schema.BigInt,
});
const BrowserLocation = Schema.Struct({
  offset: Schema.BigInt,
});

export const BrowserSourceAdapter = SourceAdapter.make({
  identity: {
    name: "browser-contract-fixture",
    version: "1",
  },
  failure: BrowserFailure,
  materialized: {
    metrics: BrowserMetrics,
    rejectionLocation: BrowserLocation,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly stream: string;
    }>(),
  },
  leased: undefined,
});

export const browserSourceDefinition =
  BrowserSourceAdapter.materializedSource({
    stream: "browser-safe",
  });
