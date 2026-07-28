import type { SourceDefinitionAny } from "@effect-view-server/source-adapter";

export type { SourceDefinitionAny } from "@effect-view-server/source-adapter";

export type NonEmptyRouteBy = readonly [string, ...ReadonlyArray<string>];

export type SourceOwnedTopicDefinition = {
  readonly source: SourceDefinitionAny;
};
