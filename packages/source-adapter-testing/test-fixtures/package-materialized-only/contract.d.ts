import type {
  SourceAdapterHandle,
  SourceDefinition,
  SourceLifecycleDeclaration,
} from "effect-view-server/source-adapter";

type Failure = {
  readonly _tag: "PackageMaterializedOnlyFailure";
  readonly message: string;
};

type Metrics = {
  readonly observed: bigint;
};

type Location = {
  readonly offset: bigint;
};

type DefinitionOptions = {
  readonly stream: string;
};

type Lifecycle = SourceLifecycleDeclaration<Metrics, Location, DefinitionOptions>;

export declare const adapter: SourceAdapterHandle<
  "package-materialized-only",
  "1",
  Failure,
  Lifecycle,
  undefined
>;

export declare const source: (options: DefinitionOptions) => SourceDefinition<
  typeof adapter,
  "materialized",
  DefinitionOptions,
  readonly [],
  never,
  {
    readonly id: string;
    readonly region: string;
    readonly value: string;
  }
>;
