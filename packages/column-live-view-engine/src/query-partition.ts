/**
 * @internal A trusted runtime-owned row partition. It is intentionally separate from the public
 * query language: adapters use it to constrain one subscription to rows owned by an acquired
 * source partition without changing the caller's local `where` semantics.
 */
export type ColumnLiveViewEngineQueryPartition = {
  readonly key: string;
  /**
   * Iterates the unique storage keys currently owned by this partition. Missing keys are ignored.
   * Topic Store uses this exact candidate set for bounded initial raw and grouped scans.
   */
  readonly ownedStorageKeys: () => Iterable<string>;
  /**
   * The storage key is authoritative for source ownership. Callers may omit it only when
   * exercising the row-level predicate directly; engine evaluation always supplies it.
   */
  readonly matches: (row: object, storageKey?: string) => boolean;
};
